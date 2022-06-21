use super::conf::BusConfig;
use super::error;
use super::message::TransportMessage;
use super::util;
use log::{debug, error, trace};
use redis::{
        Commands, Value, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use redis::streams::{
    StreamId, StreamKey, StreamReadOptions, StreamReadReply, StreamMaxlen};
use std::fmt;
use std::time;

const DEFAULT_REDIS_PORT: u16 = 6379;

/// Manages the Redis connection.
pub struct Bus {
    connection: redis::Connection,

    /// Our unique identifier on the bus
    bus_id: String,

    /// The stream this connection pulls messages from.
    stream_name: Option<String>,
}

impl Bus {

    pub fn new(bus_config: &BusConfig, bus_id: String) -> Result<Self, error::Error> {
        let info = Bus::connection_info(bus_config)?;
        debug!("Bus::new() connecting to {:?}", info);

        let client = redis::Client::open(info)?;

        let connection = match client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                return Err(error::Error::BusError(e));
            }
        };

        let mut bus = Bus {
            bus_id: bus_id,
            connection: connection,
            stream_name: None,
        };

        bus.setup_stream()?;

        Ok(bus)
    }

    pub fn setup_stream(&mut self) -> Result<(), error::Error> {

        let sname = self.stream_name().to_string();

        debug!("{} setting up stream={} group={}", self, sname, sname);

        let created: Result<(), _> =
            self.connection().xgroup_create_mkstream(&sname, &sname, "$");

        if let Err(_e) = created {
            // TODO see about differentiating error types so we can
            // report real errors.
            debug!("{} stream group {} already exists", self, sname);
        }

        Ok(())
    }

    pub fn stream_name(&self) -> &str {
        if let Some(s) = &self.stream_name {
            &s
        } else {
            &self.bus_id
        }
    }

    /// Called by service-level clients to claim a shared stream name.
    pub fn set_stream_name(&mut self, name: &str) {
        self.stream_name = Some(String::from(name));
    }

    /// Generates the Redis connection Info
    fn connection_info(bus_config: &BusConfig) -> Result<ConnectionInfo, error::Error> {
        // Build the connection info by hand because it gives us more
        // flexibility/control than compiling a URL string.

        // TODO: do we need a way to say username/password are required?
        // There may be cases where we want to use the default login,
        // e.g. out-of-band maintenance.
        let mut redis_con = RedisConnectionInfo {
            db: 0,
            username: None,
            password: None,
        };

        if let Some(username) = bus_config.username() {
            redis_con.username = Some(String::from(username));

            if let Some(password) = bus_config.password() {
                redis_con.password = Some(String::from(password));
            }
        }

        let con_addr: ConnectionAddr;

        if let Some(ref s) = bus_config.sock() {
            con_addr = ConnectionAddr::Unix(s.into());
        } else {
            if let Some(ref host) = bus_config.host() {
                let mut port = DEFAULT_REDIS_PORT;

                if let Some(p) = bus_config.port() {
                    port = *p;
                }

                // NOTE: TcpTls not currently supported
                con_addr = ConnectionAddr::Tcp(String::from(host), port);
            } else {
                return Err(error::Error::ClientConfigError(format!(
                    "Host or Unix Sock Info Required"
                )));
            }
        };

        Ok(ConnectionInfo {
            addr: con_addr,
            redis: redis_con,
        })
    }

    /// Generates a unique address with a prefix string.
    pub fn new_bus_id(prefix: &str) -> String {
        String::from(prefix) + ":" + &util::random_12()
    }

    pub fn bus_id(&self) -> &str {
        &self.bus_id
    }

    fn connection(&mut self) -> &mut redis::Connection {
        &mut self.connection
    }

    /// Returns at most one String pulled from the queue or None if the
    /// pop times out or is interrupted.
    ///
    /// The string will be valid JSON string.
    fn recv_one_chunk(&mut self, timeout: i32) -> Result<Option<String>, error::Error> {

        trace!(
            "recv_one_chunk() timeout={} for recipient {}",
            timeout,
            self.bus_id()
        );

        let bus_id = self.bus_id().to_string(); // XXX
        let stream_name = self.stream_name().to_string(); // XXX

        let mut read_opts = StreamReadOptions::default()
            .count(1)
            .group(&stream_name, &bus_id);

        if timeout != 0 {
            if timeout == -1 { // block indefinitely
                read_opts = read_opts.block(0);
            } else {
                read_opts = read_opts.block(timeout as usize * 1000); // milliseconds
            }
        }

        let reply: StreamReadReply = match self.connection()
            .xread_options(&[&stream_name], &[">"], &read_opts) {
            Ok(r) => r,
            Err(e) => match e.kind() {
                redis::ErrorKind::TypeError => {
                    // Will read a Nil value on timeout.  That's OK.
                    trace!("{} stream read returned nothing", self);
                    return Ok(None);
                }
                _ => {
                    return Err(error::Error::BusError(e));
                }
            }
        };

        let mut value_op: Option<String> = None;

		for StreamKey { key, ids } in reply.keys {
			trace!("{} read value from stream {}", self, key);

			for StreamId { id, map } in ids {
                trace!("{} read message ID {}", self, id);

                // TODO do we need this?
                let acked: Result<(), _> =
                    self.connection().xack(&stream_name, &stream_name, &[&id]);

                acked.ok(); // ignorable

                if let Some(message) = map.get("message") {
                    if let Value::Data(bytes) = message {
                        if let Ok(s) = String::from_utf8(bytes.to_vec()) {
                            value_op = Some(s);
                        } else {
                            error!("{} received unexpected stream data: {:?}", self, message);
                            return Ok(None);
                        };
                    } else {
                        error!("{} received unexpected stream data", self);
                        return Ok(None);
                    }
                };
			}
		}

        Ok(value_op)
    }

    /// Returns at most one JSON value pulled from the queue or None if
    /// the list pop times out or the pop is interrupted by a signal.
    fn recv_one_value(&mut self, timeout: i32) -> Result<Option<json::JsonValue>, error::Error> {
        let json_string = match self.recv_one_chunk(timeout)? {
            Some(s) => s,
            None => {
                return Ok(None);
            }
        };

        match json::parse(&json_string) {
            Ok(json_val) => Ok(Some(json_val)),

            // Log the error and bubble it up to the caller.
            Err(err_msg) => {
                error!("Error parsing JSON: {:?}", err_msg);
                return Err(super::error::Error::JsonError(err_msg));
            }
        }
    }

    /// Returns at most one JSON value pulled from the queue.
    ///
    /// Keeps trying until a value is returned or the timeout is exceeded.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Time in seconds to wait for a value.
    ///     A negative value means to block indefinitely.
    ///     0 means do not block.
    pub fn recv_json_value(
        &mut self,
        timeout: i32,
    ) -> Result<Option<json::JsonValue>, error::Error> {
        let mut option: Option<json::JsonValue>;

        if timeout == 0 {
            // See if any data is ready now
            return self.recv_one_value(timeout);
        } else if timeout < 0 {
            // Keep trying until we have a result.
            loop {
                option = self.recv_one_value(timeout)?;
                if let Some(_) = option {
                    return Ok(option);
                }
            }
        }

        // Keep trying until we have a result or exhaust the timeout.

        let mut seconds = timeout;

        while seconds > 0 {
            let now = time::SystemTime::now();

            option = self.recv_one_value(timeout)?;

            match option {
                None => {
                    if seconds < 0 {
                        return Ok(None);
                    }
                    seconds -= now.elapsed().unwrap().as_secs() as i32;
                    continue;
                }
                _ => return Ok(option),
            }
        }

        Ok(None)
    }

    pub fn recv(&mut self, timeout: i32) -> Result<Option<TransportMessage>, error::Error> {
        let json_op = self.recv_json_value(timeout)?;

        match json_op {
            Some(ref jv) => Ok(TransportMessage::from_json_value(jv)),
            None => Ok(None),
        }
    }

    pub fn send(&mut self, msg: &TransportMessage) -> Result<(), error::Error> {
        let recipient = msg.to();
        let json_str = msg.to_json_value().dump();

        trace!("send() writing chunk to={}: {}", recipient, json_str);

        let maxlen = StreamMaxlen::Approx(1000); // TODO CONFIG

        let res: Result<String, _> = self.connection()
            .xadd_maxlen(recipient, maxlen, "*", &[("message", json_str)]);

        trace!("here 1");

        if let Err(e) = res {
        trace!("here 2");
            return Err(error::Error::BusError(e));
        };

        Ok(())
    }

    pub fn clear_stream(&mut self) -> Result<(), error::Error> {

        let sname = self.stream_name().to_string(); // XXX
        let maxlen = StreamMaxlen::Equals(0);
        let res: Result<i32, _> = self.connection().xtrim(&sname, maxlen);

        if let Err(e) = res {
            return Err(error::Error::BusError(e));
        }

        Ok(())
    }

    /// Removes our stream, which also removes our consumer group
    pub fn delete_stream(&mut self) -> Result<(), error::Error> {

        let sname = self.stream_name().to_string(); // XXX
        let res: Result<i32, _> = self.connection().del(&sname);

        if let Err(e) = res {
            return Err(error::Error::BusError(e));
        }

        Ok(())
    }

    // Rust redis has no disconnect, but calling a method named
    // disconnect will makes sense.
    pub fn disconnect(&mut self) -> Result<(), error::Error> {

        // Avoid deleting the stream for service: connections since
        // those are shared.
        if self.stream_name()[0..7].eq("client:") {
            self.delete_stream()?;
        }

        Ok(())
    }
}

impl fmt::Display for Bus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Bus {}", self.bus_id())
    }
}
