use super::addr::ClientAddress;
use super::conf;
use super::message::TransportMessage;
use log::{debug, error, trace};
use redis::streams::{StreamId, StreamKey, StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{Commands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo, Value};
use std::fmt;
use std::time;

/// Manages the Redis connection.
pub struct Bus {
    connection: redis::Connection,

    // Every bus connection has a unique client address.
    address: ClientAddress,

    domain: String,
}

impl Bus {
    pub fn new(config: &conf::BusConnection) -> Result<Self, String> {
        let info = Bus::connection_info(config)?;

        trace!("Bus::new() connecting to {:?}", info);

        let client = match redis::Client::open(info) {
            Ok(c) => c,
            Err(e) => {
                return Err(format!("Error opening Redis connection: {e}"));
            }
        };

        let connection = match client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                return Err(format!("Cannot connect: {e}"));
            }
        };

        let addr = ClientAddress::new(config.domain().name());

        let mut bus = Bus {
            domain: config.domain().name().to_string(),
            connection,
            address: addr,
        };

        bus.setup_stream(None)?;

        Ok(bus)
    }

    pub fn setup_stream(&mut self, name: Option<&str>) -> Result<(), String> {
        let sname = match name {
            Some(n) => n.to_string(),
            None => self.address().full().to_string(),
        };

        debug!("{} setting up stream={}", self, sname);

        let created: Result<(), _> = self
            .connection()
            .xgroup_create_mkstream(&sname, &sname, "$");

        if let Err(_) = created {
            // TODO see about differentiating error types so we can
            // report real errors.
            debug!("{} stream group {} already exists", self, sname);
        }

        Ok(())
    }

    /// Generates the Redis connection Info
    fn connection_info(config: &conf::BusConnection) -> Result<ConnectionInfo, String> {
        // Build the connection info by hand because it gives us more
        // flexibility/control than compiling a URL string.

        let acct = config.connection_type().credentials();
        let redis_con = RedisConnectionInfo {
            db: 0,
            username: Some(acct.username().to_string()),
            password: Some(acct.password().to_string()),
        };

        let con_addr =
            ConnectionAddr::Tcp(config.domain().name().to_string(), config.domain().port());

        Ok(ConnectionInfo {
            addr: con_addr,
            redis: redis_con,
        })
    }

    pub fn address(&self) -> &ClientAddress {
        &self.address
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn connection(&mut self) -> &mut redis::Connection {
        &mut self.connection
    }

    /// Returns at most one String pulled from the queue or None if the
    /// pop times out or is interrupted.
    ///
    /// The string will be valid JSON string.
    fn recv_one_chunk(
        &mut self,
        timeout: i32,
        stream: Option<&str>,
    ) -> Result<Option<String>, String> {
        let sname = match stream {
            Some(s) => s.to_string(),
            None => self.address().full().to_string(),
        };

        trace!(
            "recv_one_chunk() timeout={} for recipient {}",
            timeout,
            sname
        );

        let mut read_opts = StreamReadOptions::default()
            .count(1)
            .noack()
            .group(&sname, &sname);

        if timeout != 0 {
            if timeout < 0 {
                // block indefinitely
                read_opts = read_opts.block(0);
            } else {
                read_opts = read_opts.block(timeout as usize * 1000); // milliseconds
            }
        }

        let reply: StreamReadReply =
            match self
                .connection()
                .xread_options(&[&sname], &[">"], &read_opts)
            {
                Ok(r) => r,
                Err(e) => match e.kind() {
                    redis::ErrorKind::TypeError => {
                        // Will read a Nil value on timeout.  That's OK.
                        trace!("{} stream read returned nothing", self);
                        return Ok(None);
                    }
                    _ => {
                        return Err(format!("XREAD error: {e}"));
                    }
                },
            };

        let mut value_op: Option<String> = None;

        for StreamKey { key, ids } in reply.keys {
            trace!("{} read value from stream {}", self, key);

            for StreamId { id, map } in ids {
                trace!("{} read message ID {}", self, id);

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
    fn recv_one_value(
        &mut self,
        timeout: i32,
        stream: Option<&str>,
    ) -> Result<Option<json::JsonValue>, String> {
        let json_string = match self.recv_one_chunk(timeout, stream)? {
            Some(s) => s,
            None => {
                return Ok(None);
            }
        };

        trace!("{self} read json from the bus: {json_string}");

        match json::parse(&json_string) {
            Ok(json_val) => Ok(Some(json_val)),
            Err(err_msg) => {
                return Err(format!("Error parsing JSON: {:?}", err_msg));
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
        stream: Option<&str>,
    ) -> Result<Option<json::JsonValue>, String> {
        let mut option: Option<json::JsonValue>;

        if timeout == 0 {
            // See if any data is ready now
            return self.recv_one_value(timeout, stream);
        } else if timeout < 0 {
            // Keep trying until we have a result.
            loop {
                option = self.recv_one_value(timeout, stream)?;
                if let Some(_) = option {
                    return Ok(option);
                }
            }
        }

        // Keep trying until we have a result or exhaust the timeout.

        let mut seconds = timeout;

        while seconds > 0 {
            let now = time::SystemTime::now();

            option = self.recv_one_value(timeout, stream)?;

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

    pub fn recv(
        &mut self,
        timeout: i32,
        stream: Option<&str>,
    ) -> Result<Option<TransportMessage>, String> {
        let json_op = self.recv_json_value(timeout, stream)?;

        match json_op {
            Some(ref jv) => Ok(TransportMessage::from_json_value(jv)),
            None => Ok(None),
        }
    }

    /// Sends a TransportMessage to the "to" value in the message.
    pub fn send(&mut self, msg: &TransportMessage) -> Result<(), String> {
        self.send_to(msg, msg.to())
    }

    /// Sends a TransportMessage to the specified ClientAddress, regardless
    /// of what value is in the msg.to() field.
    pub fn send_to(&mut self, msg: &TransportMessage, recipient: &str) -> Result<(), String> {
        let json_str = msg.to_json_value().dump();

        trace!("send() writing chunk to={}: {}", recipient, json_str);

        let maxlen = StreamMaxlen::Approx(1000); // TODO CONFIG

        let res: Result<String, _> =
            self.connection()
                .xadd_maxlen(recipient, maxlen, "*", &[("message", json_str)]);

        if let Err(e) = res {
            return Err(format!("Error in send(): {e}"));
        };

        Ok(())
    }

    /// Remove all pending data from the stream while leaving the stream intact.
    pub fn clear_stream(&mut self) -> Result<(), String> {
        let sname = self.address().full().to_string();
        let maxlen = StreamMaxlen::Equals(0);
        let res: Result<i32, _> = self.connection().xtrim(&sname, maxlen);

        if let Err(e) = res {
            return Err(format!("Error in clear_stream(): {e}"));
        }

        Ok(())
    }

    /// Removes our stream, which also removes our consumer group
    pub fn delete_stream(&mut self) -> Result<(), String> {
        let sname = self.address().full().to_string();
        let res: Result<i32, _> = self.connection().del(&sname);

        if let Err(e) = res {
            return Err(format!("Error in delete_stream(): {e}"));
        }

        Ok(())
    }

    /// Delete our stream.
    ///
    /// Redis connectionts are closed on free, so no specific
    /// disconnect action is taken.
    pub fn disconnect(&mut self) -> Result<(), String> {
        self.delete_stream()
    }
}

impl fmt::Display for Bus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Bus {}", self.address())
    }
}
