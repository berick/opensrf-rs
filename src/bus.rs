use std::time;
use std::cmp;
use std::fmt;
use log::{trace, debug, error};
use redis;
use redis::Commands;
use super::conf::BusConfig;
use super::message::TransportMessage;
use super::error;
use super::util;

/// Manages the Redis connection.
pub struct Bus {
    connection: redis::Connection,
    bus_id: String,
}

impl Bus {

    pub fn new(bus_config: &BusConfig, bus_id: String) -> Result<Self, error::Error> {

        let uri = Bus::connection_uri(bus_config)?;
        debug!("Bus::new() connecting to {}", uri);

        let client = redis::Client::open(uri)?;

        let connection = match client.get_connection() {
            Ok(c) => c,
            Err(e) => { return Err(error::Error::BusError(e)); }
        };

        Ok(Bus {
            bus_id: bus_id,
            connection: connection,
        })
    }

    /// Generates the Redis connection URI
    fn connection_uri(bus_config: &BusConfig) -> Result<String, error::Error> {
        let uri: String;

        if let Some(ref s) = bus_config.sock() {
            uri = format!("unix://{}", s);
        } else {
            if let Some(ref h) = bus_config.host() {
                if let Some(ref p) = bus_config.port() {
                    uri = format!("redis://{}:{}/", h, p);
                } else {
                    error!("Bus requires 'sock' or 'host' + 'port'");
                    return Err(error::Error::ClientConfigError);
                }
            } else {
                error!("Bus requires 'sock' or 'host' + 'port'");
                return Err(error::Error::ClientConfigError);
            }
        };

        Ok(uri)
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

        trace!("recv_one_chunk() timeout={} for recipient {}", timeout, self.bus_id());

        let mut value: String;
        let bus_id = self.bus_id().to_string(); // XXX

        if timeout == 0 {

            // non-blocking
            // LPOP returns only the value
            value = match self.connection().lpop(&bus_id, None) {
                Ok(c) => c,
                Err(e) => match e.kind() {
                    redis::ErrorKind::TypeError => {
                        // Will read a Nil value on timeout.  That's OK.
                        return Ok(None);
                    },
                    _ => { return Err(error::Error::BusError(e)); }
                }
            };

        } else {

            // BLPOP returns the name of the popped list and the value.
            // This code assumes we're only recv()'ing for a single endpoint,
            // no wildcard matching on the self.bus_id.

            let resp: Vec<String>;

            if timeout < 0 { // block indefinitely

                resp = match self.connection().blpop(&bus_id, 0) {
                    Ok(r) => r,
                    Err(e) => { return Err(error::Error::BusError(e)); }
                };

            } else { // block up to timeout seconds

                resp = match self.connection().blpop(&bus_id, timeout as usize) {
                    Ok(r) => r,
                    Err(e) => { return Err(error::Error::BusError(e)); }
                };
            };

            if resp.len() == 0 { return Ok(None); } // No message received

            value = resp[1].to_string(); // resp = [key, value]
        }

        trace!("recv_one_value() pulled from bus: {}", value);

        Ok(Some(value))
    }

    /// Returns at most one JSON value pulled from the queue or None if
    /// the list pop times out or the pop is interrupted by a signal.
    fn recv_one_value(&mut self, timeout: i32) ->
        Result<Option<json::JsonValue>, error::Error> {

        let json_string = match self.recv_one_chunk(timeout)? {
            Some(s) => s,
            None => { return Ok(None); }
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
    pub fn recv_json_value(&mut self, timeout: i32) ->
        Result<Option<json::JsonValue>, error::Error> {

        let mut option: Option<json::JsonValue>;

        if timeout == 0 {

            // See if any data is ready now
            return self.recv_one_value(timeout);

        } else if timeout < 0 {

            // Keep trying until we have a result.
            loop {
                option = self.recv_one_value(timeout)?;
                if let Some(_) = option { return Ok(option); }
            }
        }

        // Keep trying until we have a result or exhaust the timeout.

        let mut seconds = timeout;

        while seconds > 0 {

            let now = time::SystemTime::now();

            option = self.recv_one_value(timeout)?;

            match option {
                None => {
                    if seconds < 0 { return Ok(None); }
                    seconds -= now.elapsed().unwrap().as_secs() as i32;
                    continue;
                },
                _ => return Ok(option)
            }
        }

        Ok(None)
    }

    pub fn recv(&mut self, timeout: i32) -> Result<Option<TransportMessage>, error::Error> {

        let json_op = self.recv_json_value(timeout)?;

        match json_op {
            Some(ref jv) => Ok(TransportMessage::from_json_value(jv)),
            None => Ok(None)
        }
    }

    pub fn send(&mut self, msg: &TransportMessage) -> Result<(), error::Error> {

        let recipient = msg.to();
        let json_str = msg.to_json_value().dump();

        trace!("send() writing chunk to={}: {}", recipient, json_str);

        let res: Result<i32, _> = self.connection().rpush(recipient, json_str);

        if let Err(e) = res { return Err(error::Error::BusError(e)); }

        Ok(())
    }

    /// Clears the value for a key.
    pub fn clear(&mut self, key_op: Option<&str>) -> Result<(), error::Error> {

        let key = match key_op {
            Some(k) => k.to_string(),
            None => self.bus_id().to_string(), // mut borrow
        };

        let res: Result<i32, _> = self.connection().del(&key);

        match res {
            Ok(count) => trace!("con.del('{}') returned {}", key, count),
            Err(e) => { return Err(error::Error::BusError(e)) }
        }

        Ok(())
    }
}

impl fmt::Display for Bus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Bus {}", self.bus_id())
    }
}
