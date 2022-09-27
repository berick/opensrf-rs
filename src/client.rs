use super::addr::BusAddress;
use super::bus::Bus;
use super::conf::ClientConfig;
use super::message::Message;
use super::message::MessageStatus;
use super::message::MessageType;
use super::message::Method;
use super::message::Payload;
use super::message::TransportMessage;
use super::session::Request;
use super::session::Session;
use super::*;
use json::JsonValue;
use log::{error, info, trace, warn};
use std::collections::HashMap;
use std::fmt;
use std::time;

const CONNECT_TIMEOUT: i32 = 10;

pub trait DataSerializer {
    fn pack(&self, value: &JsonValue) -> JsonValue;
    fn unpack(&self, value: &JsonValue) -> JsonValue;
}

enum UnpackedReply {
    Value(json::JsonValue),
    ResetTimeout,
    Noop,
}

pub struct Client<'a> {
    bus: bus::Bus,

    /// Connections to remote domains.
    remote_bus_map: HashMap<String, bus::Bus>,

    config: ClientConfig,

    sessions: HashMap<String, Session>,

    /// Contains a value if this client is working on behalf of a
    /// running service.  Otherwise, it's assume to be a standalone
    /// OpenSRF client.
    for_service: Option<String>,

    /// Queue of receieved transport messages that have yet to be
    /// processed by any sessions.
    transport_backlog: Vec<message::TransportMessage>,

    pub serializer: Option<&'a dyn DataSerializer>,
}

impl Client<'_> {
    pub fn new(config: ClientConfig) -> Result<Self, error::Error> {
        let bus = Bus::new(config.bus_config(), None)?;
        Client::new_common(config, bus)
    }

    pub fn new_for_service(config: ClientConfig, service: &str) -> Result<Self, error::Error> {
        let bus = Bus::new(config.bus_config(), Some(service))?;
        Client::new_common(config, bus)
    }

    fn new_common(config: ClientConfig, bus: Bus) -> Result<Self, error::Error> {
        Ok(Client {
            config,
            bus: bus,
            sessions: HashMap::new(),
            transport_backlog: Vec::new(),
            remote_bus_map: HashMap::new(),
            serializer: None,
            for_service: None,
        })
    }

    pub fn primary_bus(&self) -> &bus::Bus {
        &self.bus
    }

    pub fn get_connection(&mut self, domain: &str) -> Result<&mut bus::Bus, error::Error> {
        // Our primary bus address always has a domain.
        if domain.eq(self.bus.address().domain().unwrap()) {
            Ok(&mut self.bus)
        } else {
            if self.remote_bus_map.contains_key(domain) {
                return Ok(self.remote_bus_map.get_mut(domain).unwrap());
            }

            self.add_connection(domain)
        }
    }

    fn add_connection(&mut self, domain: &str) -> Result<&mut bus::Bus, error::Error> {
        let bus_conf = self.config.bus_config().clone();
        let bus = Bus::new(&bus_conf, self.for_service.as_deref())?;

        info!("Opened connection to new domain: {}", domain);

        self.remote_bus_map.insert(domain.to_string(), bus);
        self.get_connection(domain)
    }

    pub fn clear_bus(&mut self) -> Result<(), error::Error> {
        self.bus.clear_stream()
    }

    pub fn disconnect_bus(&mut self) -> Result<(), error::Error> {
        self.bus.disconnect()
    }

    fn ses(&self, thread: &str) -> &Session {
        self.sessions.get(thread).unwrap()
    }

    fn ses_mut(&mut self, thread: &str) -> &mut Session {
        self.sessions.get_mut(thread).unwrap()
    }

    fn req_mut(&mut self, req: &ClientRequest) -> Option<&mut Request> {
        self.ses_mut(req.thread())
            .requests
            .get_mut(&req.thread_trace())
    }

    /// Create a new client session for the requested service.
    pub fn session(&mut self, service: &str) -> ClientSession {
        let ses = Session::new(service);
        let client_ses = ClientSession::new(&ses.thread);

        self.sessions.insert(ses.thread.to_string(), ses);

        client_ses
    }

    /// Returns the first transport message pulled from the pending
    /// messages queue that matches the provided thread.
    fn recv_session_from_backlog(&mut self, thread: &str) -> Option<TransportMessage> {
        if let Some(index) = self
            .transport_backlog
            .iter()
            .position(|tm| tm.thread() == thread)
        {
            trace!("Found a stashed reply for {}", thread);

            Some(self.transport_backlog.remove(index))
        } else {
            None
        }
    }

    /// Returns the first transport message pulled from the Bus that
    /// matches the provided thread.
    ///
    /// Messages that don't match the provided thread are pushed
    /// onto the pending transport message queue.
    pub fn recv_session_from_bus(
        &mut self,
        thread: &str,
        mut timeout: i32,
    ) -> Result<Option<TransportMessage>, error::Error> {
        loop {
            let start = time::SystemTime::now();

            let recv_op = self.bus.recv(timeout, None)?;

            if timeout > 0 {
                timeout -= start.elapsed().unwrap().as_secs() as i32;
            }

            let tm = match recv_op {
                Some(m) => m,
                None => {
                    return Ok(None);
                } // Timed out
            };

            trace!("recv_session_from_bus() read thread = {}", tm.thread());

            if tm.thread() == thread {
                trace!("recv_session_from_bus() found a response");

                return Ok(Some(tm));
            } else {
                self.transport_backlog.push(tm);
            }
        }
    }

    /// Returns the first transport message pulled from either the
    /// backlog or the bus.
    pub fn recv_session(
        &mut self,
        thread: &str,
        timeout: i32,
    ) -> Result<Option<TransportMessage>, error::Error> {
        trace!("recv_session() timeout={} thread={}", timeout, thread);

        let tm = match self.recv_session_from_backlog(thread) {
            Some(t) => t,
            None => match self.recv_session_from_bus(thread, timeout)? {
                Some(t2) => t2,
                None => {
                    return Ok(None);
                }
            },
        };

        let ses = self.ses_mut(thread);

        if ses.remote_addr().full() != tm.from() {
            // TODO return error on bad address
            ses.remote_addr = Some(BusAddress::new_from_string(tm.from()).unwrap());
        }

        Ok(Some(tm))
    }

    /// Remove session data from the local cache.
    ///
    /// This is required when completing a session to avoid unbound
    /// memory consumption over time.
    pub fn cleanup(&mut self, client_ses: &ClientSession) {
        self.sessions.remove(client_ses.thread());
    }

    /// Establish a connected session with a remote service.
    pub fn connect(&mut self, client_ses: &ClientSession) -> Result<(), error::Error> {
        trace!("Connecting {}", client_ses);

        let my_addr = self.bus.address().full().to_string(); // mut borrow

        let mut ses = self.ses_mut(client_ses.thread());
        ses.last_thread_trace += 1;

        let remote_addr = ses.remote_addr().full().to_string(); // mut borrow

        let thread_trace = ses.last_thread_trace;

        ses.requests.insert(
            thread_trace,
            Request {
                complete: false,
                thread: client_ses.thread().to_string(),
                thread_trace: thread_trace,
            },
        );

        let req = ClientRequest::new(client_ses.thread(), thread_trace);

        let msg = Message::new(
            MessageType::Connect,
            thread_trace,
            message::Payload::NoPayload,
        );

        let tm = TransportMessage::new_with_body(&remote_addr, &my_addr, client_ses.thread(), msg);

        self.bus.send(&tm)?;

        let mut timeout = CONNECT_TIMEOUT;

        while timeout > 0 {
            let start = time::SystemTime::now();

            trace!("connect() calling receive with timeout={}", timeout);

            // Ignore the response since the state of the session will
            // be updated to 'connected' lower down.
            self.recv(&req, timeout)?;

            let ses = self.ses_mut(client_ses.thread());
            if ses.connected {
                return Ok(());
            }

            timeout -= start.elapsed().unwrap().as_secs() as i32;
        }

        Err(error::Error::ConnectTimeoutError)
    }

    /// Disconnect a connected session.
    pub fn disconnect(&mut self, client_ses: &ClientSession) -> Result<(), error::Error> {
        if !self.ses(client_ses.thread()).connected {
            return Ok(());
        }

        // Disconnects need a thread trace, but no request ID, since
        // we do not track them internally.  IOW, they produce no response.
        self.ses_mut(client_ses.thread()).last_thread_trace += 1;

        let ses = self.ses(client_ses.thread());

        let msg = Message::new(
            MessageType::Disconnect,
            ses.last_thread_trace,
            message::Payload::NoPayload,
        );

        let tm = TransportMessage::new_with_body(
            ses.remote_addr().full(),
            self.bus.address().full(),
            client_ses.thread(),
            msg,
        );

        // A disconnect only makes sense in the context of communicating
        // directly with a client.
        let domain = ses.remote_addr().domain().unwrap().to_string();
        let bus = self.get_connection(&domain)?;
        bus.send(&tm)?;

        // Avoid changing remote_addr until above message is composed.
        self.ses_mut(client_ses.thread()).reset();

        Ok(())
    }

    /// Send a request message to the remote service.
    pub fn request<T>(
        &mut self,
        client_ses: &ClientSession,
        method: &str,
        params: Vec<T>,
    ) -> Result<ClientRequest, error::Error>
    where
        T: Into<JsonValue>,
    {
        let mut ses = self.sessions.get_mut(client_ses.thread()).unwrap();

        ses.last_thread_trace += 1;

        let mut param_vec: Vec<JsonValue> = Vec::new();
        for param in params {
            param_vec.push(json::from(param));
        }

        let payload;

        if let Some(s) = self.serializer {
            let mut packed_params = Vec::new();
            for par in param_vec {
                packed_params.push(s.pack(&par));
            }
            payload = Payload::Method(Method::new(method, packed_params));
        } else {
            payload = Payload::Method(Method::new(method, param_vec));
        }

        let req = Message::new(MessageType::Request, ses.last_thread_trace, payload);

        let tm = TransportMessage::new_with_body(
            ses.remote_addr().full(),
            // All messages we send have a from address on our primary domain
            self.bus.address().full(),
            client_ses.thread(),
            req,
        );

        if ses.remote_addr().is_client() {
            // We are in a connected session.
            // Our remote end could be on a different domain.
            let domain = ses.remote_addr().domain().unwrap().to_string();
            let bus = self.get_connection(&domain)?;
            bus.send(&tm)?;
        } else {
            self.bus.send(&tm)?;
        }

        let ses = self.ses_mut(client_ses.thread());

        trace!("request() adding request to {}", client_ses);

        ses.requests.insert(
            ses.last_thread_trace,
            Request {
                complete: false,
                thread: client_ses.thread().to_string(),
                thread_trace: ses.last_thread_trace,
            },
        );

        let mut r = ClientRequest::new(client_ses.thread(), ses.last_thread_trace);
        r.method = Some(method.to_string());

        Ok(r)
    }

    fn recv_from_backlog(&mut self, req: &ClientRequest) -> Option<message::Message> {
        let ses = self.ses_mut(req.thread());

        trace!("recv_from_backlog() called for {}", req);

        match ses
            .backlog
            .iter()
            .position(|resp| resp.thread_trace() == req.thread_trace())
        {
            Some(index) => {
                trace!("recv_from_backlog() found response for {}", req);
                return Some(ses.backlog.remove(index));
            }
            None => None,
        }
    }

    /// Receive up to one response, then mark the request as complete.
    ///
    /// Any remaining responses will be discarded.
    pub fn recv_one(
        &mut self,
        req: &ClientRequest,
        timeout: i32,
    ) -> Result<Option<JsonValue>, error::Error> {
        match self.recv(req, timeout)? {
            Some(resp) => {
                if let Some(r) = self.req_mut(req) {
                    r.complete = true;
                }
                Ok(Some(resp))
            }
            None => Ok(None),
        }
    }

    /// Receive the next response to the provided ClientRequest.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Max time to wait for a response.  A value of -1 means
    ///   block indefinitely.
    pub fn recv(
        &mut self,
        req: &ClientRequest,
        timeout: i32,
    ) -> Result<Option<JsonValue>, error::Error> {
        let mut resp: Result<Option<JsonValue>, error::Error> = Ok(None);

        if self.complete(req) {
            return resp;
        }

        let thread_trace = req.thread_trace();

        trace!("recv() called for {}", req);

        // Reply for this request was previously pulled from the
        // bus and tossed into our queue.  Go until we find a returnable
        // response or drain the backlog.
        loop {
            if let Some(msg) = self.recv_from_backlog(req) {
                match self.handle_reply(req, &msg)? {
                    UnpackedReply::Value(r) => {
                        return Ok(Some(r));
                    }
                    // Timeout has not yet been decremented, so there's
                    // no need to check for ResetTimeout
                    _ => {
                        // May have pulled a Complete message from the backlog.
                        if self.complete(req) {
                            return resp;
                        }
                    }
                }
            } else {
                break;
            }
        }

        let mut mut_timeout = timeout;
        while mut_timeout >= 0 {
            let start = time::SystemTime::now();

            // Could return a response to any request that's linked
            // to this session.
            let tm = match self.recv_session(req.thread(), mut_timeout)? {
                Some(m) => m,
                None => {
                    return resp;
                }
            };

            mut_timeout -= start.elapsed().unwrap().as_secs() as i32;

            trace!("{}", tm.to_json_value().dump());

            let mut msg_list = tm.body().to_owned();

            if msg_list.len() == 0 {
                continue;
            }

            let msg = msg_list.remove(0);

            let mut found = false;
            if msg.thread_trace() == thread_trace {
                found = true;
                match self.handle_reply(req, &msg)? {
                    UnpackedReply::Value(r) => resp = Ok(Some(r)),
                    UnpackedReply::ResetTimeout => mut_timeout = timeout,
                    _ => {}
                }
            } else {
                // Pulled a reply to a request made by this client session
                // but not matching the requested thread trace.

                trace!(
                    "recv() found a reply for a request {}, stashing",
                    msg.thread_trace()
                );

                self.ses_mut(req.thread()).add_to_backlog(msg);
            }

            // A recv() that pulls data from bus can return multiple
            // responses.  This method only returns 1.  Put any
            // remainders in the backlog for later receiving.
            while msg_list.len() > 0 {
                trace!(
                    "recv() adding to session backlog thread_trace={}",
                    msg_list[0].thread_trace()
                );

                self.ses_mut(req.thread())
                    .add_to_backlog(msg_list.remove(0));
            }

            if found {
                break;
            }
        }

        return resp;
    }

    /// Unpack and process a response message.
    fn handle_reply(
        &mut self,
        req: &ClientRequest,
        msg: &message::Message,
    ) -> Result<UnpackedReply, error::Error> {
        trace!("handle_reply() {} mtype={}", req, msg.mtype());

        if let Payload::Result(resp) = msg.payload() {
            trace!("handle_reply() found response for {}", req);
            match self.serializer {
                Some(s) => {
                    return Ok(UnpackedReply::Value(s.unpack(resp.content())));
                }
                None => {
                    return Ok(UnpackedReply::Value(resp.content().clone()));
                }
            }
        };

        let ses = self.ses_mut(req.thread());

        if let Payload::Status(stat) = msg.payload() {
            match stat.status() {
                MessageStatus::Ok => {
                    trace!("handle_reply() marking {} as connected", req);
                    ses.connected = true;
                }
                MessageStatus::Continue => {
                    return Ok(UnpackedReply::ResetTimeout);
                }
                MessageStatus::Complete => {
                    trace!("Marking {} as complete", req);
                    if let Some(r) = self.req_mut(req) {
                        r.complete = true;
                    }
                }
                MessageStatus::Timeout => {
                    ses.reset();
                    warn!("Stateful session ended by server on keepalive timeout");
                    return Err(error::Error::RequestTimeoutError);
                }
                MessageStatus::NotFound => {
                    ses.reset();
                    if let Some(m) = req.method() {
                        warn!("Method Not Found: {}", m);
                    }
                    return Err(error::Error::MethodNotFoundError);
                }
                _ => {
                    ses.reset();
                    warn!("Unexpected response status {}", stat.status());
                    return Err(error::Error::BadResponseError);
                }
            }

            return Ok(UnpackedReply::Noop);
        }

        error!(
            "handle_reply() {} unexpected response {}",
            req.thread_trace(),
            msg.to_json_value().dump()
        );

        ses.reset();

        return Err(error::Error::BadResponseError);
    }

    /// Returns true if the provided request is marked as complete
    /// and has no more messages in the backlog of replies.
    pub fn complete(&self, req: &ClientRequest) -> bool {
        let ses = self.ses(req.thread());
        match ses.requests.get(&req.thread_trace()) {
            Some(r) => r.complete && !ses.has_pending_replies(r.thread_trace),
            None => false,
        }
    }

    pub fn send_router_command(
        &mut self,
        domain: &str,
        rtr_command: &str,
        rtr_class: &str,
    ) -> Result<(), error::Error> {
        let addr = BusAddress::new_for_router(domain);

        // Always use the address of our primary Bus
        let mut tmsg = TransportMessage::new(
            addr.full(),
            self.bus.address().full(),
            &util::random_number(16),
        );

        tmsg.set_router_command(rtr_command);
        tmsg.set_router_class(rtr_class);

        let bus = self.get_connection(domain)?;
        bus.send(&tmsg)
    }
}

// Immutable context structs the caller owns for managing
// sessions and requests.  These link to mutable variants
// internally so we don't have to bandy about mutable refs.
pub struct ClientSession {
    thread: String,
}

impl ClientSession {
    pub fn new(thread: &str) -> Self {
        ClientSession {
            thread: thread.to_string(),
        }
    }
    pub fn thread(&self) -> &str {
        &self.thread
    }
}

impl fmt::Display for ClientSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "session thread={}", self.thread)
    }
}

/// API request for sending to a remote service.
pub struct ClientRequest {
    thread: String,
    thread_trace: usize,
    method: Option<String>,
}

impl ClientRequest {
    pub fn new(thread: &str, thread_trace: usize) -> Self {
        ClientRequest {
            thread_trace,
            thread: thread.to_string(),
            method: None,
        }
    }

    pub fn thread(&self) -> &str {
        &self.thread
    }

    pub fn thread_trace(&self) -> usize {
        self.thread_trace
    }

    pub fn method(&self) -> &Option<String> {
        &self.method
    }

    pub fn set_method(&mut self, method: &str) {
        self.method = Some(method.to_string());
    }
}

impl fmt::Display for ClientRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "request thread={} thread_trace={}",
            self.thread, self.thread_trace
        )
    }
}
