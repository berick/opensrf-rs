use log::{trace, warn, error};
use std::collections::HashMap;
use std::time;
use std::fmt;
use super::*;
use super::conf::BusConfig;
use super::bus::Bus;
use super::message::TransportMessage;
use super::message::Message;
use super::message::MessageType;
use super::message::MessageStatus;
use super::message::Method;
use super::message::Payload;
use super::session::Request;
use super::session::Session;
use super::session::SessionType;

const CONNECT_TIMEOUT: i32 = 10;

pub trait DataSerializer {
    fn pack(&self, value: &json::JsonValue) -> json::JsonValue;
    fn unpack(&self, value: &json::JsonValue) -> json::JsonValue;
}

pub struct Client<'a> {
    bus: bus::Bus,

    sessions: HashMap<String, Session>,

    /// Queue of receieved transport messages that have yet to be
    /// processed by any sessions.
    transport_backlog: Vec<message::TransportMessage>,

    pub serializer: Option<&'a DataSerializer>,
}

impl Client<'_> {

    pub fn new(bus_config: &BusConfig) -> Result<Self, error::Error> {

        let bus = Bus::new(bus_config, Bus::new_bus_id("client"))?;

        Ok(Client {
            bus: bus,
            sessions: HashMap::new(),
            transport_backlog: Vec::new(),
            serializer: None,
        })
    }

    pub fn clear_bus(&mut self) -> Result<(), error::Error> {
        self.bus.clear(None)
    }

    fn ses(&self, thread: &str) -> &Session {
        self.sessions.get(thread).unwrap()
    }

    fn ses_mut(&mut self, thread: &str) -> &mut Session {
        self.sessions.get_mut(thread).unwrap()
    }

    fn req(&self, req: &ClientRequest) -> &Request {
        self.ses(req.thread()).requests.get(&req.thread_trace()).unwrap()
    }

    fn req_mut(&mut self, req: &ClientRequest) -> &mut Request {
        self.ses_mut(req.thread()).requests.get_mut(&req.thread_trace()).unwrap()
    }

    pub fn session(&mut self, service: &str) -> ClientSession {

        let ses = Session::new(service);
        let client_ses = ClientSession::new(&ses.thread);

        self.sessions.insert(ses.thread.to_string(), ses);

        client_ses
    }

    /// Returns the first transport message pulled from the pending
    /// messages queue that matches the provided thread.
    fn recv_session_from_backlog(
        &mut self,
        thread: &str,
        mut timeout: i32
    ) -> Option<TransportMessage> {

        if let Some(index) =
            self.transport_backlog.iter().position(|tm| tm.thread() == thread) {

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
        mut timeout: i32
    ) -> Result<Option<TransportMessage>, error::Error> {

        loop {

            let start = time::SystemTime::now();

            let recv_op = self.bus.recv(timeout)?;

            if timeout > 0 {
                timeout -= start.elapsed().unwrap().as_secs() as i32;
            }

            let mut tm = match recv_op {
                Some(m) => m,
                None => { return Ok(None); } // Timed out
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
        mut timeout: i32
    ) -> Result<Option<TransportMessage>, error::Error> {

        trace!("recv_session() timeout={} thread={}", timeout, thread);

        let tm = match self.recv_session_from_backlog(thread, timeout) {
            Some(t) => t,
            None => {
                match self.recv_session_from_bus(thread, timeout)? {
                    Some(t2) => t2,
                    None => { return Ok(None); }
                }
            }
        };

        let ses = self.ses_mut(thread);

        if ses.remote_addr() != tm.from() {
            ses.remote_addr = Some(tm.from().to_string());
        }

        Ok(Some(tm))
    }

    pub fn cleanup(&mut self, client_ses: &ClientSession) {
        self.sessions.remove(client_ses.thread());
    }

    pub fn connect(
        &mut self,
        client_ses: &ClientSession
    ) -> Result<(), error::Error> {

        trace!("Connecting {}", client_ses);

        let mut ses = self.ses_mut(client_ses.thread());
        ses.last_thread_trace += 1;

        let thread_trace = ses.last_thread_trace;
        let remote_addr = ses.remote_addr().to_string();

        ses.requests.insert(thread_trace,
            Request {
                complete: false,
                thread: client_ses.thread().to_string(),
                thread_trace: thread_trace,
            }
        );

        let req = ClientRequest::new(client_ses.thread(), thread_trace);

        let msg = Message::new(MessageType::Connect,
            thread_trace, message::Payload::NoPayload);

        let tm = TransportMessage::new_with_body(
            &remote_addr, self.bus.bus_id(), client_ses.thread(), msg);

        self.bus.send(&tm)?;

        let mut timeout = CONNECT_TIMEOUT;

        while timeout > 0 {

            let start = time::SystemTime::now();

            trace!("connect() calling receive with timeout={}", timeout);
            let recv_op = self.recv(&req, timeout)?;

            let ses = self.ses_mut(client_ses.thread());
            if ses.connected { return Ok(()) }

            timeout -= start.elapsed().unwrap().as_secs() as i32;
        }

        Err(error::Error::ConnectTimeoutError)
    }

    pub fn disconnect(
        &mut self,
        client_ses: &ClientSession
    ) -> Result<(), error::Error> {

        // Disconnects need a thread trace, but no request ID, since
        // we do not track them internally -- they produce no response.
        self.ses_mut(client_ses.thread()).last_thread_trace += 1;

        let ses = self.ses(client_ses.thread());

        let msg = Message::new(MessageType::Disconnect,
            ses.last_thread_trace, message::Payload::NoPayload);

        let tm = TransportMessage::new_with_body(
            &ses.remote_addr(), self.bus.bus_id(), client_ses.thread(), msg);

        self.bus.send(&tm)?;

        // Avoid changing remote_addr until above message is composed.
        self.ses_mut(client_ses.thread()).reset();

        Ok(())
    }

    pub fn request(
        &mut self,
        client_ses: &ClientSession,
        method: &str,
        params: Vec<json::JsonValue>
    ) -> Result<ClientRequest, error::Error> {

        // self.sessions lookup instead of self.get_mut to avoid borrow
        let mut ses = self.sessions.get_mut(client_ses.thread()).unwrap();

        ses.last_thread_trace += 1;

        let mut payload;

        if let Some(s) = self.serializer {
            let mut packed_params = Vec::new();
            for par in params {
                packed_params.push(s.pack(&par));
            }
            payload = Payload::Method(Method::new(method, packed_params));
        } else {
            payload = Payload::Method(Method::new(method, params));
        }

        let req = Message::new(MessageType::Request, ses.last_thread_trace, payload);

        // If we're not connected, all requets go to the root service address.
        let remote_addr = match ses.connected {
            true => ses.remote_addr(),
            false => &ses.service
        };

        let tm = TransportMessage::new_with_body(
            remote_addr, self.bus.bus_id(), client_ses.thread(), req);

        self.bus.send(&tm)?;

        let mut ses = self.ses_mut(client_ses.thread());

        trace!("request() adding request to {}", client_ses);

        ses.requests.insert(ses.last_thread_trace,
            Request {
                complete: false,
                thread: client_ses.thread().to_string(),
                thread_trace: ses.last_thread_trace,
            }
        );

        Ok(ClientRequest::new(client_ses.thread(), ses.last_thread_trace))
    }

    fn recv_from_backlog(
        &mut self,
        req: &ClientRequest
    ) -> Option<message::Message> {

        let ses = self.ses_mut(req.thread());

        trace!("recv_from_backlog() called for {}", req);

        match ses.backlog.iter().position(|resp| resp.thread_trace() == req.thread_trace()) {
            Some(index) => {
                trace!("recv_from_backlog() found response for {}", req);
                return Some(ses.backlog.remove(index));
            }
            None => None,
        }
    }

    pub fn recv(
        &mut self,
        req: &ClientRequest,
        mut timeout: i32
    ) -> Result<Option<json::JsonValue>, error::Error> {

        let mut resp: Result<Option<json::JsonValue>, error::Error> = Ok(None);

        if self.complete(req) { return resp; }

        let thread_trace = req.thread_trace();

        trace!("recv() called for {}", req);

        // Reply for this request was previously pulled from the
        // bus and tossed into our queue.
        if let Some(msg) = self.recv_from_backlog(req) {
            return self.handle_reply(req, &msg);
        }

        while timeout >= 0 {

            let start = time::SystemTime::now();

            // Could return a response to any request that's linked
            // to this session.
            let tm = match self.recv_session(req.thread(), timeout)? {
                Some(m) => m,
                None => { return resp; }
            };

            timeout -= start.elapsed().unwrap().as_secs() as i32;

            trace!("{}", tm.to_json_value().dump());

            let mut msg_list = tm.body().to_owned();

            if msg_list.len() == 0 { continue; }

            let msg = msg_list.remove(0);

            let mut found = false;
            if msg.thread_trace() == thread_trace {

                found = true;
                resp = self.handle_reply(req, &msg);

            } else {

                /// Pulled a reply to a request made by this client session
                /// but not matching the requested thread trace.

                trace!("recv() found a reply for a request {}, stashing",
                    msg.thread_trace());

                self.ses_mut(req.thread()).backlog.push(msg);
            }

            while msg_list.len() > 0 {
                trace!("recv() adding to session backlog thread_trace={}",
                    msg_list[0].thread_trace());

                self.ses_mut(req.thread()).backlog.push(msg_list.remove(0));
            }

            if found { break; }
        }

        return resp;
    }

    fn handle_reply(
        &mut self,
        req: &ClientRequest,
        msg: &message::Message
    ) -> Result<Option<json::JsonValue>, error::Error> {

        trace!("handle_reply() {} mtype={}", req, msg.mtype());

        if let Payload::Result(resp) = msg.payload() {
            trace!("handle_reply() found response for {}", req);
            match self.serializer {
                Some(s) => { return Ok(Some(s.unpack(resp.content()))); }
                None => { return Ok(Some(resp.content().clone())); }
            }
        };

        let ses = self.ses_mut(req.thread());

        if let Payload::Status(stat) = msg.payload() {

            match stat.status() {
                MessageStatus::Ok => {
                    trace!("handle_reply() marking {} as connected", req);
                    ses.connected = true;
                },
                MessageStatus::Continue => {}, // TODO
                MessageStatus::Complete => {
                    trace!("Marking {} as complete", req);
                    self.req_mut(req).complete = true;
                },
                MessageStatus::Timeout => {
                    self.ses_mut(req.thread()).reset();
                    warn!("Stateful session ended by server on keepalive timeout");
                    return Err(error::Error::RequestTimeoutError);
                },
                _ => {
                    ses.reset();
                    warn!("Unexpected response status {}", stat.status());
                    return Err(error::Error::RequestTimeoutError);
                }
            }

            return Ok(None);
        }

        error!("handle_reply() {} unexpected response {}",
            req.thread_trace(), msg.to_json_value().dump());

        ses.reset();

        return Err(error::Error::BadResponseError);
    }

    pub fn complete(&self, req: &ClientRequest) -> bool {
        let ses = self.ses(req.thread());
        match ses.requests.get(&req.thread_trace()) {
            Some(r) => r.complete && !ses.has_pending_replies(r.thread_trace),
            None => false,
        }
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

pub struct ClientRequest {
    thread: String,
    thread_trace: usize,
}

impl ClientRequest {
    pub fn new(thread: &str, thread_trace: usize) -> Self {
        ClientRequest {
            thread_trace,
            thread: thread.to_string(),
        }
    }

    pub fn thread(&self) -> &str {
        &self.thread
    }
    pub fn thread_trace(&self) -> usize {
        self.thread_trace
    }
}

impl fmt::Display for ClientRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "request thread={} thread_trace={}", self.thread, self.thread_trace)
    }
}

