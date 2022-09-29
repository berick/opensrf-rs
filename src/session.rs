use super::addr::BusAddress;
use super::client::Client;
use super::message::Message;
use super::message::MessageStatus;
use super::message::MessageType;
use super::message::Method;
use super::message::Payload;
use super::message::TransportMessage;
use super::util;
use json::JsonValue;
use log::{debug, error, trace, warn};
use std::cell::{Ref, RefCell, RefMut};
use std::fmt;
use std::rc::Rc;

const CONNECT_TIMEOUT: i32 = 10;
const DEFAULT_REQUEST_TIMEOUT: i32 = 60;

/// Response data propagated from a session to the calling Request.
pub struct Response {
    value: Option<JsonValue>,
    complete: bool,
}

/// Models a single API call through which the caller can receive responses.
pub struct Request {
    /// Link to our session so we can ask it for bus data.
    session: Rc<RefCell<Session>>,

    /// Have we received all of the replies yet?
    complete: bool,

    /// Unique ID per thread/session.
    thread_trace: usize,
}

impl Request {
    pub fn new(session: Rc<RefCell<Session>>, thread_trace: usize) -> Request {
        Request {
            session,
            complete: false,
            thread_trace,
        }
    }

    /// Receive the next response to this Request
    ///
    /// timeout:
    ///     <0 == wait indefinitely
    ///      0 == do not wait/block
    ///     >0 == wait up to this many seconds for a reply.
    pub fn recv(&mut self, timeout: i32) -> Result<Option<JsonValue>, String> {
        if self.complete {
            // If we are marked complete, it means we've read all the
            // replies, the last of which was a request-complete message.
            // Nothing left to read.
            return Ok(None);
        }

        let response = self.session.borrow_mut().recv(self.thread_trace, timeout)?;

        if let Some(r) = response {
            if r.complete {
                self.complete = true;
            }
            return Ok(r.value);
        }

        Ok(None)
    }
}

pub enum SessionType {
    Client,
    Server,
}

pub struct Session {
    /// Link to our Client so we can ask it to pull data from the Bus.
    client: Rc<RefCell<Client>>,

    /// Client or Server
    /// Pending server code dev.
    _session_type: SessionType,

    /// Each session is identified on the network by a random thread string.
    thread: String,

    /// Have we successfully established a connection withour
    /// destination service?
    connected: bool,

    /// Service name.
    ///
    /// For Clients, this doubles as the remote_addr when initiating
    /// a new conversation.
    /// For Servers, this is the name of the service we host.
    service: String,

    /// Top-level bus address for our service.
    service_addr: BusAddress,

    /// Worker-specific bus address for our session.
    ///
    /// Only set if we are connected directly to a remote worker.
    remote_addr: Option<BusAddress>,

    /// Most recently used per-thread request id.
    ///
    /// Each new Request within a Session gets a new thread_trace.
    /// Replies have the same thread_trace as their request.
    last_thread_trace: usize,

    /// Replies to this thread which have not yet been pulled by
    /// any requests.
    backlog: Vec<Message>,
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session({} {})", &self.service, &self.thread)
    }
}

impl Session {
    pub fn new(client: Rc<RefCell<Client>>, service: &str) -> SessionHandle {
        let ses = Session {
            client,
            _session_type: SessionType::Client,
            service: String::from(service),
            remote_addr: None,
            service_addr: BusAddress::new_for_service(&service),
            connected: false,
            last_thread_trace: 0,
            backlog: Vec::new(),
            thread: util::random_number(16),
        };

        trace!("Created new session {ses}");

        SessionHandle {
            session: Rc::new(RefCell::new(ses)),
        }
    }

    pub fn client(&self) -> Ref<Client> {
        self.client.borrow()
    }

    pub fn client_mut(&self) -> RefMut<Client> {
        self.client.borrow_mut()
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    pub fn thread(&self) -> &str {
        &self.thread
    }

    pub fn connected(&self) -> bool {
        self.connected
    }

    pub fn reset(&mut self) {
        trace!("{self} resetting...");
        self.remote_addr = None;
        self.connected = false;
        self.backlog.clear();
    }

    /// Returns the address of the remote end if we are connected.  Otherwise,
    /// returns the default remote address of the service we are talking to.
    pub fn remote_addr(&self) -> &BusAddress {
        if self.connected {
            if let Some(ref ra) = self.remote_addr {
                return ra;
            }
        }

        &self.service_addr
    }

    fn recv_from_backlog(&mut self, thread_trace: usize) -> Option<Message> {
        if let Some(index) = self
            .backlog
            .iter()
            .position(|m| m.thread_trace() == thread_trace)
        {
            trace!("{self} found a reply in the backlog for request {thread_trace}");

            Some(self.backlog.remove(index))
        } else {
            None
        }
    }

    pub fn recv(&mut self, thread_trace: usize, timeout: i32) -> Result<Option<Response>, String> {
        let mut timer = util::Timer::new(timeout);

        loop {
            trace!(
                "{self} in recv() for trace {thread_trace} with {} remaining",
                timer.remaining()
            );

            if let Some(msg) = self.recv_from_backlog(thread_trace) {
                return self.unpack_reply(&mut timer, msg);
            }

            if timer.done() {
                // Nothing in the backlog and all out of time.
                return Ok(None);
            }

            let recv_op = self.client_mut().recv_session(&mut timer, self.thread())?;

            if recv_op.is_none() {
                continue;
            }

            let tmsg = recv_op.unwrap();

            // Toss the messages onto our backlog as we receive them.

            if self.remote_addr().full() != tmsg.from() {
                // Response from a specific worker
                self.remote_addr = Some(BusAddress::new_from_string(tmsg.from())?);
            }

            trace!("{self} pulled messages from the data bus");

            for msg in tmsg.body() {
                self.backlog.push(msg.to_owned());
            }

            // Loop back around and see if we can pull the message
            // we want from our backlog.
        }
    }

    fn unpack_reply(
        &mut self,
        timer: &mut util::Timer,
        msg: Message,
    ) -> Result<Option<Response>, String> {
        if let Payload::Result(resp) = msg.payload() {
            // .to_owned() because this message is about to get dropped.
            let mut value = resp.content().to_owned();
            if let Some(s) = self.client().serializer() {
                value = s.unpack(&value);
            }

            return Ok(Some(Response {
                value: Some(value),
                complete: false,
            }));
        }

        let err_msg;
        let trace = msg.thread_trace();

        if let Payload::Status(stat) = msg.payload() {
            match self.unpack_status_message(trace, timer, stat.status()) {
                Ok(v) => {
                    return Ok(v);
                }
                Err(e) => err_msg = e,
            }
        } else {
            err_msg = format!("{self} unexpected response for request {trace}: {msg:?}");
        }

        self.reset();

        return Err(err_msg);
    }

    fn unpack_status_message(
        &mut self,
        trace: usize,
        timer: &mut util::Timer,
        stat: &MessageStatus,
    ) -> Result<Option<Response>, String> {
        let err_msg;

        match stat {
            MessageStatus::Ok => {
                trace!("{self} Marking self as connected");
                self.connected = true;
                return Ok(None);
            }
            MessageStatus::Continue => {
                timer.reset();
                return Ok(None);
            }
            MessageStatus::Complete => {
                trace!("{self} request {trace} complete");
                return Ok(Some(Response {
                    value: None,
                    complete: true,
                }));
            }
            MessageStatus::Timeout => {
                err_msg = format!("{self} request {trace} timed out");
            }
            MessageStatus::NotFound => {
                err_msg = format!("{self} method bot found for request {trace}: {stat:?}");
            }
            _ => {
                err_msg = format!("{self} unexpected status message for request {trace} {stat:?}");
            }
        }

        Err(err_msg)
    }

    fn incr_thread_trace(&mut self) -> usize {
        self.last_thread_trace += 1;
        self.last_thread_trace
    }

    /// Issue a new API call and return the thread_trace of the sent request.
    pub fn request<T>(&mut self, method: &str, params: Vec<T>) -> Result<usize, String>
    where
        T: Into<JsonValue>,
    {
        debug!("{self} sending request {method}");

        let trace = self.incr_thread_trace();

        let mut pvec = Vec::new();
        for p in params {
            let mut jv = json::from(p);
            if let Some(s) = self.client().serializer() {
                jv = s.pack(&jv);
            }
            pvec.push(jv);
        }

        let payload = Payload::Method(Method::new(method, pvec));

        let req = Message::new(MessageType::Request, trace, payload);

        let mut client = self.client_mut();

        let tm = TransportMessage::new_with_body(
            self.remote_addr().full(),
            client.address(),
            self.thread(),
            req,
        );

        let bus = match self.remote_addr().is_client() {
            true => client.get_connection(self.remote_addr().domain().unwrap())?,
            false => client.primary_connection_mut(),
        };

        bus.send(&tm)?;

        Ok(trace)
    }

    /// Establish a connected session with a remote worker.
    pub fn connect(&mut self) -> Result<(), String> {
        if self.connected() {
            warn!("{self} is already connected");
            return Ok(());
        }

        debug!("{self} sending CONNECT");

        let trace = self.incr_thread_trace();

        let msg = Message::new(MessageType::Connect, trace, Payload::NoPayload);

        let tm = TransportMessage::new_with_body(
            self.remote_addr().full(),
            self.client().address(),
            self.thread(),
            msg,
        );

        // A CONNECT always comes first, so we always drop it onto our
        // primary domain and let the router figure it out.
        self.client_mut().primary_connection_mut().send(&tm)?;

        self.recv(trace, CONNECT_TIMEOUT)?;

        if self.connected {
            Ok(())
        } else {
            self.reset();
            Err(format!("CONNECT timed out"))
        }
    }

    /// Send a DISCONNECT to our remote worker.
    ///
    /// Does not wait for any response.  NO-OP if not connected.
    pub fn disconnect(&mut self) -> Result<(), String> {
        if !self.connected() {
            // Nothing to disconnect
            return Ok(());
        }

        debug!("{self} sending DISCONNECT");

        let trace = self.incr_thread_trace();

        let msg = Message::new(MessageType::Disconnect, trace, Payload::NoPayload);

        let tm = TransportMessage::new_with_body(
            self.remote_addr().full(),
            self.client().address(),
            self.thread(),
            msg,
        );

        if let Some(domain) = self.remote_addr().domain() {
            // There should always be a domain in this context

            let mut client = self.client_mut();

            // We may be disconnecting from a remote domain.
            let bus = client.get_connection(&domain)?;

            bus.send(&tm)?;
        }

        // Fire and forget.  All done.

        self.reset();

        Ok(())
    }
}

pub struct SessionHandle {
    session: Rc<RefCell<Session>>,
}

impl SessionHandle {
    /// Issue a new API call and return the Request
    ///
    /// params is a Vec of JSON-able things.  E.g. vec![1,2,3], vec![json::object!{a: "b"}]
    pub fn request<T>(&mut self, method: &str, params: Vec<T>) -> Result<Request, String>
    where
        T: Into<JsonValue>,
    {
        Ok(Request {
            complete: false,
            session: self.session.clone(),
            thread_trace: self.session.borrow_mut().request(method, params)?,
        })
    }

    /// Send a request and receive a ResponseIterator for iterating
    /// the responses to the method.
    ///
    /// Uses the default request timeout DEFAULT_REQUEST_TIMEOUT.
    pub fn sendrecv<T>(&mut self, method: &str, params: Vec<T>) -> Result<ResponseIterator, String>
    where
        T: Into<JsonValue>,
    {
        Ok(ResponseIterator::new(self.request(method, params)?))
    }

    pub fn connect(&self) -> Result<(), String> {
        self.session.borrow_mut().connect()
    }

    pub fn disconnect(&self) -> Result<(), String> {
        self.session.borrow_mut().disconnect()
    }
}

/// Iterates over a series of replies to an API request.
pub struct ResponseIterator {
    request: Request,
}

impl Iterator for ResponseIterator {
    type Item = JsonValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self.request.recv(DEFAULT_REQUEST_TIMEOUT) {
            Ok(op) => op,
            Err(e) => {
                error!("ResponseIterator failed with {e}");
                None
            }
        }
    }
}

impl ResponseIterator {
    pub fn new(request: Request) -> Self {
        ResponseIterator { request }
    }
}
