use std::fmt;
use json::JsonValue;
use super::*;
use super::addr::BusAddress;
use super::message::Payload;
use super::message::MessageStatus;
use log::{trace, debug, info, warn};
use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

const CONNECT_TIMEOUT: i32 = 10;

pub struct Response {
    value: Option<JsonValue>,
    complete: bool,
}

pub struct Request {
    session: Rc<RefCell<Session>>,
    complete: bool,
    thread_trace: usize,
    method: String,
}

impl Request {

    pub fn new(session: Rc<RefCell<Session>>, method: &str, thread_trace: usize) -> Request {
        Request {
            session,
            method: method.to_string(),
            complete: false,
            thread_trace,
        }
    }

    pub fn recv(&mut self, timeout: i32) -> Result<Option<JsonValue>, String> {

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
    client: Rc<RefCell<Client>>,

    session_type: SessionType,

    /// Each session is identified on the network by a random thread string.
    thread: String,

    connected: bool,

    /// Service name.
    ///
    /// For Clients, this doubles as the remote_addr when initiating
    /// a new conversation.
    /// For Servers, this is the name of the service we host.
    service: String,

    /// Bus ID for our service.
    service_addr: BusAddress,

    /// Worker-specific bus address for our session.
    /// Only set once we are communicating with a specific worker.
    remote_addr: Option<BusAddress>,

    /// Each new Request within a Session gets a new thread_trace.
    /// Replies have the same thread_trace as their request.
    ///
    /// This is effectively a request ID.
    last_thread_trace: usize,

    /// Replies to this thread which have not yet been pulled by
    /// any requests.
    backlog: Vec<message::Message>,
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
            session_type: SessionType::Client,
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
            session: Rc::new(RefCell::new(ses))
        }
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

    fn recv_from_backlog(&mut self, thread_trace: usize) -> Option<message::Message> {

        if let Some(index) = self.backlog.iter()
            .position(|m| m.thread_trace() == thread_trace) {

            trace!("{self} found a reply in the backlog for request {thread_trace}");

            Some(self.backlog.remove(index))

        } else {
            None
        }
    }


    pub fn recv(&mut self, thread_trace: usize,
        timeout: i32) -> Result<Option<Response>, String> {

        let mut timer = util::Timer::new(timeout);

        loop {

            if let Some(msg) = self.recv_from_backlog(thread_trace) {
                return self.unpack_reply(&mut timer, msg);
            }

            if timer.done() {
                // Nothing in the backlog and all out of time.
                return Ok(None);
            }

            if let Some(tmsg) =
                self.client.borrow_mut().recv_session(&mut timer, self.thread())? {
                // Toss the messages onto our backlog as we receive them.

                if self.remote_addr().full() != tmsg.from() {
                    // Response from a specific worker
                    self.remote_addr = Some(BusAddress::new_from_string(tmsg.from())?);
                }

                trace!("{self} pulled messages from the data bus");

                for msg in tmsg.body() {
                    self.backlog.push(msg.to_owned());
                }
            }

            // Loop back around and see if we can pull the message
            // we want from our backlog.
        }
    }

    fn unpack_reply(&mut self, timer: &mut util::Timer,
        msg: message::Message) -> Result<Option<Response>, String> {

        if let Payload::Result(resp) = msg.payload() {

            // TODO serializer

            return Ok(Some(Response {
                // We can to_owned() here because we are done with the
                // container message.  We just need the content now.
                value: Some(resp.content().to_owned()),
                complete: false,
            }));

        }

        let err_msg;
        let trace = msg.thread_trace();

        if let Payload::Status(stat) = msg.payload() {
            match self.unpack_status_message(trace, timer, stat.status()) {
                Ok(v) => { return Ok(v); }
                Err(e) => err_msg = e,
            }
        } else {
            err_msg =
                format!("{self} unexpected response for request {trace}: {msg:?}");
        }

        self.reset();

        return Err(err_msg);
    }

    fn unpack_status_message(
        &mut self,
        trace: usize,
        timer: &mut util::Timer,
        stat: &message::MessageStatus,
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
                return Ok(Some(Response { value: None, complete: true }));
            }
            MessageStatus::Timeout => {
                err_msg = format!("{self} request {trace} timed out");
            }
            MessageStatus::NotFound => {
                err_msg = format!(
                    "{self} method bot found for request {trace}: {stat:?}");
            }
            _ => {
                err_msg = format!(
                    "{self} unexpected status message for request {trace} {stat:?}");
            }
        }

        Err(err_msg)
    }

    fn incr_thread_trace(&mut self) -> usize {
        self.last_thread_trace += 1;
        self.last_thread_trace
    }

    pub fn request<T>(&mut self, method: &str, params: Vec<T>) -> Result<usize, String>
    where
        T: Into<JsonValue>,
    {
        debug!("{self} sending request {method}");

        let trace = self.incr_thread_trace();

        let mut pvec = Vec::new();
        for p in params {
            // TODO serializer
            pvec.push(json::from(p));
        }
        let payload = Payload::Method(message::Method::new(method, pvec));


        /*
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

        */

        let req = message::Message::new(message::MessageType::Request, trace, payload);

        let mut client = self.client.borrow_mut();

        let tm = message::TransportMessage::new_with_body(
            self.remote_addr().full(),
            // All messages we send have a from address on our primary domain
            client.bus.address().full(),
            self.thread(),
            req,
        );

        let mut bus: &mut bus::Bus = &mut client.bus;

        if self.remote_addr().is_client() {
            // We are in a connected session.
            // Our remote end could be on a different domain.
            let domain = self.remote_addr().domain().unwrap().to_string();
            bus = client.get_connection(&domain)?;
        }

        bus.send(&tm)?;

        Ok(trace)
    }

    /// Establish a connected session with a remote service.
    pub fn connect(&mut self) -> Result<(), String> {

        if self.connected() {
            warn!("{self} is already connected");
            return Ok(())
        }

        debug!("{self} sending CONNECT");

        let trace = self.incr_thread_trace();

        let msg = message::Message::new(
            message::MessageType::Connect,
            trace,
            message::Payload::NoPayload,
        );

        let tm = message::TransportMessage::new_with_body(
            self.remote_addr().full(),
            self.client.borrow().bus.address().full(),
            self.thread(),
            msg
        );

        // A CONNECT always comes first, so we always drop it onto our
        // primary domain and let the router figure it out.
        self.client.borrow_mut().bus.send(&tm)?;

        self.recv(trace, CONNECT_TIMEOUT)?;

        if self.connected {
            Ok(())
        } else {
            self.reset();
            Err(format!("CONNECT timed out"))
        }
    }

    pub fn disconnect(&mut self) -> Result<(), String> {

        if !self.connected() {
            // Nothing to disconnect
            return Ok(())
        }

        debug!("{self} sending DISCONNECT");

        let trace = self.incr_thread_trace();

        let msg = message::Message::new(
            message::MessageType::Disconnect,
            trace,
            message::Payload::NoPayload,
        );

        let tm = message::TransportMessage::new_with_body(
            self.remote_addr().full(),
            self.client.borrow().bus.address().full(),
            self.thread(),
            msg,
        );

        if let Some(domain) = self.remote_addr().domain() {
            // There should always be a domain in this context

            let mut client = self.client.borrow_mut();

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

    pub fn request<T>(&mut self, method: &str, params: Vec<T>) -> Result<Request, String>
    where
        T: Into<JsonValue>,
    {
        Ok(Request {
            complete: false,
            session: self.session.clone(),
            method: method.to_string(),
            thread_trace: self.session.borrow_mut().request(method, params)?,
        })
    }

    pub fn connect(&self) -> Result<(), String> {
        self.session.borrow_mut().connect()
    }

    pub fn disconnect(&self) -> Result<(), String> {
        self.session.borrow_mut().disconnect()
    }
}


pub struct Client {
    bus: bus::Bus,

    primary_domain: String,

    /// Connections to remote domains.
    remote_bus_map: HashMap<String, bus::Bus>,

    config: conf::ClientConfig,

    /// Queue of receieved transport messages that have yet to be
    /// processed by any sessions.
    backlog: Vec<message::TransportMessage>,

    //pub serializer: Option<&'a dyn DataSerializer>,
}

impl Client {

    pub fn new(config: conf::ClientConfig) -> Result<ClientHandle, String> {

        let domain = config.bus_config().domain().as_deref()
            .expect("Domain required for client connection").to_string();

        let bus = bus::Bus::new(config.bus_config())?;

        let client = Client {
            config,
            bus: bus,
            primary_domain: domain.to_string(),
            backlog: Vec::new(),
            remote_bus_map: HashMap::new(),
            //serializer: None,
        };

        Ok(ClientHandle {
            client: Rc::new(RefCell::new(client))
        })
    }

    pub fn get_connection(&mut self, domain: &str) -> Result<&mut bus::Bus, String> {
        // Our primary bus address always has a domain.
        if domain.eq(&self.primary_domain) {
            Ok(&mut self.bus)
        } else {
            if self.remote_bus_map.contains_key(domain) {
                return Ok(self.remote_bus_map.get_mut(domain).unwrap());
            }

            self.add_connection(domain)
        }
    }

    fn add_connection(&mut self, domain: &str) -> Result<&mut bus::Bus, String> {
        let bus_conf = self.config.bus_config().clone();
        let bus = bus::Bus::new(&bus_conf)?;

        info!("Opened connection to new domain: {}", domain);

        self.remote_bus_map.insert(domain.to_string(), bus);
        self.get_connection(domain)
    }

    pub fn clear_bus(&mut self) -> Result<(), String> {
        self.bus.clear_stream()
    }

    pub fn disconnect_bus(&mut self) -> Result<(), String> {
        self.bus.disconnect()
    }

    /// Returns the first transport message pulled from the transport
    /// message backlog that matches the provided thread.
    fn recv_session_from_backlog(&mut self, thread: &str) -> Option<message::TransportMessage> {
        if let Some(index) = self.backlog.iter().position(|tm| tm.thread() == thread) {
            trace!("Found a backlog reply for thread {thread}");
            Some(self.backlog.remove(index))
        } else {
            None
        }
    }

    pub fn recv_session(&mut self, timer: &mut util::Timer,
        thread: &str) -> Result<Option<message::TransportMessage>, String> {

        loop {
            if let Some(tm) = self.recv_session_from_backlog(thread) {
                return Ok(Some(tm));
            }

            if timer.done() {
                // Nothing in the backlog and all out of time.
                return Ok(None);
            }

            // See what we can pull from the message bus

            if let Some(tm) = self.bus.recv(timer.remaining(), None)? {
                self.backlog.push(tm);
            }

            // Loop back around and see if we can pull a transport
            // message from the backlog matching the requested thread.
        }
    }
}

pub struct ClientHandle {
    client: Rc<RefCell<Client>>,
}

impl ClientHandle {

    /// Create a new client session for the requested service.
    pub fn session(&mut self, service: &str) -> SessionHandle {
        Session::new(self.client.clone(), service)
    }
}
