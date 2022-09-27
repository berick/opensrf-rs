use json;
use super::*;
use log::{trace, info};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::cell::RefCell;
use std::rc::Rc;

struct Request {
    session: Rc<RefCell<Session>>,
    complete: bool,
    thread_trace: usize,
    method: String,
    /// Replies pulled from the bus that have not yet been
    /// returned to the caller.
    replies: VecDeque<json::JsonValue>,
}

impl Request {

    pub fn new(session: Rc<RefCell<Session>>, method: &str, thread_trace: usize) -> Request {
        Request {
            session,
            method: method.to_string(),
            complete: false,
            thread_trace,
            replies: VecDeque::new(),
        }
    }

    pub fn has_pending_replies(&self) -> bool {
        !self.replies.is_empty()
    }

    /// Adds a response to the session backlog so it can
    /// be recv()'d later.
    pub fn add_pending_reply(&mut self, value: json::JsonValue) {
        self.replies.push_back(value);
    }

    pub fn recv(&mut self, timeout: i32) -> Result<Option<json::JsonValue>, String> {
        let mut resp: Result<Option<json::JsonValue>, String> = Ok(None);

        if let Some(value) = self.replies.pop_front() {
            return Ok(Some(value));
        }

        if self.complete {
            return resp;
        }

        resp
    }
}

enum SessionType {
    Client,
    Server,
}

struct Session {
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
    service_addr: addr::BusAddress,

    /// Worker-specific bus address for our session.
    /// Only set once we are communicating with a specific worker.
    remote_addr: Option<addr::BusAddress>,

    /// Each new Request within a Session gets a new thread_trace.
    /// Replies have the same thread_trace as their request.
    ///
    /// This is effectively a request ID.
    last_thread_trace: usize,

    requests: HashMap<usize, Rc<RefCell<Request>>>,
}

impl Session {

    pub fn new(client: Rc<RefCell<Client>>, service: &str) -> SessionHandle {

        let ses = Session {
            client,
            session_type: SessionType::Client,
            service: String::from(service),
            remote_addr: None,
            service_addr: addr::BusAddress::new_for_service(&service),
            connected: false,
            last_thread_trace: 0,
            thread: util::random_number(16),
            requests: HashMap::new(),
        };

        trace!("Creating session service={} thread={}", service, ses.thread);

        SessionHandle {
            session: Rc::new(RefCell::new(ses))
        }
    }

    pub fn reset(&mut self) {
        self.remote_addr = None;
        self.connected = false;
    }

    /// Returns the address of the remote end if we are connected.  Otherwise,
    /// returns the default remote address of the service we are talking to.
    pub fn remote_addr(&self) -> &addr::BusAddress {
        if self.connected {
            if let Some(ref ra) = self.remote_addr {
                return ra;
            }
        }

        &self.service_addr
    }
}



struct SessionHandle {
    session: Rc<RefCell<Session>>,
}

struct Client {
    bus: bus::Bus,

    primary_domain: String,

    /// Connections to remote domains.
    remote_bus_map: HashMap<String, bus::Bus>,

    config: conf::ClientConfig,

    /// Queue of receieved transport messages that have yet to be
    /// processed by any sessions.
    transport_backlog: Vec<message::TransportMessage>,

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
            transport_backlog: Vec::new(),
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
}

struct ClientHandle {
    client: Rc<RefCell<Client>>,
}

impl ClientHandle {

    /// Create a new client session for the requested service.
    pub fn session(&mut self, service: &str) -> SessionHandle {
        Session::new(self.client.clone(), service)
    }
}
