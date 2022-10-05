use super::addr::BusAddress;
use super::session::ResponseIterator;
use super::session::Session;
use super::session::SessionHandle;
use super::*;
use json::JsonValue;
use log::{info, trace};
use std::cell::RefCell;
use std::cell::RefMut;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

pub trait DataSerializer {
    fn pack(&self, value: &JsonValue) -> JsonValue;
    fn unpack(&self, value: &JsonValue) -> JsonValue;
}

pub struct Client {
    bus: bus::Bus,

    /// Our primary domain
    domain: String,

    /// Connections to remote domains.
    remote_bus_map: HashMap<String, bus::Bus>,

    config: conf::ClientConfig,

    /// Queue of receieved transport messages that have yet to be
    /// processed by any sessions.
    backlog: Vec<message::TransportMessage>,

    /// If present, JsonValue's will be passed through its pack() and
    /// unpack() methods before/after data hits the network.
    serializer: Option<Arc<dyn DataSerializer>>,
}

impl Client {
    pub fn new(config: conf::ClientConfig) -> Result<ClientHandle, String> {
        let domain = config
            .bus_config()
            .domain()
            .as_deref()
            .expect("Domain required for client connection")
            .to_string();

        let bus = bus::Bus::new(config.bus_config())?;

        let client = Client {
            config,
            bus: bus,
            domain: domain.to_string(),
            backlog: Vec::new(),
            remote_bus_map: HashMap::new(),
            serializer: None,
        };

        Ok(ClientHandle {
            client: Rc::new(RefCell::new(client)),
        })
    }

    pub fn serializer(&self) -> &Option<Arc<dyn DataSerializer>> {
        &self.serializer
    }

    /// Full bus address as a string
    pub fn address(&self) -> &str {
        self.bus.address().full()
    }

    /// Our primary bus domain
    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn bus(&self) -> &bus::Bus {
        &self.bus
    }

    pub fn bus_mut(&mut self) -> &mut bus::Bus {
        &mut self.bus
    }

    pub fn get_domain_bus(&mut self, domain: &str) -> Result<&mut bus::Bus, String> {
        if domain.eq(self.domain()) {
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
        self.get_domain_bus(domain)
    }

    /// Discard any unprocessed messages from our backlog and clear our
    /// stream of pending messages on the bus.
    pub fn clear(&mut self) -> Result<(), String> {
        self.backlog.clear();
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

    pub fn recv_session(
        &mut self,
        timer: &mut util::Timer,
        thread: &str,
    ) -> Result<Option<message::TransportMessage>, String> {
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

    pub fn send_router_command(
        &mut self,
        domain: &str,
        router_command: &str,
        router_class: &str,
    ) -> Result<(), String> {
        let addr = BusAddress::new_for_router(domain);

        // Always use the address of our primary Bus
        let mut tmsg = message::TransportMessage::new(
            addr.full(),
            self.bus.address().full(),
            &util::random_number(16),
        );

        tmsg.set_router_command(router_command);
        tmsg.set_router_class(router_class);

        let bus = self.get_domain_bus(domain)?;
        bus.send(&tmsg)
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Client({})", self.address())
    }
}

pub struct ClientHandle {
    client: Rc<RefCell<Client>>,
}

impl ClientHandle {
    /// Create a new client session for the requested service.
    pub fn session(&mut self, service: &str) -> SessionHandle {
        Session::new(
            self.client.clone(),
            service,
            self.client.borrow().config.multi_domain_support(),
        )
    }

    pub fn client_mut(&self) -> RefMut<Client> {
        self.client.borrow_mut()
    }

    pub fn set_serializer(&self, serializer: Arc<dyn DataSerializer>) {
        self.client.borrow_mut().serializer = Some(serializer);
    }

    pub fn send_router_command(
        &self,
        domain: &str,
        command: &str,
        router_class: &str,
    ) -> Result<(), String> {
        self.client
            .borrow_mut()
            .send_router_command(domain, command, router_class)
    }

    /// Send a request and receive a ResponseIterator for iterating
    /// the responses to the method.
    ///
    /// Uses the default request timeout DEFAULT_REQUEST_TIMEOUT.
    pub fn sendrecv<T>(
        &mut self,
        service: &str,
        method: &str,
        params: Vec<T>,
    ) -> Result<ResponseIterator, String>
    where
        T: Into<JsonValue>,
    {
        Ok(ResponseIterator::new(
            self.session(service).request(method, params)?,
        ))
    }
}
