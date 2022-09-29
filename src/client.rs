use std::fmt;
use json::JsonValue;
use super::*;
use super::session::Session;
use super::session::SessionHandle;
use super::addr::BusAddress;
use log::{trace, info};
use std::collections::HashMap;
use std::cell::{Ref, RefCell};
use std::rc::Rc;

pub trait DataSerializer {
    fn pack(&self, value: &JsonValue) -> JsonValue;
    fn unpack(&self, value: &JsonValue) -> JsonValue;
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

    pub serializer: Option<Rc<RefCell<dyn DataSerializer>>>,
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
            serializer: None,
        };

        Ok(ClientHandle {
            client: Rc::new(RefCell::new(client))
        })
    }

    pub fn serializer(&self) -> Option<Ref<dyn DataSerializer>> {
        if self.serializer.is_some() {
            Some(self.serializer.as_deref().unwrap().borrow())
        } else {
            None
        }
    }

    /// Full bus address as a string
    pub fn address(&self) -> &str {
        self.bus.address().full()
    }

    pub fn domain(&self) -> &str {
        &self.primary_domain
    }

    pub fn primary_connection(&self) -> &bus::Bus {
        &self.bus
    }

    pub fn primary_connection_mut(&mut self) -> &mut bus::Bus {
        &mut self.bus
    }

    pub fn get_connection(&mut self, domain: &str) -> Result<&mut bus::Bus, String> {
        // Our primary bus address always has a domain.
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
        self.get_connection(domain)
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

        let bus = self.get_connection(domain)?;
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
        Session::new(self.client.clone(), service)
    }

    pub fn set_serializer(&self, serializer: Rc<RefCell<dyn DataSerializer>>) {
        self.client.borrow_mut().serializer = Some(serializer);
    }
}


