use super::addr::BusAddress;
use super::session::ResponseIterator;
use super::session::SessionHandle;
use super::*;
use json::JsonValue;
use log::{info, trace};
use std::cell::RefCell;
use std::cell::{Ref, RefMut};
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

const DEFAULT_ROUTER_COMMAND_TIMEOUT: i32 = 10;

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

    config: Arc<conf::Config>,

    /// Queue of receieved transport messages that have yet to be
    /// processed by any sessions.
    backlog: Vec<message::TransportMessage>,

    /// If present, JsonValue's will be passed through its pack() and
    /// unpack() methods before/after data hits the network.
    serializer: Option<Arc<dyn DataSerializer>>,
}

impl Client {
    pub fn new(config: Arc<conf::Config>) -> Result<ClientHandle, String> {
        let con = match config.primary_connection() {
            Some(c) => c,
            None => {
                return Err(format!("Client Config requires a primary connection"));
            }
        };

        let bus = bus::Bus::new(&con)?;
        let domain = con.domain().name().to_string();

        let client = Client {
            config,
            domain,
            bus: bus,
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
        log::trace!("Loading bus connection for domain: {domain}");

        if domain.eq(self.domain()) {
            Ok(&mut self.bus)
        } else {
            if self.remote_bus_map.contains_key(domain) {
                return Ok(self.remote_bus_map.get_mut(domain).unwrap());
            }

            self.add_connection(domain)
        }
    }

    /// Add a connection to a new remote domain.
    ///
    /// Panics if our configuration has no primary domain.
    fn add_connection(&mut self, domain: &str) -> Result<&mut bus::Bus, String> {
        // When adding a connection to a remote domain, assume the same
        // connection type, etc. is used and just change the domain.
        let mut con = self.config.primary_connection().unwrap().clone();

        let bus_domain = match self.config.get_domain(domain) {
            Some(d) => d,
            None => {
                return Err(format!("No configuration for domain: {domain}"));
            }
        };

        con.set_domain(bus_domain);

        let bus = bus::Bus::new(&con)?;

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
        router_class: Option<&str>,
        await_reply: bool,
    ) -> Result<Option<JsonValue>, String> {
        let addr = BusAddress::new_for_router(domain);

        // Always use the address of our primary Bus
        let mut tmsg = message::TransportMessage::new(
            addr.full(),
            self.bus.address().full(),
            &util::random_number(16),
        );

        tmsg.set_router_command(router_command);
        if let Some(rc) = router_class {
            tmsg.set_router_class(rc);
        }

        let bus = self.get_domain_bus(domain)?;
        bus.send(&tmsg)?;

        if !await_reply {
            return Ok(None);
        }

        // Always listen on our primary bus.
        match self.bus.recv(DEFAULT_ROUTER_COMMAND_TIMEOUT, None)? {
            Some(tm) => match tm.router_reply() {
                Some(reply) => match json::parse(reply) {
                    Ok(jv) => Ok(Some(jv)),
                    Err(e) => Err(format!(
                        "Router command {} return unparseable content: {} {}",
                        router_command, reply, e
                    )),
                },
                _ => Err(format!(
                    "Router command {} returned without reply_content",
                    router_command
                )),
            },
            _ => Err(format!(
                "Router command {} returned no results in {} seconds",
                router_command, DEFAULT_ROUTER_COMMAND_TIMEOUT
            )),
        }
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Client({})", self.address())
    }
}

/// Wrapper around a Client Ref so we can easily share a client
/// within a given thread.
///
/// Wrapping the Ref in a struct allows us to present a client-like
/// API to the caller.  I.e. the caller is not required to .borrow() /
/// .borrow_mut() directly when performing actions against the client Ref.
///
/// When a new client Ref is needed, clone the ClientHandle.
pub struct ClientHandle {
    client: Rc<RefCell<Client>>,
}

impl ClientHandle {
    pub fn clone(&self) -> Self {
        ClientHandle {
            client: self.client.clone(),
        }
    }

    pub fn new(client: Rc<RefCell<Client>>) -> Self {
        ClientHandle { client }
    }

    /// Create a new client session for the requested service.
    pub fn session(&mut self, service: &str) -> SessionHandle {
        SessionHandle::new(
            self.client.clone(),
            service,
            self.client.borrow().config.multi_domain_support(),
        )
    }

    pub fn client_mut(&self) -> RefMut<Client> {
        self.client.borrow_mut()
    }

    pub fn client(&self) -> Ref<Client> {
        self.client.borrow()
    }

    pub fn set_serializer(&self, serializer: Arc<dyn DataSerializer>) {
        self.client.borrow_mut().serializer = Some(serializer);
    }

    pub fn clone_client(&self) -> Rc<RefCell<Client>> {
        self.client.clone()
    }

    pub fn send_router_command(
        &self,
        domain: &str,
        command: &str,
        router_class: Option<&str>,
        await_reply: bool,
    ) -> Result<Option<JsonValue>, String> {
        self.client
            .borrow_mut()
            .send_router_command(domain, command, router_class, await_reply)
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
