use super::addr::{ClientAddress, RouterAddress};
use super::session::ResponseIterator;
use super::session::SessionHandle;
use super::message;
use super::bus;
use super::conf;
use super::util;
use json::JsonValue;
use log::{info, trace};
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

const DEFAULT_ROUTER_COMMAND_TIMEOUT: i32 = 10;

pub trait DataSerializer {
    fn pack(&self, value: &JsonValue) -> JsonValue;
    fn unpack(&self, value: &JsonValue) -> JsonValue;
}

/// Generally speaking, we only need 1 ClientSingleton per thread (hence
/// the name).  This manages one bus connection per node and stores
/// messages pulled from the bus that have not yet been processed by
/// higher-up modules.
pub struct ClientSingleton {
    bus: bus::Bus,

    /// Our primary node
    node_name: String,

    /// Connections to remote nodes
    remote_bus_map: HashMap<String, bus::Bus>,

    config: Arc<conf::Config>,

    /// Queue of receieved transport messages that have yet to be
    /// processed by any sessions.
    backlog: Vec<message::TransportMessage>,

    /// If present, JsonValue's will be passed through its pack() and
    /// unpack() methods before/after data hits the network.
    serializer: Option<Arc<dyn DataSerializer>>,
}

impl ClientSingleton {
    fn new(config: Arc<conf::Config>) -> Result<ClientSingleton, String> {
        let con = match config.primary_connection() {
            Some(c) => c,
            None => {
                return Err(format!("ClientSingleton Config requires a primary connection"));
            }
        };

        let bus = bus::Bus::new(&con)?;
        let node_name = con.node_name().to_string();

        Ok(ClientSingleton {
            config,
            node_name,
            bus: bus,
            backlog: Vec::new(),
            remote_bus_map: HashMap::new(),
            serializer: None,
        })
    }

    pub fn serializer(&self) -> &Option<Arc<dyn DataSerializer>> {
        &self.serializer
    }

    fn clear_backlog(&mut self) {
        self.backlog.clear();
    }

    /// Full bus address as a string
    fn address(&self) -> &str {
        self.bus.address().full()
    }

    /// Our primary bus node_name
    fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn bus(&self) -> &bus::Bus {
        &self.bus
    }

    pub fn bus_mut(&mut self) -> &mut bus::Bus {
        &mut self.bus
    }

    pub fn get_node_bus(&mut self, node_name: &str) -> Result<&mut bus::Bus, String> {
        log::trace!("Loading bus connection for node_name: {node_name}");

        if node_name.eq(self.node_name()) {
            Ok(&mut self.bus)
        } else {
            if self.remote_bus_map.contains_key(node_name) {
                return Ok(self.remote_bus_map.get_mut(node_name).unwrap());
            }

            self.add_connection(node_name)
        }
    }

    /// Add a connection to a new remote node.
    ///
    /// Panics if our configuration has no primary node.
    fn add_connection(&mut self, node_name: &str) -> Result<&mut bus::Bus, String> {
        // When adding a connection to a remote node, assume the same
        // connection type, etc. is used and just change the node.
        let mut con = self.config.primary_connection().unwrap().clone();

        if self.config.get_node(node_name).is_none() {
            return Err(format!("No configuration for node: {node_name}"));
        };

        con.set_node_name(node_name);

        let bus = bus::Bus::new(&con)?;

        info!("Opened connection to new node: {}", node_name);

        self.remote_bus_map.insert(node_name.to_string(), bus);
        self.get_node_bus(node_name)
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

    fn send_router_command(
        &mut self,
        node_name: &str,
        router_command: &str,
        router_class: Option<&str>,
        await_reply: bool,
    ) -> Result<Option<JsonValue>, String> {
        let addr = RouterAddress::new(node_name);

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

        let bus = self.get_node_bus(node_name)?;
        bus.send(&tmsg)?;

        if !await_reply {
            return Ok(None);
        }

        // Always listen on our primary bus.
        // TODO rethink this.  If we have replies from other requests
        // sitting in the bus, they may be received here instead
        // of the expected router response.  self.bus.clear() before
        // send is one option, but pretty heavy-handed.
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

impl fmt::Display for ClientSingleton {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientSingleton({})", self.address())
    }
}

/// Wrapper around our ClientSingleton Ref so we can easily share a client
/// within a given thread.
///
/// Wrapping the Ref in a struct allows us to present a client-like
/// API to the caller.  I.e. the caller is not required to .borrow() /
/// .borrow_mut() directly when performing actions against the client Ref.
///
/// When a new client Ref is needed, clone the Client.
pub struct Client {
    singleton: Rc<RefCell<ClientSingleton>>,
    address: ClientAddress,
    node_name: String,
}

impl Client {
    pub fn connect(config: Arc<conf::Config>) -> Result<Client, String> {

        // This performs the actual bus-level connection.
        let singleton = ClientSingleton::new(config)?;

        let address = singleton.bus().address().clone();
        let node_name = singleton.node_name().to_string();

        Ok(Client {
            address,
            node_name,
            singleton: Rc::new(RefCell::new(singleton)),
        })
    }

    pub fn singleton(&self) -> &Rc<RefCell<ClientSingleton>> {
        &self.singleton
    }

    pub fn clone(&self) -> Self {
        Client {
            address: self.address().clone(),
            node_name: self.node_name().to_string(),
            singleton: self.singleton().clone()
        }
    }

    pub fn set_serializer(&self, serializer: Arc<dyn DataSerializer>) {
        self.singleton.borrow_mut().serializer = Some(serializer);
    }

    pub fn address(&self) -> &ClientAddress {
        &self.address
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Create a new client session for the requested service.
    pub fn session(&mut self, service: &str) -> SessionHandle {
        SessionHandle::new(self.clone(), service)
    }

    /// Discard any unprocessed messages from our backlog and clear our
    /// stream of pending messages on the bus.
    pub fn clear(&mut self) -> Result<(), String> {
        self.singleton().borrow_mut().clear_backlog();
        self.singleton().borrow_mut().bus_mut().clear_stream()
    }

    pub fn setup_stream(&mut self, name: Option<&str>) -> Result<(), String> {
        self.singleton().borrow_mut().bus_mut().setup_stream(name)
    }

    pub fn send_router_command(
        &self,
        node_name: &str,
        command: &str,
        router_class: Option<&str>,
        await_reply: bool,
    ) -> Result<Option<JsonValue>, String> {
        self.singleton()
            .borrow_mut()
            .send_router_command(node_name, command, router_class, await_reply)
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
