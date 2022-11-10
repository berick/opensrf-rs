use chrono::prelude::{DateTime, Local};
use log::{debug, error, info, trace, warn};
use opensrf::addr::{BusAddress, ClientAddress, RouterAddress, ServiceAddress};
use opensrf::bus::Bus;
use opensrf::conf;
use opensrf::message::{Message, MessageStatus, MessageType, Payload, Status, TransportMessage};
use std::thread;

/// A service controller.
///
/// This is what we traditionally call a "Listener" in OpenSRF.
/// A service can have multiple controllers on a single node_name.
#[derive(Debug, Clone)]
struct ServiceInstance {
    address: ClientAddress,
    register_time: DateTime<Local>,
}

impl ServiceInstance {
    fn address(&self) -> &ClientAddress {
        &self.address
    }

    fn register_time(&self) -> &DateTime<Local> {
        &self.register_time
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            address: json::from(self.address().full()),
            register_time: json::from(self.register_time().to_rfc3339()),
        }
    }
}

/// An API-producing service.
///
/// E.g. "opensrf.settings"
#[derive(Debug, Clone)]
struct ServiceEntry {
    name: String,
    controllers: Vec<ServiceInstance>,
}

impl ServiceEntry {
    fn name(&self) -> &str {
        &self.name
    }

    fn controllers(&self) -> &Vec<ServiceInstance> {
        &self.controllers
    }

    fn remove_controller(&mut self, address: &ClientAddress) {
        if let Some(pos) = self
            .controllers
            .iter()
            .position(|c| c.address().full().eq(address.full()))
        {
            debug!(
                "Removing controller for service={} address={}",
                self.name, address
            );
            self.controllers.remove(pos);
        } else {
            debug!(
                "Cannot remove unknown controller service={} address={}",
                self.name, address
            );
        }
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            name: json::from(self.name()),
            controllers: json::from(
                self.controllers().iter()
                    .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }
}

/// One node_name entry.
///
/// A node_name will typically host multiple services.
/// E.g. "public.localhost"
struct RouterNode {
    // e.g. public.localhost
    node_name: String,

    /// Bus connection to the redis instance for this node_name.
    ///
    /// A connection is only opened when needed.  Once opened, it's left
    /// open until the connection is shut down on the remote end.
    bus: Option<Bus>,

    /// How many requests have been routed to this node_name.
    ///
    /// We count node_name-level routing instead of service controller-level
    /// routing, since we can't guarantee which service controller will
    /// pick up any given request routed to a node_name.
    route_count: usize,

    services: Vec<ServiceEntry>,

    config: conf::BusConnection,
}

impl RouterNode {
    fn new(config: &conf::BusConnection) -> Self {
        RouterNode {
            node_name: config.node_name().to_string(),
            bus: None,
            route_count: 0,
            services: Vec::new(),
            config: config.clone(),
        }
    }

    fn node_name(&self) -> &str {
        &self.node_name
    }

    fn bus(&self) -> Option<&Bus> {
        self.bus.as_ref()
    }

    fn bus_mut(&mut self) -> Option<&mut Bus> {
        self.bus.as_mut()
    }

    fn route_count(&self) -> usize {
        self.route_count
    }

    fn services(&self) -> &Vec<ServiceEntry> {
        &self.services
    }

    fn has_service(&self, name: &str) -> bool {
        return self.services.iter().filter(|s| s.name().eq(name)).count() > 0;
    }

    fn remove_service(&mut self, service: &str, address: &ClientAddress) {
        if let Some(s_pos) = self.services.iter().position(|s| s.name().eq(service)) {
            let svc = self.services.get_mut(s_pos).unwrap(); // known OK
            svc.remove_controller(address);

            if svc.controllers.len() == 0 {
                debug!(
                    "Removing registration for service={} on removal of last controller address={}",
                    service, address
                );

                if let Some(s_pos) = self.services.iter().position(|s| s.name().eq(service)) {
                    self.services.remove(s_pos);
                }
            }
        }
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            node_name: json::from(self.node_name()),
            route_count: json::from(self.route_count()),
            services: json::from(self.services().iter()
                .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }

    /// Connect to the Redis instance on this node_name.
    fn connect(&mut self) -> Result<(), String> {
        if self.bus.is_some() {
            return Ok(());
        }

        let bus = match Bus::new(&self.config) {
            Ok(b) => b,
            Err(e) => return Err(format!("Cannot connect bus: {}", e)),
        };

        self.bus = Some(bus);

        Ok(())
    }

    /// Send a message to this node_name via our node_name connection.
    fn send_to_node(&mut self, tm: TransportMessage) -> Result<(), String> {
        trace!(
            "send_to_node({}) routing message to {}",
            self.node_name(),
            tm.to()
        );

        let bus = match &mut self.bus {
            Some(b) => b,
            None => {
                return Err(format!(
                    "We have no connection to node_name {}",
                    self.node_name()
                ));
            }
        };

        bus.send(&tm)
    }
}

struct Router {
    /// Primary node_name for this router instance.
    primary_node: RouterNode,

    /// Well-known address where top-level API calls should be routed.
    listen_address: RouterAddress,

    remote_nodes: Vec<RouterNode>,

    config: conf::Config,
}

impl Router {
    pub fn new(config: conf::Config) -> Self {
        let busconf = config.primary_connection().unwrap();
        let node_name = busconf.node_name().to_string();
        let addr = RouterAddress::new(&node_name);
        let primary_node = RouterNode::new(&busconf);

        Router {
            config,
            primary_node,
            listen_address: addr,
            remote_nodes: Vec::new(),
        }
    }

    fn init(&mut self) -> Result<(), String> {
        self.primary_node.connect()?;
        self.setup_stream()?;
        Ok(())
    }

    fn primary_node(&self) -> &RouterNode {
        &self.primary_node
    }

    fn remote_nodes(&self) -> &Vec<RouterNode> {
        &self.remote_nodes
    }

    /// Setup the Redis stream/group we listen to
    pub fn setup_stream(&mut self) -> Result<(), String> {
        let sname = self.listen_address.full();

        info!("Setting up primary stream={sname}");

        let bus = &mut self
            .primary_node
            .bus_mut()
            .expect("Primary node_name must have a bus");

        bus.delete_named_stream(sname)?;
        bus.setup_stream(Some(sname))
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            listen_address: json::from(self.listen_address.full()),
            primary_node: self.primary_node().to_json_value(),
            remote_nodes: json::from(self.remote_nodes().iter()
                .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }

    /// Find or create a new RouterNode entry.
    fn find_or_create_node_name(&mut self, node_name: &str) -> Result<&mut RouterNode, String> {
        if self.primary_node.node_name.eq(node_name) {
            return Ok(&mut self.primary_node);
        }

        let mut pos_op = self
            .remote_nodes
            .iter()
            .position(|d| d.node_name.eq(node_name));

        if pos_op.is_none() {
            debug!("Adding new RouterNode for node_name={}", node_name);

            // Primary connection is required at this point.
            let mut busconf = self.config.primary_connection().unwrap().clone();

            if self.config.get_node(node_name).is_some() {
                busconf.set_node_name(node_name);
            } else {
                return Err(format!("Cannot route to unknown node_name: {node_name}"));
            }

            self.remote_nodes.push(RouterNode::new(&busconf));

            pos_op = Some(self.remote_nodes.len() - 1);
        }

        // Here the position is known to have data.
        Ok(self.remote_nodes.get_mut(pos_op.unwrap()).unwrap())
    }

    fn handle_unregister(&mut self, address: &ClientAddress, service: &str) -> Result<(), String> {
        let node_name = address.domain();

        info!(
            "De-registering node_name={} service={} address={}",
            node_name, service, address
        );

        if self.primary_node.node_name.eq(node_name) {
            // When removing a service from the primary node_name, leave the
            // node_name as a whole intact since we'll likely need it again.
            // Remove services and controllers as necessary, though.

            self.primary_node.remove_service(service, &address);
            return Ok(());
        }

        // When removing the last service from a remote node_name, remove
        // the node_name entry as a whole.
        let mut rem_pos_op: Option<usize> = None;
        let mut idx = 0;

        for r_node in &mut self.remote_nodes {
            if r_node.node_name().eq(node_name) {
                r_node.remove_service(service, address);
                if r_node.services.len() == 0 {
                    // Cannot remove here since it would be modifying
                    // self.remote_nodes while it's aready mutably borrowed.
                    rem_pos_op = Some(idx);
                }
                break;
            }
            idx += 1;
        }

        if let Some(pos) = rem_pos_op {
            debug!(
                "Removing cleared node_name entry for node_name={}",
                node_name
            );
            self.remote_nodes.remove(pos);
        }

        Ok(())
    }

    fn handle_register(&mut self, address: ClientAddress, service: &str) -> Result<(), String> {
        let node_name = address.domain(); // Known to be a client addr.

        info!("Registering new node_name={}", node_name);

        let r_node = self.find_or_create_node_name(node_name)?;

        for svc in &mut r_node.services {
            // See if we have a ServiceEntry for this service on this node_name.

            if svc.name.eq(service) {
                for controller in &mut svc.controllers {
                    if controller.address.full().eq(address.full()) {
                        warn!(
                            "Controller with address {} already registered for service {} and node_name {}",
                            address, service, node_name
                        );
                        return Ok(());
                    }
                }

                debug!(
                    "Adding new ServiceInstance node_name={} service={} address={}",
                    node_name, service, address
                );

                svc.controllers.push(ServiceInstance {
                    address: address.clone(),
                    register_time: Local::now(),
                });

                return Ok(());
            }
        }

        // We have no Service Entry for this node_name+service+address.
        // Add a ServiceEntry and a new ServiceInstance

        debug!(
            "Adding new ServiceEntry node_name={} service={} address={}",
            node_name, service, address
        );

        r_node.services.push(ServiceEntry {
            name: service.to_string(),
            controllers: vec![ServiceInstance {
                address: address,
                register_time: Local::now(),
            }],
        });

        Ok(())
    }

    /// List of currently active services by service name.
    fn _active_services(&self) -> Vec<&str> {
        let mut services: Vec<&str> = self
            .primary_node()
            .services()
            .iter()
            .map(|s| s.name())
            .collect();

        for d in self.remote_nodes() {
            for s in d.services() {
                if !services.contains(&s.name()) {
                    services.push(s.name());
                }
            }
        }

        return services;
    }

    fn listen(&mut self) {
        // Listen for inbound requests / router commands on our primary
        // node_name and route accordingly.

        loop {
            let tm = match self.recv_one() {
                Ok(m) => m,
                Err(s) => {
                    error!(
                        "Exiting on error receiving data from primary connection: {}",
                        s
                    );
                    return;
                }
            };

            if let Err(s) = self.route_message(tm) {
                error!("Error routing message: {}", s);
            }
        }
    }

    fn route_message(&mut self, tm: TransportMessage) -> Result<(), String> {
        let to = tm.to();

        debug!("Received message destined for {}", to);

        let addr = BusAddress::new_from_string(to)?;

        if addr.is_service() {
            let addr = ServiceAddress::from_addr(addr)?;
            return self.route_api_request(&addr, tm);
        } else if addr.is_router() {
            return self.handle_router_command(tm);
        } else {
            return Err(format!("Unexpected message recipient: {}", to));
        }
    }

    fn route_api_request(
        &mut self,
        to_addr: &ServiceAddress,
        tm: TransportMessage,
    ) -> Result<(), String> {
        let service = to_addr.service();

        if self.primary_node.has_service(service) {
            self.primary_node.route_count += 1;
            return self.primary_node.send_to_node(tm);
        }

        for r_node in &mut self.remote_nodes {
            if r_node.has_service(service) {
                if r_node.bus.is_none() {
                    // We only connect to remote node_names when it's
                    // time to send them a message.
                    r_node.connect()?;
                }

                r_node.route_count += 1;
                return r_node.send_to_node(tm);
            }
        }

        error!(
            "Router at {} has no service controllers for service {service}",
            self.primary_node.node_name()
        );

        let payload = Payload::Status(Status::new(
            MessageStatus::ServiceNotFound,
            &format!("Service {service} not found"),
            "osrfServiceException",
        ));

        let mut trace = 0;
        if tm.body().len() > 0 {
            // It would be odd, but not impossible to receive a
            // transport message destined for a service that has no
            // messages in its body.
            trace = tm.body()[0].thread_trace();
        }

        let from = match self.primary_node.bus() {
            Some(b) => b.address().full(),
            None => self.listen_address.full(),
        };

        let tm = TransportMessage::with_body(
            tm.from(), // Recipient.  Bounce it back.
            from,
            tm.thread(),
            Message::new(MessageType::Status, trace, payload),
        );

        // Bounce-backs will always be directed back to a client
        // on our primary node_name, since clients only ever talk to
        // the router on their own node_name.
        self.primary_node.send_to_node(tm)
    }

    fn handle_router_command(&mut self, tm: TransportMessage) -> Result<(), String> {
        let router_command = match tm.router_command() {
            Some(s) => s,
            None => {
                return Err(format!(
                    "No router command present: {}",
                    tm.to_json_value().dump()
                ));
            }
        };

        let from = tm.from();

        let from_addr = ClientAddress::from_string(from)?;

        debug!(
            "Router command received command={} from={}",
            router_command, from
        );

        // Not all router commands require a router class.
        let router_class = || {
            if let Some(rc) = tm.router_class() {
                return Ok(rc);
            } else {
                return Err(format!(
                    "Message has no router class: {}",
                    tm.to_json_value().dump()
                ));
            }
        };

        match router_command {
            "register" => self.handle_register(from_addr, router_class()?),
            "unregister" => self.handle_unregister(&from_addr, router_class()?),
            _ => self.deliver_information(from_addr, tm),
        }
    }

    /// Deliver stats, etc. to clients that request it.
    fn deliver_information(
        &mut self,
        from_addr: ClientAddress,
        mut tm: TransportMessage,
    ) -> Result<(), String> {
        let router_command = tm.router_command().unwrap(); // known exists
        debug!("Handling info router command : {router_command}");

        match router_command {
            "summarize" => tm.set_router_reply(&self.to_json_value().dump()),
            _ => {
                return Err(format!("Unsupported router command: {router_command}"));
            }
        }

        // Bounce the message back to the caller with the requested data.
        // Should our FROM address be our unique bus address or the router
        // address? Does it matter?
        tm.set_from(self.primary_node.bus().unwrap().address().full());
        tm.set_to(from_addr.full());

        let r_node = self.find_or_create_node_name(from_addr.domain())?;

        if r_node.bus.is_none() {
            r_node.connect()?;
        }

        r_node.send_to_node(tm)
    }

    fn recv_one(&mut self) -> Result<TransportMessage, String> {
        let bus = self
            .primary_node
            .bus_mut()
            .expect("We always maintain a connection on the primary node_name");

        loop {
            // Looping should not be required here, but can't hurt.

            if let Some(tm) = bus.recv(-1, Some(self.listen_address.full()))? {
                return Ok(tm);
            } else {
                debug!("recv() on main bus address returned None.  Will keep trying");
            }
        }
    }
}

// TODO notify connected service coordinators when we shut down?

fn main() {
    let config = opensrf::init("private_router").unwrap();

    // Each name gets 1 router for each of its 2 public/private sudbomains.
    // Each runs in its own thread.
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();

    // Private Router Thread ---
    let conf = config.clone();

    threads.push(thread::spawn(|| {
        let mut router = Router::new(conf);
        router.init().unwrap();
        router.listen();
    }));

    // Public Router Thread ---
    let mut conf = config.clone();
    let domain = config.primary_connection().unwrap().domain_name();
    conf.set_primary_connection("public_router", &domain)
        .unwrap();

    threads.push(thread::spawn(|| {
        let mut router = Router::new(conf);
        router.init().unwrap();
        router.listen();
    }));

    // Block here while the routers are running.
    for thread in threads {
        thread.join().ok();
    }
}
