use chrono::prelude::{DateTime, Local};
use log::{debug, error, info, trace, warn};
use opensrf::addr::{BusAddress, RouterAddress};
use opensrf::bus::Bus;
use opensrf::conf;
use opensrf::message::{Message, MessageStatus, MessageType, Payload, Status, TransportMessage};
use opensrf::Logger;
use std::thread;

/// A service controller.
///
/// This is what we traditionally call a "Listener" in OpenSRF.
/// A service can have multiple controllers on a single domain.
#[derive(Debug, Clone)]
struct ServiceInstance {
    address: BusAddress,
    register_time: DateTime<Local>,
}

impl ServiceInstance {
    fn address(&self) -> &BusAddress {
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

    fn remove_controller(&mut self, address: &BusAddress) {
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

/// One domain entry.
///
/// A domain will typically host multiple services.
/// E.g. "public.localhost"
struct RouterDomain {
    // e.g. public.localhost
    domain: String,

    /// Bus connection to the redis instance for this domain.
    ///
    /// A connection is only opened when needed.  Once opened, it's left
    /// open until the connection is shut down on the remote end.
    bus: Option<Bus>,

    /// How many requests have been routed to this domain.
    ///
    /// We count domain-level routing instead of service controller-level
    /// routing, since we can't guarantee which service controller will
    /// pick up any given request routed to a domain.
    route_count: usize,

    services: Vec<ServiceEntry>,

    config: conf::BusConnection,
}

impl RouterDomain {
    fn new(config: &conf::BusConnection) -> Self {
        RouterDomain {
            domain: config.domain().name().to_string(),
            bus: None,
            route_count: 0,
            services: Vec::new(),
            config: config.clone(),
        }
    }

    fn domain(&self) -> &str {
        &self.domain
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

    fn remove_service(&mut self, service: &str, address: &BusAddress) {
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
            domain: json::from(self.domain()),
            route_count: json::from(self.route_count()),
            services: json::from(self.services().iter()
                .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }

    /// Connect to the Redis instance on this domain.
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

    /// Send a message to this domain via our domain connection.
    fn send_to_domain(&mut self, tm: TransportMessage) -> Result<(), String> {
        trace!(
            "send_to_domain({}) routing message to {}",
            self.domain(),
            tm.to()
        );

        let bus = match &mut self.bus {
            Some(b) => b,
            None => {
                return Err(format!("We have no connection to domain {}", self.domain()));
            }
        };

        bus.send(&tm)
    }
}

struct Router {
    /// Primary domain for this router instance.
    primary_domain: RouterDomain,

    /// Well-known address where top-level API calls should be routed.
    listen_address: RouterAddress,

    remote_domains: Vec<RouterDomain>,

    config: conf::Config,
}

impl Router {
    pub fn new(config: conf::Config) -> Self {
        let busconf = config.primary_connection().unwrap();
        let domain = busconf.domain().name();
        let addr = RouterAddress::new(domain);
        let primary_domain = RouterDomain::new(&busconf);

        Router {
            config,
            primary_domain,
            listen_address: addr,
            remote_domains: Vec::new(),
        }
    }

    fn init(&mut self) -> Result<(), String> {
        self.primary_domain.connect()?;
        self.setup_stream()?;
        Ok(())
    }

    fn primary_domain(&self) -> &RouterDomain {
        &self.primary_domain
    }

    fn remote_domains(&self) -> &Vec<RouterDomain> {
        &self.remote_domains
    }

    /// Setup the Redis stream/group we listen to
    pub fn setup_stream(&mut self) -> Result<(), String> {
        let sname = self.listen_address.full();

        info!("Setting up primary stream={sname}");

        let bus = &mut self
            .primary_domain
            .bus_mut()
            .expect("Primary domain must have a bus");

        bus.setup_stream(Some(sname))
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            listen_address: json::from(self.listen_address.full()),
            primary_domain: self.primary_domain().to_json_value(),
            remote_domains: json::from(self.remote_domains().iter()
                .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }

    /// Find or create a new RouterDomain entry.
    fn find_or_create_domain(&mut self, domain: &str) -> Result<&mut RouterDomain, String> {
        if self.primary_domain.domain.eq(domain) {
            return Ok(&mut self.primary_domain);
        }

        let mut pos_op = self.remote_domains.iter().position(|d| d.domain.eq(domain));

        if pos_op.is_none() {
            debug!("Adding new RouterDomain for domain={}", domain);

            // Primary connection is required at this point.
            let mut busconf = self.config.primary_connection().unwrap().clone();

            if let Some(d) = self.config.get_domain(domain) {
                busconf.set_domain(d);
            } else {
                return Err(format!("Cannot route to unknown domain: {domain}"));
            }

            self.remote_domains.push(RouterDomain::new(&busconf));

            pos_op = Some(self.remote_domains.len() - 1);
        }

        // Here the position is known to have data.
        Ok(self.remote_domains.get_mut(pos_op.unwrap()).unwrap())
    }

    fn handle_unregister(&mut self, address: &BusAddress, service: &str) -> Result<(), String> {
        let domain = address.domain().unwrap(); // Known client address

        info!(
            "De-registering domain={} service={} address={}",
            domain, service, address
        );

        if self.primary_domain.domain.eq(domain) {
            // When removing a service from the primary domain, leave the
            // domain as a whole intact since we'll likely need it again.
            // Remove services and controllers as necessary, though.

            self.primary_domain.remove_service(service, &address);
            return Ok(());
        }

        // When removing the last service from a remote domain, remove
        // the domain entry as a whole.
        let mut rem_pos_op: Option<usize> = None;
        let mut idx = 0;

        for r_domain in &mut self.remote_domains {
            if r_domain.domain().eq(domain) {
                r_domain.remove_service(service, address);
                if r_domain.services.len() == 0 {
                    // Cannot remove here since it would be modifying
                    // self.remote_domains while it's aready mutably borrowed.
                    rem_pos_op = Some(idx);
                }
                break;
            }
            idx += 1;
        }

        if let Some(pos) = rem_pos_op {
            debug!("Removing cleared domain entry for domain={}", domain);
            self.remote_domains.remove(pos);
        }

        Ok(())
    }

    fn handle_register(&mut self, address: BusAddress, service: &str) -> Result<(), String> {
        let domain = address.domain().unwrap(); // Known to be a client addr.

        info!("Registering new domain={}", domain);

        let r_domain = self.find_or_create_domain(domain)?;

        for svc in &mut r_domain.services {
            // See if we have a ServiceEntry for this service on this domain.

            if svc.name.eq(service) {
                for controller in &mut svc.controllers {
                    if controller.address.full().eq(address.full()) {
                        warn!(
                            "Controller with address {} already registered for service {} and domain {}",
                            address, service, domain
                        );
                        return Ok(());
                    }
                }

                debug!(
                    "Adding new ServiceInstance domain={} service={} address={}",
                    domain, service, address
                );

                svc.controllers.push(ServiceInstance {
                    address: address.clone(),
                    register_time: Local::now(),
                });

                return Ok(());
            }
        }

        // We have no Service Entry for this domain+service+address.
        // Add a ServiceEntry and a new ServiceInstance

        debug!(
            "Adding new ServiceEntry domain={} service={} address={}",
            domain, service, address
        );

        r_domain.services.push(ServiceEntry {
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
            .primary_domain()
            .services()
            .iter()
            .map(|s| s.name())
            .collect();

        for d in self.remote_domains() {
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
        // domain and route accordingly.

        loop {
            let tm = match self.recv_one() {
                Ok(m) => m,
                Err(s) => {
                    error!(
                        "Error receiving data on our primary domain connection: {}",
                        s
                    );
                    continue; // break and exit?
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
            return self.route_api_request(&addr, tm);
        } else if addr.is_router() {
            return self.handle_router_command(tm);
        } else {
            return Err(format!("Unexpected message recipient: {}", to));
        }
    }

    fn route_api_request(
        &mut self,
        to_addr: &BusAddress,
        tm: TransportMessage,
    ) -> Result<(), String> {
        let service = to_addr.service().unwrap(); // required for is_service

        if self.primary_domain.has_service(service) {
            self.primary_domain.route_count += 1;
            return self.primary_domain.send_to_domain(tm);
        }

        for r_domain in &mut self.remote_domains {
            if r_domain.has_service(service) {
                if r_domain.bus.is_none() {
                    // We only connect to remote domains when it's
                    // time to send them a message.
                    r_domain.connect()?;
                }

                r_domain.route_count += 1;
                return r_domain.send_to_domain(tm);
            }
        }

        error!("We have no service controllers for service {service}");

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

        let stat = Message::new(MessageType::Status, trace, payload);

        let tm = TransportMessage::with_body(
            tm.from(), // Bounce it back
            self.listen_address.full(),
            tm.thread(),
            stat,
        );

        // Bounce-backs will always be directed back to a client
        // on our primary domain, since clients only ever talk to
        // the router on their own domain.
        self.primary_domain.send_to_domain(tm)
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

        let from_addr = BusAddress::new_from_string(from)?;

        if !from_addr.is_client() {
            return Err(format!("Router command received from non-client address"));
        }

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
        from_addr: BusAddress,
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
        tm.set_from(self.primary_domain.bus().unwrap().address().full());
        tm.set_to(from_addr.full());

        let domain = match from_addr.domain() {
            Some(d) => d,
            None => {
                return Err(format!(
                    "Cannot send router reply to addrwess with hno domain: {from_addr}"
                ));
            }
        };

        let r_domain = self.find_or_create_domain(domain)?;

        if r_domain.bus.is_none() {
            r_domain.connect()?;
        }

        r_domain.send_to_domain(tm)
    }

    fn recv_one(&mut self) -> Result<TransportMessage, String> {
        let bus = self
            .primary_domain
            .bus_mut()
            .expect("We always maintain a connection on the primary domain");

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

fn main() {
    let config = conf::Config::from_file("conf/opensrf.yml").unwrap();
    let ctype = config.get_connection_type("router").unwrap();

    // Init our global logger instance so we can use, e.g. info!(...)
    Logger::new(ctype.log_level(), ctype.log_facility())
        .init()
        .unwrap();

    // Each domain gets a router running in its own thread.
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();

    for domain in config.domains() {
        let mut conf = config.clone();
        conf.set_primary_connection("router", domain.name())
            .unwrap();

        threads.push(thread::spawn(|| {
            let mut router = Router::new(conf);
            router.init().unwrap();
            router.listen();
        }));
    }

    // Block here while the routers are running.
    for thread in threads {
        thread.join().ok();
    }
}
