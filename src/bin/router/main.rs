use chrono::prelude::{DateTime, Local};
use log::{debug, error, info, trace, warn};
use opensrf::addr::{BusAddress, ClientAddress, RouterAddress, ServiceAddress};
use opensrf::bus::Bus;
use opensrf::conf;
use opensrf::message::{Message, MessageStatus, MessageType, Payload, Status, TransportMessage};
use opensrf::Logger;
use std::thread;
use std::env;
use getopts::Options;

/// A service controller.
///
/// This is what we traditionally call a "Listener" in OpenSRF.
/// A service can have multiple controllers on a single subdomain.
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

/// One subdomain entry.
///
/// A subdomain will typically host multiple services.
/// E.g. "public.localhost"
struct RouterDomain {
    // e.g. public.localhost
    subdomain: String,

    /// Bus connection to the redis instance for this subdomain.
    ///
    /// A connection is only opened when needed.  Once opened, it's left
    /// open until the connection is shut down on the remote end.
    bus: Option<Bus>,

    /// How many requests have been routed to this subdomain.
    ///
    /// We count subdomain-level routing instead of service controller-level
    /// routing, since we can't guarantee which service controller will
    /// pick up any given request routed to a subdomain.
    route_count: usize,

    services: Vec<ServiceEntry>,

    config: conf::BusConnection,
}

impl RouterDomain {
    fn new(config: &conf::BusConnection) -> Self {
        RouterDomain {
            subdomain: config.subdomain().to_string(),
            bus: None,
            route_count: 0,
            services: Vec::new(),
            config: config.clone(),
        }
    }

    fn subdomain(&self) -> &str {
        &self.subdomain
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
            subdomain: json::from(self.subdomain()),
            route_count: json::from(self.route_count()),
            services: json::from(self.services().iter()
                .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }

    /// Connect to the Redis instance on this subdomain.
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

    /// Send a message to this subdomain via our subdomain connection.
    fn send_to_subdomain(&mut self, tm: TransportMessage) -> Result<(), String> {
        trace!(
            "send_to_subdomain({}) routing message to {}",
            self.subdomain(),
            tm.to()
        );

        let bus = match &mut self.bus {
            Some(b) => b,
            None => {
                return Err(format!("We have no connection to subdomain {}", self.subdomain()));
            }
        };

        bus.send(&tm)
    }
}

struct Router {
    /// Primary subdomain for this router instance.
    primary_subdomain: RouterDomain,

    /// Well-known address where top-level API calls should be routed.
    listen_address: RouterAddress,

    remote_subdomains: Vec<RouterDomain>,

    config: conf::Config,
}

impl Router {
    pub fn new(config: conf::Config) -> Self {
        let busconf = config.primary_connection().unwrap();
        let subdomain = busconf.subdomain().to_string();
        let addr = RouterAddress::new(&subdomain);
        let primary_subdomain = RouterDomain::new(&busconf);

        Router {
            config,
            primary_subdomain,
            listen_address: addr,
            remote_subdomains: Vec::new(),
        }
    }

    fn init(&mut self) -> Result<(), String> {
        self.primary_subdomain.connect()?;
        self.setup_stream()?;
        Ok(())
    }

    fn primary_subdomain(&self) -> &RouterDomain {
        &self.primary_subdomain
    }

    fn remote_subdomains(&self) -> &Vec<RouterDomain> {
        &self.remote_subdomains
    }

    /// Setup the Redis stream/group we listen to
    pub fn setup_stream(&mut self) -> Result<(), String> {
        let sname = self.listen_address.full();

        info!("Setting up primary stream={sname}");

        let bus = &mut self
            .primary_subdomain
            .bus_mut()
            .expect("Primary subdomain must have a bus");

        bus.delete_named_stream(sname)?;
        bus.setup_stream(Some(sname))
    }

    fn to_json_value(&self) -> json::JsonValue {
        json::object! {
            listen_address: json::from(self.listen_address.full()),
            primary_subdomain: self.primary_subdomain().to_json_value(),
            remote_subdomains: json::from(self.remote_subdomains().iter()
                .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }

    /// Find or create a new RouterDomain entry.
    fn find_or_create_subdomain(&mut self, subdomain: &str) -> Result<&mut RouterDomain, String> {
        if self.primary_subdomain.subdomain.eq(subdomain) {
            return Ok(&mut self.primary_subdomain);
        }

        let mut pos_op = self.remote_subdomains.iter().position(|d| d.subdomain.eq(subdomain));

        if pos_op.is_none() {
            debug!("Adding new RouterDomain for subdomain={}", subdomain);

            // Primary connection is required at this point.
            let mut busconf = self.config.primary_connection().unwrap().clone();

            if self.config.get_subdomain(subdomain).is_some() {
                busconf.set_subdomain(subdomain);
            } else {
                return Err(format!("Cannot route to unknown subdomain: {subdomain}"));
            }

            self.remote_subdomains.push(RouterDomain::new(&busconf));

            pos_op = Some(self.remote_subdomains.len() - 1);
        }

        // Here the position is known to have data.
        Ok(self.remote_subdomains.get_mut(pos_op.unwrap()).unwrap())
    }

    fn handle_unregister(&mut self, address: &ClientAddress, service: &str) -> Result<(), String> {
        let subdomain = address.domain();

        info!(
            "De-registering subdomain={} service={} address={}",
            subdomain, service, address
        );

        if self.primary_subdomain.subdomain.eq(subdomain) {
            // When removing a service from the primary subdomain, leave the
            // subdomain as a whole intact since we'll likely need it again.
            // Remove services and controllers as necessary, though.

            self.primary_subdomain.remove_service(service, &address);
            return Ok(());
        }

        // When removing the last service from a remote subdomain, remove
        // the subdomain entry as a whole.
        let mut rem_pos_op: Option<usize> = None;
        let mut idx = 0;

        for r_subdomain in &mut self.remote_subdomains {
            if r_subdomain.subdomain().eq(subdomain) {
                r_subdomain.remove_service(service, address);
                if r_subdomain.services.len() == 0 {
                    // Cannot remove here since it would be modifying
                    // self.remote_subdomains while it's aready mutably borrowed.
                    rem_pos_op = Some(idx);
                }
                break;
            }
            idx += 1;
        }

        if let Some(pos) = rem_pos_op {
            debug!("Removing cleared subdomain entry for subdomain={}", subdomain);
            self.remote_subdomains.remove(pos);
        }

        Ok(())
    }

    fn handle_register(&mut self, address: ClientAddress, service: &str) -> Result<(), String> {
        let subdomain = address.domain(); // Known to be a client addr.

        info!("Registering new subdomain={}", subdomain);

        let r_subdomain = self.find_or_create_subdomain(subdomain)?;

        for svc in &mut r_subdomain.services {
            // See if we have a ServiceEntry for this service on this subdomain.

            if svc.name.eq(service) {
                for controller in &mut svc.controllers {
                    if controller.address.full().eq(address.full()) {
                        warn!(
                            "Controller with address {} already registered for service {} and subdomain {}",
                            address, service, subdomain
                        );
                        return Ok(());
                    }
                }

                debug!(
                    "Adding new ServiceInstance subdomain={} service={} address={}",
                    subdomain, service, address
                );

                svc.controllers.push(ServiceInstance {
                    address: address.clone(),
                    register_time: Local::now(),
                });

                return Ok(());
            }
        }

        // We have no Service Entry for this subdomain+service+address.
        // Add a ServiceEntry and a new ServiceInstance

        debug!(
            "Adding new ServiceEntry subdomain={} service={} address={}",
            subdomain, service, address
        );

        r_subdomain.services.push(ServiceEntry {
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
            .primary_subdomain()
            .services()
            .iter()
            .map(|s| s.name())
            .collect();

        for d in self.remote_subdomains() {
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
        // subdomain and route accordingly.

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

        if self.primary_subdomain.has_service(service) {
            self.primary_subdomain.route_count += 1;
            return self.primary_subdomain.send_to_subdomain(tm);
        }

        for r_subdomain in &mut self.remote_subdomains {
            if r_subdomain.has_service(service) {
                if r_subdomain.bus.is_none() {
                    // We only connect to remote subdomains when it's
                    // time to send them a message.
                    r_subdomain.connect()?;
                }

                r_subdomain.route_count += 1;
                return r_subdomain.send_to_subdomain(tm);
            }
        }

        error!(
            "Router at {} has no service controllers for service {service}",
            self.primary_subdomain.subdomain()
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

        let from = match self.primary_subdomain.bus() {
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
        // on our primary subdomain, since clients only ever talk to
        // the router on their own subdomain.
        self.primary_subdomain.send_to_subdomain(tm)
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
        tm.set_from(self.primary_subdomain.bus().unwrap().address().full());
        tm.set_to(from_addr.full());

        let r_subdomain = self.find_or_create_subdomain(from_addr.domain())?;

        if r_subdomain.bus.is_none() {
            r_subdomain.connect()?;
        }

        r_subdomain.send_to_subdomain(tm)
    }

    fn recv_one(&mut self) -> Result<TransportMessage, String> {
        let bus = self
            .primary_subdomain
            .bus_mut()
            .expect("We always maintain a connection on the primary subdomain");

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
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();

    opts.optopt("h", "host", "Hostname", "HOSTNAME");
    opts.optopt("d", "domain", "domain", "domain");
    opts.optflag("l", "localhost", "Use Localhost");
    opts.optopt("c", "osrf-config", "OpenSRF Config", "OSRF_CONFIG");

    let params = match opts.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => {
            println!("\n{e}\n{}", opts.usage("Usage: "));
            return;
        }
    };

    let conf_file = params.opt_get_default(
        "osrf-config", "conf/opensrf_core.yml".to_string()).unwrap();

    let config = conf::Config::from_file(&conf_file).unwrap();

    let domain;

    if let Some(d) = params.opt_str("domain") {
        domain = d.to_string();
    } else if params.opt_present("localhost") {
        domain = "localhost".to_string();
    } else {
        eprintln!("Router requires --localhost or a --domain value");
        return;
    }

    let contype = match config.get_connection_type("private_router") {
        Some(c) => c,
        None => panic!("No such connection type: private_router"),
    };

    Logger::new(&contype).init().unwrap();

    // Each subdomain gets 1 router for each of its 2 public/private sudbomains.
    // Each runs in its own thread.
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();

    // Private Router Thread ---
    let mut conf = config.clone();
    conf.set_primary_connection("private_router", &domain).unwrap();

	threads.push(thread::spawn(|| {
		let mut router = Router::new(conf);
		router.init().unwrap();
		router.listen();
	}));

    // Public Router Thread ---
    let mut conf = config.clone();
    conf.set_primary_connection("public_router", &domain).unwrap();

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
