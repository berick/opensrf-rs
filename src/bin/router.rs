use chrono::prelude::{DateTime, Local};
use log::{debug, error, info, trace, warn};
use opensrf::addr::BusAddress;
use opensrf::bus::Bus;
use opensrf::conf::BusConfig;
use opensrf::conf::ClientConfig;
use opensrf::message::TransportMessage;
use redis::streams::{StreamId, StreamKey, StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{Commands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo, Value};
use std::collections::HashMap;
use std::fmt;
use std::thread;

const DEFAULT_REDIS_PORT: u16 = 6379;

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
}

impl RouterDomain {
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

            if let Some(c_pos) = svc
                .controllers
                .iter()
                .position(|c| c.address().full().eq(address.full()))
            {
                if svc.controllers.len() == 1 {
                    debug!(
                        "Removing registration for service={} on removal of last controller address={}",
                        service, address
                    );

                    if let Some(s_pos) = self.services.iter().position(|s| s.name().eq(service)) {
                        self.services.remove(s_pos);
                    }
                } else {
                    debug!("Removing registration for service={}", service);
                    svc.controllers.remove(c_pos);
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
    fn connect(&mut self, username: &str, password: &str, port: u16) -> Result<(), String> {
        if self.bus.is_some() {
            return Ok(());
        }

        let mut conf = BusConfig::new();
        conf.set_domain(self.domain());
        conf.set_port(port);
        conf.set_username(username);
        conf.set_password(password);

        let bus = match Bus::new(&conf) {
            Ok(b) => b,
            Err(e) => return Err(format!("Cannot connect bus: {}", e)),
        };

        self.bus = Some(bus);

        Ok(())
    }

    /// Send a message to this domain via our domain connection.
    fn send_to_domain(&mut self, tm: TransportMessage) -> Result<(), String> {
        trace!("send_to_domain() routing API call to {}", tm.to());

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
    listen_address: BusAddress,

    remote_domains: Vec<RouterDomain>,

    // Retain the bus connections details at the Router level so they
    // can be used when connecting to each new domain.
    bus_username: String,
    bus_password: String,
    bus_port: u16,
}

impl Router {
    pub fn new(domain: &str, username: &str, password: &str, port: u16) -> Self {
        let addr = BusAddress::new_for_router(domain);

        let d = RouterDomain {
            domain: domain.to_string(),
            bus: None,
            route_count: 0,
            services: Vec::new(),
        };

        Router {
            bus_port: port,
            bus_username: username.to_string(),
            bus_password: password.to_string(),
            primary_domain: d,
            listen_address: addr,
            remote_domains: Vec::new(),
        }
    }

    fn init(&mut self) -> Result<(), String> {
        self.primary_domain
            .connect(&self.bus_username, &self.bus_password, self.bus_port)?;

        self.setup_stream()?;

        Ok(())
    }

    fn bus_port(&self) -> u16 {
        self.bus_port
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
            bus_username: json::from(self.bus_username.as_str()),
            listen_address: json::from(self.listen_address.full()),
            primary_domain: self.primary_domain().to_json_value(),
            remote_domains: json::from(self.remote_domains().iter()
                .map(|s| s.to_json_value()).collect::<Vec<json::JsonValue>>()
            )
        }
    }

    /// Find or create a new RouterDomain entry.
    fn find_or_create_domain(&mut self, domain: &str) -> Option<&mut RouterDomain> {
        if self.primary_domain.domain.eq(domain) {
            return Some(&mut self.primary_domain);
        }

        let mut pos_op = self.remote_domains.iter().position(|d| d.domain.eq(domain));

        if pos_op.is_none() {
            debug!("Adding new RouterDomain for domain={}", domain);

            self.remote_domains.push(RouterDomain {
                domain: domain.to_string(),
                bus: None,
                route_count: 0,
                services: Vec::new(),
            });

            pos_op = Some(self.remote_domains.len() - 1);
        }

        self.remote_domains.get_mut(pos_op.unwrap())
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

        let r_domain = match self.find_or_create_domain(domain) {
            Some(d) => d,
            None => {
                return Err(format!(
                    "Cannot find/create domain entry for domain {}",
                    domain
                ));
            }
        };

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
    fn active_services(&self) -> Vec<&str> {
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

    fn route_api_request(&mut self, addr: &BusAddress, tm: TransportMessage) -> Result<(), String> {
        let service = addr.service().unwrap(); // required for is_service

        if self.primary_domain.has_service(service) {
            return self.primary_domain.send_to_domain(tm);
        }

        for r_domain in &mut self.remote_domains {
            if r_domain.has_service(service) {
                if r_domain.bus.is_none() {
                    // We only connect to remote domains when it's
                    // time to send them a message.
                    r_domain.connect(&self.bus_username, &self.bus_password, self.bus_port)?;
                }

                return r_domain.send_to_domain(tm);
            }
        }

        // TODO communicate this to the caller.

        return Err(format!(
            "We have no service controllers for service {}",
            service
        ));
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
        let get_class = || {
            if let Some(rc) = tm.router_class() {
                return Ok(rc);
            } else {
                return Err(format!(
                    "Message has not router class: {}",
                    tm.to_json_value().dump()
                ));
            }
        };

        match router_command {
            "register" => self.handle_register(from_addr, get_class()?),
            "unregister" => self.handle_unregister(&from_addr, get_class()?),
            _ => Err(format!("Unsupported router command: {router_command}")),
        }
    }

    fn recv_one(&mut self) -> Result<TransportMessage, String> {
        let bus = self
            .primary_domain
            .bus_mut()
            .expect("We always maintain a connection on the primary domain");

        loop {
            // Looping should not be required, but can't hurt.

            if let Some(tm) = bus.recv(-1, Some(self.listen_address.full()))? {
                return Ok(tm);
            } else {
                debug!("recv() on main bus address returned None.  Will keep trying");
            }
        }
    }
}

fn main() {
    let conf = ClientConfig::from_file("conf/opensrf_client.yml").unwrap();

    let t1 = thread::spawn(|| {
        let mut router: Router =
            Router::new("private.localhost", "opensrf@private", "password", 6379);

        router.init().expect("Router init");

        router.listen();
    });

    let t2 = thread::spawn(|| {
        let mut router: Router =
            Router::new("public.localhost", "opensrf@private", "password", 6379);

        router.init().expect("Router init");

        router.listen();
    });

    t1.join();
    t2.join();

    /*
    router.handle_register(
        "private.localhost",
        "opensrf.settings",
        "opensrf:client:opensrf.settings.123"
    ).expect("Register OK");

    router.handle_register(
        "public.localhost",
        "opensrf.math",
        "opensrf:client:opensrf.math.123"
    ).expect("Register OK");

    println!("{}", router.to_json_value().dump());

    router.handle_unregister(
        "public.localhost",
        "opensrf.math",
        "opensrf:client:opensrf.math.123"
    ).expect("UnRegister OK");
    */

    //println!("{}", router.to_json_value().dump());
}
