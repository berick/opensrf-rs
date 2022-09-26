use chrono::prelude::{DateTime, Local};
use log::{debug, error, info, trace, warn};
use opensrf::addr::BusAddress;
use opensrf::bus::Bus;
use opensrf::conf::BusConfig;
use opensrf::conf::ClientConfig;
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

        let bus = match Bus::new(&conf, None) {
            Ok(b) => b,
            Err(e) => return Err(format!("Cannot connect bus: {}", e)),
        };

        self.bus = Some(bus);

        Ok(())
    }

    /// Send a message to this domain via our domain connection.
    fn send_to_domain(&mut self, addr: &BusAddress, msg: &json::JsonValue) -> Result<(), String> {
        let json_str = msg.dump();

        trace!("send() writing chunk to={}: {}", addr.full(), json_str);

        let maxlen = StreamMaxlen::Approx(1000); // TODO CONFIG

        // TODO use bus.send() instead

        let bus = match &mut self.bus {
            Some(b) => b,
            None => {
                return Err(format!("We have no connection to domain {}", self.domain()));
            }
        };

        let res: Result<String, _> =
            bus.connection()
                .xadd_maxlen(addr.full(), maxlen, "*", &[("message", json_str)]);

        if let Err(e) = res {
            return Err(format!("Error sending to domain {} : {}", self.domain(), e));
        };

        Ok(())
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
        let sname = self.listen_address.full().to_string();

        info!("Setting up primary stream={} group={}", &sname, &sname);

        let con = &mut self.primary_domain.bus_mut().unwrap().connection();

        let created: Result<(), _> = con.xgroup_create_mkstream(&sname, &sname, "$");

        if let Err(e) = created {
            // TODO Differentiate error types.
            // Some errors are worse than others.
            debug!("stream group {} probably already exists: {}", &sname, e);
        }

        Ok(())
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
            let msg_str = match self.recv_one() {
                Ok(s) => s,
                Err(s) => {
                    error!(
                        "Error receiving data on our primary domain connection: {}",
                        s
                    );
                    continue; // break and exit?
                }
            };

            if let Err(s) = self.route_message(&msg_str) {
                error!("Error routing message: {}", s);
            }
        }
    }

    fn route_message(&mut self, msg_str: &str) -> Result<(), String> {
        let json_val = match json::parse(msg_str) {
            Ok(v) => v,
            Err(e) => {
                return Err(format!(
                    "Failed to parse message as JSON: {} {}",
                    e, msg_str
                ));
            }
        };

        let to = match json_val["to"].as_str() {
            Some(i) => i,
            None => {
                return Err(format!("Message has no recipient: {}", msg_str));
            }
        };

        debug!("Received message destined for {}", to);

        let addr = BusAddress::new_from_string(to)?;

        if addr.is_service() {
            return self.route_api_request(&addr, &json_val);
        } else if addr.is_router() {
            return self.handle_router_command(&json_val);
        } else {
            return Err(format!("Unexpected message recipient: {}", to));
        }
    }

    fn route_api_request(
        &mut self,
        addr: &BusAddress,
        json_val: &json::JsonValue,
    ) -> Result<(), String> {
        let service = addr.service().unwrap(); // required for is_service

        if self.primary_domain.has_service(service) {
            return self.primary_domain.send_to_domain(addr, json_val);
        }

        for r_domain in &mut self.remote_domains {
            if r_domain.has_service(service) {
                if r_domain.bus.is_none() {
                    // We only connect to remote domains when it's
                    // time to send them a message.
                    r_domain.connect(&self.bus_username, &self.bus_password, self.bus_port)?;
                }

                return r_domain.send_to_domain(addr, json_val);
            }
        }

        // TODO communicate this to the caller.

        return Err(format!(
            "We have no service controllers for service {}",
            service
        ));
    }

    fn handle_router_command(&mut self, json_val: &json::JsonValue) -> Result<(), String> {
        let router_command = match json_val["router_command"].as_str() {
            Some(s) => s,
            None => {
                return Err(format!("No router command present: {}", json_val));
            }
        };

        let from = match json_val["from"].as_str() {
            Some(s) => s,
            None => {
                return Err(format!(
                    "Router command message has no 'from': {}",
                    json_val
                ));
            }
        };

        let from_addr = match BusAddress::new_from_string(from) {
            Ok(a) => a,
            Err(e) => {
                return Err(format!(
                    "Router command received invalid from address: {}",
                    e
                ));
            }
        };

        if !from_addr.is_client() {
            return Err(format!("Router command received from non-client address"));
        }

        debug!(
            "Router command received command={} from={}",
            router_command, from
        );

        // Not all router commands require a router class.
        let router_class_op = json_val["router_class"].as_str();

        if router_command.eq("register") {
            match router_class_op {
                Some(rclass) => {
                    return self.handle_register(from_addr, rclass);
                }
                None => {
                    return Err(format!("No router class defined: {}", json_val));
                }
            }
        } else if router_command.eq("unregister") {
            match router_class_op {
                Some(rclass) => {
                    return self.handle_unregister(&from_addr, rclass);
                }
                None => {
                    return Err(format!("No router class defined: {}", json_val));
                }
            }
        }

        Ok(())
    }

    fn recv_one(&mut self) -> Result<String, String> {
        // TODO use bus.recv()

        let addr = self.listen_address.full();

        let read_opts = StreamReadOptions::default()
            .count(1) // One at a time, please
            .block(0) // Block indefinitely
            .noack() // We don't need ACK's
            .group(addr, addr);

        let bus = self
            .primary_domain
            .bus_mut()
            .expect("We always maintain a connection on the primary domain");

        let connection = bus.connection();

        debug!("Waiting for messages at {}", addr);

        let reply: StreamReadReply = match connection.xread_options(&[&addr], &[">"], &read_opts) {
            Ok(r) => r,
            Err(e) => match e.kind() {
                redis::ErrorKind::TypeError => {
                    return Err(format!(
                        "Redis returned unexpected type; could be a signal interrupt"
                    ));
                }
                _ => {
                    return Err(format!(
                        "Error reading Redis instance for primary domain: {}",
                        e
                    ));
                }
            },
        };

        for StreamKey { key, ids } in reply.keys {
            trace!("Read value from stream {}", key);

            for StreamId { id, map } in ids {
                trace!("Read message ID {}", id);

                if let Some(message) = map.get("message") {
                    if let Value::Data(bytes) = message {
                        if let Ok(s) = String::from_utf8(bytes.to_vec()) {
                            trace!("Read value from bus: {}", s);
                            return Ok(s);
                        } else {
                            return Err(format!("Received unexpected stream data: {:?}", message));
                        };
                    } else {
                        return Err(format!("Received unexpected stream data"));
                    }
                };
            }
        }

        Err(format!("No value read from primary domain connection"))
    }
}

fn main() {
    let mut conf = ClientConfig::new();

    conf.load_file("conf/opensrf_client.yml").unwrap();

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
