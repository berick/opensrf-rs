use opensrf::bus::Bus;
use opensrf::addr::BusAddress;
use opensrf::conf::BusConfig;
use opensrf::conf::ClientConfig;
use opensrf::message::TransportMessage;

const PRIVATE_SERVICE: &str = "opensrf.private";
const PRIVATE_ROUTER: &str = "opensrf:router:private.localhost";
const PUBLIC_SERVICE: &str = "opensrf.public";
const PUBLIC_ROUTER: &str = "opensrf:router:public.localhost";

fn main() {

    // Useful for logging
    let mut conf = ClientConfig::new();
    conf.load_file("conf/opensrf_client.yml").expect("Cannot load config file");

    /// Create a bus for a service running on private.localhost
    let mut conf = BusConfig::new();
    conf.set_username("opensrf@private");
    conf.set_password("password");
    conf.set_domain("private.localhost");

    let mut pvt_bus = Bus::new(&conf, Some(PRIVATE_SERVICE)).unwrap();

    /// Create a bus for a service running on public.localhost
    conf.set_username("opensrf@private");

    let mut pub_bus = Bus::new(&conf, Some(PUBLIC_SERVICE)).unwrap();

    // Register with the private router

    let mut tmsg = TransportMessage::new(PRIVATE_ROUTER, pvt_bus.address().full(), "router-thread");
    tmsg.set_router_command("register");
    tmsg.set_router_class(PRIVATE_SERVICE);

    pvt_bus.send(&tmsg).unwrap();

}



