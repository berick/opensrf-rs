use opensrf::addr::BusAddress;
use opensrf::bus::Bus;
use opensrf::client::Client;
use opensrf::conf::BusConfig;
use opensrf::conf::ClientConfig;
use opensrf::message::TransportMessage;

const PRIVATE_SERVICE: &str = "opensrf.private";
const PUBLIC_SERVICE: &str = "opensrf.public";

const PRIVATE_DOMAIN: &str = "private.localhost";
const PUBLIC_DOMAIN: &str = "public.localhost";

fn main() {
    // Useful for logging
    let mut conf = ClientConfig::new();

    // Force the config to use the private domain.
    conf.bus_config_mut().set_domain(PRIVATE_DOMAIN);
    conf.load_file("conf/opensrf_client.yml").unwrap();

    let mut conf2 = conf.clone();

    let mut pvt_client = Client::new(conf).unwrap();

    pvt_client
        .send_router_command(PRIVATE_DOMAIN, "register", PRIVATE_SERVICE)
        .unwrap();

    conf2.bus_config_mut().set_domain(PUBLIC_DOMAIN);

    let mut pub_client = Client::new(conf2).unwrap();

    pub_client
        .send_router_command(PRIVATE_DOMAIN, "register", PUBLIC_SERVICE)
        .unwrap();
    pub_client
        .send_router_command(PUBLIC_DOMAIN, "register", PUBLIC_SERVICE)
        .unwrap();

    pvt_client
        .send_router_command(PRIVATE_DOMAIN, "unregister", PRIVATE_SERVICE)
        .unwrap();
    pub_client
        .send_router_command(PRIVATE_DOMAIN, "unregister", PUBLIC_SERVICE)
        .unwrap();
    pub_client
        .send_router_command(PUBLIC_DOMAIN, "unregister", PUBLIC_SERVICE)
        .unwrap();
}
