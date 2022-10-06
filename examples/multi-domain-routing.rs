use opensrf::Client;
use opensrf::ClientConfig;
use opensrf::addr::BusAddress;

const PRIVATE_SERVICE: &str = "opensrf.private";
const PUBLIC_SERVICE: &str = "opensrf.public";

const PRIVATE_DOMAIN: &str = "private.localhost";
const PUBLIC_DOMAIN: &str = "public.localhost";

fn main() -> Result<(), String> {
    let mut conf = ClientConfig::from_file("conf/opensrf_client.yml")?;
    conf.enable_multi_domain_support();

    // Force the config to use the private domain.
    conf.bus_config_mut().set_domain(PRIVATE_DOMAIN);

    // Force our 2ndary config to use the public domain
    let mut conf2 = conf.clone();
    conf2.bus_config_mut().set_domain(PUBLIC_DOMAIN);

    // Create a public and private client for x-domain communication
    let mut pvt_client = Client::new(conf)?;
    let mut pub_client = Client::new(conf2)?;

    let pvt_addr = BusAddress::new_for_service(PRIVATE_SERVICE);
    let pub_addr = BusAddress::new_for_service(PUBLIC_SERVICE);

    // Register the service-level listen stream
    pvt_client
        .client_mut()
        .bus_mut()
        .setup_stream(Some(pvt_addr.full()))?;
    pvt_client
        .client_mut()
        .bus_mut()
        .setup_stream(Some(pub_addr.full()))?;

    pvt_client.send_router_command(PRIVATE_DOMAIN, "register", Some(PRIVATE_SERVICE), false)?;
    pub_client.send_router_command(PRIVATE_DOMAIN, "register", Some(PUBLIC_SERVICE), false)?;
    pub_client.send_router_command(PUBLIC_DOMAIN, "register", Some(PUBLIC_SERVICE), false)?;

    // Send a x-domain request
    let mut ses = pvt_client.session(PUBLIC_SERVICE);
    let mut req = ses.request("opensrf.system.echo", vec!["Hello", "Goodbye"]);

    // See if our request was correctly routed.
    let resp_op = pub_client
        .client_mut()
        .bus_mut()
        .recv(10, Some(pub_addr.full()))?;

    if let Some(resp) = resp_op {
        println!("Routed message arrived: {}", resp.to_json_value().dump());
    }

    if let Some(jv) = pvt_client.send_router_command(PRIVATE_DOMAIN, "summarize", None, true)? {
        println!("Router command returned: {}", jv.dump());
    }

    if let Some(jv) = pub_client.send_router_command(PUBLIC_DOMAIN, "summarize", None, true)? {
        println!("Router command returned: {}", jv.dump());
    }


    pvt_client.send_router_command(PRIVATE_DOMAIN, "unregister", Some(PRIVATE_SERVICE), false)?;
    pub_client.send_router_command(PRIVATE_DOMAIN, "unregister", Some(PUBLIC_SERVICE), false)?;
    pub_client.send_router_command(PUBLIC_DOMAIN, "unregister", Some(PUBLIC_SERVICE), false)?;

    Ok(())
}
