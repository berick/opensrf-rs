use opensrf::addr::BusAddress;
use opensrf::Client;
use opensrf::Config;
use opensrf::Logger;

const PRIVATE_SERVICE: &str = "opensrf.private";
const PUBLIC_SERVICE: &str = "opensrf.public";

const PRIVATE_DOMAIN: &str = "private.localhost";
const PUBLIC_DOMAIN: &str = "public.localhost";

/*
 * Setup 2 clients on the private domain, one private service, and one
 * public service.  Both register with the router on the private domain.  The public
 * service also registers with the public domain router.
 *
 * Add a 3rd client that acts as a public-domain client.  Have it send
 * requests to both services.  It should get through to the public service
 * and should be rejected from the public service.
 */

fn main() -> Result<(), String> {
    let mut conf = Config::from_file("conf/opensrf.yml")?;
    let connection = conf.set_primary_connection("service", PRIVATE_DOMAIN)?;

    let ctype = connection.connection_type();
    Logger::new(ctype.log_level(), ctype.log_facility()).init().unwrap();

    let conf2 = conf.clone();

    // Force our 3rd config to use the public domain
    let mut conf3 = conf.clone();
    conf3.set_primary_connection("gateway", PUBLIC_DOMAIN);

    // Create a public and private client for x-domain communication
    let mut pvt_client = Client::new(conf)?;
    let mut pvt_client2 = Client::new(conf2)?;
    let mut pub_client = Client::new(conf3)?;

    /*
    let pvt_addr = BusAddress::new_for_service(PRIVATE_SERVICE);
    let pub_addr = BusAddress::new_for_service(PUBLIC_SERVICE);

    // Register the service-level listen stream
    pvt_client
        .client_mut()
        .bus_mut()
        .setup_stream(Some(pvt_addr.full()))?;

    pvt_client2
        .client_mut()
        .bus_mut()
        .setup_stream(Some(pub_addr.full()))?;
    */

    pvt_client.send_router_command(PRIVATE_DOMAIN, "register", Some(PRIVATE_SERVICE), false)?;
    pvt_client2.send_router_command(PRIVATE_DOMAIN, "register", Some(PUBLIC_SERVICE), false)?;
    pvt_client2.send_router_command(PUBLIC_DOMAIN, "register", Some(PUBLIC_SERVICE), false)?;

    // Send a x-domain request
    let mut ses = pvt_client.session(PUBLIC_SERVICE);
    let mut req = ses.request("opensrf.system.echo", vec!["Hello", "Goodbye"])?;

    if let Some(resp) = req.recv(1)? {
        println!("Routed message arrived: {}", resp.dump());
    } else {
        println!(
            "Routed request did not receive a reply.  Possibly {PUBLIC_SERVICE} is not running"
        );
    }

    if let Some(jv) = pvt_client.send_router_command(PRIVATE_DOMAIN, "summarize", None, true)? {
        println!("Router command returned: {}", jv.dump());
    }

    if let Some(jv) = pvt_client.send_router_command(PUBLIC_DOMAIN, "summarize", None, true)? {
        println!("Router command returned: {}", jv.dump());
    }

    // Send a request for a private service on our public router.
    // This should result in a failure on receive.
    let mut ses = pub_client.session(PRIVATE_SERVICE);
    let mut req = ses.request("opensrf.system.echo", vec!["Hello", "Goodbye"])?;

    if let Err(e) = req.recv(5) {
        println!("This should fail:: {e}");
    }

    pvt_client.send_router_command(PRIVATE_DOMAIN, "unregister", Some(PRIVATE_SERVICE), false)?;
    pvt_client2.send_router_command(PRIVATE_DOMAIN, "unregister", Some(PUBLIC_SERVICE), false)?;
    pvt_client2.send_router_command(PUBLIC_DOMAIN, "unregister", Some(PUBLIC_SERVICE), false)?;

    Ok(())
}
