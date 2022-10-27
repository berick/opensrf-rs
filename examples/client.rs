use opensrf::Client;
use opensrf::Config;
use opensrf::Logger;

const SERVICE: &str = "opensrf.rspublic";
const METHOD: &str = "opensrf.rspublic.echo";
const DOMAIN: &str = "public.localhost";

fn main() -> Result<(), String> {
    let mut conf = Config::from_file("conf/opensrf.yml")?;

    let connection = conf.set_primary_connection("service", DOMAIN)?;

    let ctype = connection.connection_type();
    Logger::new("client", ctype.log_level(), ctype.log_facility())
        .init()
        .unwrap();

    let mut client = Client::new(conf.to_shared())?;

    if let Some(jv) = client.send_router_command(DOMAIN, "summarize", None, true)? {
        println!("Router command returned: {}", jv.dump());
    }

    let mut ses = client.session(SERVICE);
    let mut ses2 = client.session(SERVICE);

    let params = vec!["hello2", "world2", "again"];
    let params2 = vec!["whatever", "floats", "boats"];

    ses.connect()?; // optional
    ses2.connect()?;

    // Request -> Receive example
    let mut req = ses.request(METHOD, params)?;
    let mut req2 = ses2.request(METHOD, params2)?;

    while let Some(resp) = req2.recv(10)? {
        println!("Response: {}", resp.dump());
    }

    while let Some(resp) = req.recv(10)? {
        println!("Response: {}", resp.dump());
    }

    // Iterator example
    let params = vec!["hello2", "world2", "again"];
    for resp in ses.sendrecv(METHOD, params)? {
        println!("Response: {}", resp.dump());
    }

    ses2.disconnect()?; // only required if ses.connect() was called
    ses.disconnect()?; // only required if ses.connect() was called

    // Iterator example of a one-off request for a service
    let params = vec!["hello2", "world2", "again"];
    for resp in client.sendrecv(SERVICE, METHOD, params)? {
        println!("Response: {}", resp.dump());
    }

    let params: Vec<json::JsonValue> = vec![];
    for resp in client.sendrecv(SERVICE, "opensrf.rspublic.time", params)? {
        println!("TIME IS: {}", resp.dump());
    }

    Ok(())
}
