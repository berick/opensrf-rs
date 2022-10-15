use opensrf::Client;
use opensrf::Config;

const SERVICE: &str = "opensrf.settings";
const METHOD: &str = "opensrf.system.echo";

fn main() -> Result<(), String> {
    let mut conf = Config::from_file("conf/opensrf_client.yml")?;
    conf.set_primary_connection("service", "private.localhost")?;

    let mut client = Client::new(conf)?;

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

    Ok(())
}
