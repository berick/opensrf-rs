use opensrf::Client;
use opensrf::ClientConfig;

const SERVICE: &str = "opensrf.settings";
const METHOD: &str = "opensrf.system.echo";

fn main() -> Result<(), String> {
    let conf = ClientConfig::from_file("conf/opensrf_client.yml")?;

    let mut client = Client::new(conf)?;

    let mut ses = client.session(SERVICE);

    let params = vec!["hello2", "world2", "again"];

    ses.connect()?; // optional

    // Request -> Receive example
    let mut req = ses.request(METHOD, params)?;

    while let Some(resp) = req.recv(10)? {
        println!("Response: {}", resp.dump());
    }

    // Iterator example
    let params = vec!["hello2", "world2", "again"];
    for resp in ses.sendrecv(METHOD, params)? {
        println!("Response: {}", resp.dump());
    }

    ses.disconnect()?; // only required if ses.connect() was called

    // Iterator example of a one-off request for a service
    let params = vec!["hello2", "world2", "again"];
    for resp in client.sendrecv(SERVICE, METHOD, params)? {
        println!("Response: {}", resp.dump());
    }

    Ok(())
}
