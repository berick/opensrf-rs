use opensrf::client::Client;
use opensrf::conf::ClientConfig;

fn main() -> Result<(), String> {
    let mut conf = ClientConfig::new();

    conf.load_file("conf/opensrf_client.yml")?;

    let mut client = Client::new(conf)?;

    let mut ses = client.session("opensrf.settings");

    let method = "opensrf.system.echo";
    let params = vec!["hello2", "world2", "again"];

    ses.connect()?; // optional

    // Request -> Receive example
    let mut req = ses.request(method, params)?;

    while let Some(resp) = req.recv(10)? {
        println!("Response: {}", resp.dump());
    }

    // Iterator example
    let params = vec!["hello2", "world2", "again"];
    for resp in ses.sendrecv(method, params)? {
        println!("Response: {}", resp.dump());
    }

    ses.disconnect()?; // only required if ses.connect() was called

    Ok(())
}

