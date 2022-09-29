use opensrf::client::Client;
use opensrf::conf::ClientConfig;

fn main() {
    let mut conf = ClientConfig::new();

    conf.load_file("conf/opensrf_client.yml").unwrap();

    let mut client = Client::new(conf).unwrap();

    let mut ses = client.session("opensrf.settings");

    ses.connect();

    let mut req = ses.request("opensrf.system.echo", vec!["hello", "world"]).unwrap();

    while let Some(resp) = req.recv(10).unwrap() {
        println!("Response: {}", resp.dump());
    }

    ses.disconnect();
}

