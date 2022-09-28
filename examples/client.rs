use opensrf::client2::Client;
use opensrf::conf::ClientConfig;

fn main() {
    let mut conf = ClientConfig::new();

    conf.load_file("conf/opensrf_client.yml").unwrap();

    let mut client = Client::new(conf).unwrap();

    let mut ses = client.session("opensrf.settings");

    let mut req = ses.request("opensrf.system.echo", vec!["hello", "world"]).unwrap();

    while let Some(resp) = req.recv(10).unwrap() {
        println!("Response: {resp:?}");
    }

    /*
    let mut client = Client::new(conf).expect("Could not build client");

    let ses = client.session("opensrf.settings");

    client.connect(&ses).expect("Could not connect to service");

    let params = vec!["Hello", "World"];
    let params2 = params.clone();

    let req = client
        .request(&ses, "opensrf.system.echo", params)
        .expect("Error creating request");

    let req2 = client
        .request(&ses, "opensrf.system.echo", params2)
        .expect("Error creating request");

    // Receive them out of order for testing purposes.

    while !client.complete(&req2) {
        match client.recv(&req2, 10).unwrap() {
            Some(value) => println!("REQ2 GOT RESPONSE: {}", value.dump()),
            None => {
                println!("Request complete OR timed out");
                break;
            }
        }
    }

    println!("Request 2 is complete");

    // A leaner recv() approach that assumes receiving a None, which can
    // happen with a timeout or a completed request, suffices to continue.

    while let Some(value) = client.recv(&req, 10).expect("recv() Failed") {
        println!("Request 1 returned: {}", value.dump());
    }

    client.disconnect(&ses).unwrap();
    client.cleanup(&ses);

    let ses = client.session("open-ils.storage");

    client.connect(&ses).unwrap();

    let params = vec![1];
    let req = client
        .request(&ses, "open-ils.storage.direct.actor.user.retrieve", params)
        .unwrap();

    while !client.complete(&req) {
        match client.recv(&req, 10).unwrap() {
            Some(value) => {
                println!("REQ2 GOT RESPONSE: {}", value.dump());
                //let jwc = JsonWithClass::decode(&value).unwrap();
                //println!("class = {} value = {}", jwc.class(), jwc.json().dump());
            }
            None => {
                println!("req returned None");
                break;
            }
        }
    }

    client.disconnect(&ses).unwrap();
    client.cleanup(&ses);
    client.disconnect_bus().expect("bus disconnected");
    */
}
