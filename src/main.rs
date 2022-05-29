use opensrf::conf::ClientConfig;
use opensrf::client::Client;
//use opensrf::websocket::WebsocketClient;

fn main() {
    let mut conf = ClientConfig::new();

    conf.load_file("conf/opensrf_client.yml").expect("Error parsing client config");

    /*
    let mut wsclient = WebsocketClient::new("ws://localhost:7682/");
    let mut ses = wsclient.session("open-ils.actor");

    let req = wsclient.request(&ses, "opensrf.system.echo", vec!["HOWDY!", "Neighbor"]);

    if let Some(value) = wsclient.recv(&ses).unwrap() {
        println!("WE GOT A {}", value);

    }
    */


    let mut client = Client::new(conf.bus_config()).unwrap();

    let ses = client.session("opensrf.settings");

    client.connect(&ses).unwrap();

    let params = vec![json::from("Hello"), json::from("World")];
    let req = client.request(&ses, "opensrf.system.echo", params).unwrap();

    let params2 = vec![json::from("Hello"), json::from("World")];
    let req2 = client.request(&ses, "opensrf.system.echo", params2).unwrap();

    while !client.complete(&req2) {
        match client.recv(&req2, 10).unwrap() {
            Some(value) => println!("REQ2 GOT RESPONSE: {}", value.dump()),
            None => {
                println!("req2 returned None");
                break;
            }
        }
    }

    while !client.complete(&req) {
        match client.recv(&req, 10).unwrap() {
            Some(value) => println!("REQ1 GOT RESPONSE: {}", value.dump()),
            None => {
                println!("req1 returned None");
                break;
            }
        }
    }

    client.disconnect(&ses).unwrap();
    client.cleanup(&ses);

    let ses = client.session("open-ils.cstore");

    client.connect(&ses).unwrap();

    let params = vec![json::from(1)];
    let req = client.request(&ses, "open-ils.cstore.direct.actor.user.retrieve", params).unwrap();

    while !client.complete(&req) {
        match client.recv(&req, 10).unwrap() {
            Some(value) => {
                println!("REQ2 GOT RESPONSE: {}", value.dump());
                //let jwc = JsonWithClass::decode(&value).unwrap();
                //println!("class = {} value = {}", jwc.class(), jwc.json().dump());
            },
            None => {
                println!("req returned None");
                break;
            }
        }
    }

    client.disconnect(&ses).unwrap();
    client.cleanup(&ses);
}


