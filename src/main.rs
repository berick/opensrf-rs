use opensrf::conf::ClientConfig;
use opensrf::message::MessageType;
use opensrf::message::MessageStatus;
use opensrf::message::TransportMessage;
use opensrf::message::Payload;
use opensrf::message::Method;
use opensrf::message::Message;
use opensrf::client::Client;
use opensrf::client::ClientSession;
use opensrf::client::ClientRequest;
use opensrf::websocket::WebsocketClient;

use redis;
use redis::Commands;
use std::{thread, time};

fn main() {
    let mut conf = ClientConfig::new();
    conf.load_file("conf/opensrf_client.yml");

    let mut wsclient = WebsocketClient::new("ws://localhost:7682/");
    let req = Message::new(MessageType::Request, 1,
        Payload::Method(Method::new("opensrf.system.echo", vec![json::from("HOWDY!")])));

    wsclient.send(req).unwrap();

    if let Some(value) = wsclient.recv().unwrap() {
        println!("WE GOT A {}", value);

    }


    /*

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
    */
}


