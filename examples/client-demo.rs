use opensrf::{Client, Config};

const SERVICE: &str = "opensrf.rspublic";
const METHOD: &str = "opensrf.rspublic.echo";

fn main() -> Result<(), String> {
    let mut conf = Config::from_file("conf/opensrf.yml")?;
    conf.set_primary_connection("service", "private.localhost")?;

    let mut client = Client::new(conf.to_shared())?;

    // ---------------------------------------------------------
    // SESSION + MANUAL REQUEST --------------------------------

    let mut ses = client.session(SERVICE);

    ses.connect()?; // Optional

    let params = vec!["Hello", "World", "Pamplemousse"];

    let mut req = ses.request(METHOD, params)?;

    // Loop will continue until the request is complete or a recv()
    // call times out.
    while let Some(resp) = req.recv(60)? {
        println!("Response: {}", resp.dump());
    }

    ses.disconnect()?; // Only required if connected

    // ---------------------------------------------------------
    // SESSION REQUEST WITH ITERATOR ---------------------------

    let mut ses = client.session(SERVICE);

    // Requests consume our params vec, so we need a new (or cloned)
    // one for each request.
    let params = vec!["Hello", "World", "Pamplemousse"];

    for resp in ses.sendrecv(METHOD, params)? {
        println!("Response: {}", resp.dump());
    }

    // --------------------------------------------------------
    // ONE-OFF REQUEST WITH ITERATOR --------------------------

    let params = vec!["Hello", "World", "Pamplemousse"];

    for resp in client.sendrecv(SERVICE, METHOD, params)? {
        println!("Response: {}", resp.dump());
    }

    for _ in 0..10 {
        let params: Vec<u8> = vec![];
        for resp in client.sendrecv(SERVICE, "opensrf.rspublic.counter", params)? {
            println!("Counter is {}", resp.dump());
        }
    }

    Ok(())
}
