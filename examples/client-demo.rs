use opensrf::{Client, Config};

const SERVICE: &str = "opensrf.rsprivate";
const METHOD: &str = "opensrf.rsprivate.echo";

fn main() -> Result<(), String> {

    let mut conf = Config::from_file("conf/opensrf.yml")?;
    conf.set_primary_connection("service", "private.localhost")?;

    let mut client = Client::new(conf.to_shared())?;

    // ---------------------------------------------------------
    // EXAMPLE SESSION + MANUAL REQUEST ------------------------

    let mut ses = client.session(SERVICE);

    let params = vec!["Hello", "World", "Pamplemousse"];

    let mut req = ses.request(METHOD, params)?;

    // Loop will continue until the request is complete or a recv()
    // call times out.
    while let Some(resp) = req.recv(60)? {
        println!("Response: {}", resp.dump());
    }

    // ---------------------------------------------------------
    // EXAMPLE SESSION REQUEST WITH ITERATOR -------------------

    let mut ses = client.session(SERVICE);

    // Requests consume our params vec, so we need a new one
    // for each request.
    let params = vec!["Hello", "World", "Pamplemousse"];

    for resp in ses.sendrecv(METHOD, params)? {
        println!("Response: {}", resp.dump());
    }

    // --------------------------------------------------------
    // EXAMPLE ONE-OFF REQUEST WITH ITERATOR ------------------

    let params = vec!["Hello", "World", "Pamplemousse"];

    for resp in client.sendrecv(SERVICE, METHOD, params)? {
        println!("Response: {}", resp.dump());
    }

    Ok(())
}
