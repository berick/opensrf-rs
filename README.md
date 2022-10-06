# opensrf-rs
OpenSRF Rust Bindings

## Synopsis

```rs
use opensrf::Client;
use opensrf::ClientConfig;

fn main() -> Result<(), String> {

    let conf = ClientConfig::from_file("conf/opensrf_client.yml")?;
    let mut client = Client::new(conf)?;

    let method = "opensrf.system.echo";
    let service = "opensrf.settings";

    // EXAMPLE SESSION + MANUAL REQUEST ------------------------

    let mut ses = client.session(service);

    let params = vec!["Hello", "World", "Pamplemousse"];

    let mut req = ses.request(method, params)?;

    // Loop will continue until the request is complete or a recv()
    // call times out.
    while let Some(resp) = req.recv(60)? {
        println!("Response: {}", resp.dump());
    }

    // EXAMPLE SESSION REQUEST WITH ITERATOR ---------------

    let mut ses = client.session(service);

    // Requests consume our params vec, so we need a new one
    // for each request.
    let params = vec!["Hello", "World", "Pamplemousse"];

    for resp in ses.sendrecv(method, params)? {
        println!("Response: {}", resp.dump());
    }

    // EXAMPLE ONE-OFF REQUEST WITH ITERATOR --------------------

    let params = vec!["Hello", "World", "Pamplemousse"];

    for resp in client.sendrecv(service, method, params)? {
        println!("Response: {}", resp.dump());
    }

    Ok(())
}
```

## Example

```sh
RUST_LOG=trace cargo run --example client
```
