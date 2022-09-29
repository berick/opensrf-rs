# opensrf-rs
OpenSRF Rust Bindings

## Synopsis

```rs
use opensrf::client::Client;
use opensrf::conf::ClientConfig;

fn main() -> Result<(), String> {

    let conf = ClientConfig::from_file("conf/opensrf_client.yml")?;

    let mut client = Client::new(conf)?;

    let mut ses = client.session("opensrf.settings");

    let method = "opensrf.system.echo";
    let params = vec!["Hello", "World", "Pamplemousse"];

    for resp in ses.sendrecv(method, params)? {
        println!("Response: {}", resp.dump());
    }

    Ok(())
}
```

## Example

```sh
RUST_LOG=trace cargo run --example client
```
