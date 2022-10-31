use opensrf::Client;
use opensrf::Config;
use opensrf::Logger;

fn main() -> Result<(), String> {
    let mut conf = Config::from_file("conf/opensrf.yml")?;
    let connection = conf.set_primary_connection("service", "public.localhost")?;

    let ctype = connection.connection_type();
    Logger::new("client", ctype.log_level(), ctype.log_facility())
        .init()
        .unwrap();

    let mut client = Client::new(conf.to_shared())?;

    // Sending a request to a public service should be OK with our
    // public client.
    let mut ses = client.session("opensrf.rspublic");
    let mut req = ses.request("opensrf.system.echo", vec!["Hello", "Goodbye"])?;

    while let Some(resp) = req.recv(5)? {
        println!("Routed message arrived: {}", resp.dump());
    }

    if let Some(jv) = client.send_router_command("public.localhost", "summarize", None, true)? {
        println!("Router command returned: {}", jv.dump());
    }

    // Sending a request to a private service via our public client
    // should fail.

    let mut ses = client.session("opensrf.rsprivate");
    let mut req = ses.request("opensrf.system.echo", vec!["Hello", "Goodbye"])?;

    if let Err(e) = req.recv(5) {
        println!("This SHOULD fail: {e}");
    }

    Ok(())
}
