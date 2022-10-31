/*
use opensrf::client::ClientHandle;
use opensrf::conf;
use opensrf::message;
use opensrf::method;
use opensrf::server::Server;
use opensrf::session::ServerSession;
use std::env;

const METHODS: &'static [method::Method] = &[
    method::Method {
        api_spec: "opensrf.rspublic.echo",
        param_count: method::ParamCount::Any,
        handler: relay_request,
    },
    method::Method {
        api_spec: "opensrf.rspublic.time",
        param_count: method::ParamCount::Any,
        handler: relay_request,
    },
    method::Method {
        api_spec: "opensrf.rspublic.sleep",
        param_count: method::ParamCount::Range(0, 1),
        handler: relay_request,
    },
];

fn main() {
    let _args: Vec<String> = env::args().collect(); // TODO config file

    let mut server = Server::new(
        "private.localhost", // TODO
        "opensrf.rspublic",
        conf::Config::from_file("conf/opensrf.yml").unwrap(),
        METHODS,
    );

    server.listen().unwrap();
}

fn relay_request(
    mut client: ClientHandle,
    ses: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {

    let api_name = method.method().replace("rspublic", "rsprivate");

    // We're borrowing method.  Clone params so they can be
    // handed over to sendrecv().
    let params = method.params().clone();

    for resp in client.sendrecv("opensrf.rsprivate", &api_name, params)? {
        ses.respond(resp)?;
    }

    Ok(())
}
*/

fn main() {}
