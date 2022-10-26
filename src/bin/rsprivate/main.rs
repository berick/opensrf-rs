use opensrf::client::ClientHandle;
use opensrf::conf;
use opensrf::message;
use opensrf::method;
use opensrf::server::Server;
use opensrf::session::ServerSession;
use std::env;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const METHODS: &'static [method::Method] = &[
    method::Method {
        api_spec: "opensrf.rsprivate.echo",
        param_count: method::ParamCount::Any,
        handler: echo,
    },
    method::Method {
        api_spec: "opensrf.rsprivate.time",
        param_count: method::ParamCount::Any,
        handler: time,
    },
    method::Method {
        api_spec: "opensrf.rsprivate.sleep",
        param_count: method::ParamCount::Exactly(1),
        handler: sleep,
    },
];

fn main() {
    let _args: Vec<String> = env::args().collect(); // TODO config file

    let mut server = Server::new(
        "private.localhost",
        "opensrf.rsprivate",
        conf::Config::from_file("conf/opensrf.yml").unwrap(),
        METHODS,
    );

    server.listen();
}

fn echo(
    _client: ClientHandle,
    ses: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    for p in method.params() {
        ses.respond(p.clone())?;
    }

    Ok(())
}

fn time(
    _client: ClientHandle,
    ses: &mut ServerSession,
    _method: &message::Method,
) -> Result<(), String> {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    ses.respond(json::from(dur.as_secs()))?;
    Ok(())
}

fn sleep(
    _client: ClientHandle,
    _ses: &mut ServerSession,
    method: &message::Method,
) -> Result<(), String> {
    // We known params contains at least one values because of Server
    // param count checks.
    let secs = method.params()[0].as_u8().unwrap_or(1);

    log::debug!("sleep() waiting for {} seconds", secs);

    thread::sleep(Duration::from_secs(secs as u64));

    Ok(())
}
