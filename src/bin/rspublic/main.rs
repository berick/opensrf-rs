use std::env;
use std::sync::Arc;
use std::any::Any;
use opensrf::client::ClientHandle;
use opensrf::conf;
use opensrf::message;
use opensrf::method;
use opensrf::method::ParamCount;
use opensrf::server;
use opensrf::session::ServerSession;
use opensrf::app::{ApplicationEnv, Application, ApplicationWorker, ApplicationWorkerFactory};

const APPNAME: &str = "opensrf.rspublic";

#[derive(Debug, Clone)]
struct RsPublicEnv;

impl RsPublicEnv {
    pub fn new() -> Self {
        RsPublicEnv { }
    }
}

impl ApplicationEnv for RsPublicEnv {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct RsPublicApplication;

impl RsPublicApplication {
    pub fn new() -> Self {
        RsPublicApplication {
        }
    }
}

impl Application for RsPublicApplication {

    fn name(&self) -> &str {
        APPNAME
    }

    fn env(&self) -> Box<dyn ApplicationEnv> {
        Box::new(RsPublicEnv::new())
    }

    fn register_methods(
        &self,
        _client: ClientHandle,
        _config: Arc<conf::Config>,
    ) -> Result<Vec<method::Method>, String> {

        Ok(vec![
            method::Method::new("opensrf.rspublic.time", ParamCount::Zero, relay),
            method::Method::new("opensrf.rspublic.echo", ParamCount::Any, relay),
            method::Method::new("opensrf.rspublic.counter", ParamCount::Zero, relay),
            method::Method::new("opensrf.rspublic.sleep", ParamCount::Range(0, 1), relay),
        ])
    }

    fn worker_factory(&self) -> ApplicationWorkerFactory {
        || { Box::new(RsPublicWorker::new()) }
    }
}

struct RsPublicWorker {
    env: Option<RsPublicEnv>,
    client: Option<ClientHandle>,
    config: Option<Arc<conf::Config>>,
    relay_count: usize,
}

impl RsPublicWorker {
    pub fn new() -> Self {
        RsPublicWorker {
            env: None,
            client: None,
            config: None,
            // A value that increases with each call relayed.
            relay_count: 0,
        }
    }

    /// We must have a value here since absorb_env() is invoked on the worker.
    pub fn env(&self) -> &RsPublicEnv {
        self.env.as_ref().unwrap()
    }

    pub fn downcast(w: &mut Box<dyn ApplicationWorker>) -> Result<&mut RsPublicWorker, String> {
        match w.as_any_mut().downcast_mut::<RsPublicWorker>() {
            Some(eref) => Ok(eref),
            None => Err(format!("Cannot downcast")),
        }
    }

    ///
    /// self.client is guaranteed to set after absorb_env()
    fn client(&self) -> &ClientHandle {
        self.client.as_ref().unwrap()
    }

    fn client_mut(&mut self) -> &mut ClientHandle {
        self.client.as_mut().unwrap()
    }
}

impl ApplicationWorker for RsPublicWorker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Panics if we cannot downcast the env provided to the expected type.
    fn absorb_env(
        &mut self,
        client: ClientHandle,
        config: Arc<conf::Config>,
        env: Box<dyn ApplicationEnv>
    ) -> Result<(), String> {

        self.client = Some(client);
        self.config = Some(config);

        match env.as_any().downcast_ref::<RsPublicEnv>() {
            Some(eref) => { self.env = Some(eref.clone()) }
            None => panic!("Unexpected environment type in absorb_env()")
        }
        Ok(())
    }

    fn thread_start(&mut self) -> Result<(), String> {
        log::debug!("Thread starting");
        Ok(())
    }

    fn thread_end(&mut self) -> Result<(), String> {
        log::debug!("Thread ending");
        Ok(())
    }
}


fn main() {
    let _args: Vec<String> = env::args().collect(); // TODO config file

    let app = RsPublicApplication::new();

    let mut server = server::Server::new(
        "private.localhost", // TODO command line
        conf::Config::from_file("conf/opensrf.yml").unwrap(),
        Box::new(app)
    );

    server.listen().unwrap();
}

fn relay(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::Method
) -> Result<(), String> {

    let mut worker = RsPublicWorker::downcast(worker)?;
    worker.relay_count += 1;
    let api_name = method.method().replace("rspublic", "rsprivate");

    for resp in worker.client_mut().sendrecv(
        "opensrf.rsprivate", &api_name, method.params().clone())? {

        session.respond(resp.clone())?;
        session.respond(json::from(format!("Relay count: {}", worker.relay_count)))?
    }

    Ok(())
}

