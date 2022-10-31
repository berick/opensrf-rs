use opensrf::app;
use opensrf::client;
use opensrf::conf;
use opensrf::message;
use opensrf::method;
use opensrf::server;
use opensrf::session;
use std::env;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::any::Any;

#[derive(Debug, Clone)]
struct RsprivateEnv {
    some_global_thing: Arc<String>,
}

impl RsprivateEnv {
    pub fn new(something: Arc<String>) -> Self {
        RsprivateEnv {
            some_global_thing: something,
        }
    }
}

impl app::ApplicationEnv for RsprivateEnv {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct RsprivateApplication {
}

impl RsprivateApplication {
    pub fn new() -> Self {
        RsprivateApplication {
        }
    }
}

impl app::Application for RsprivateApplication {

    fn name(&self) -> &str {
        "opensrf.rsprivate"
    }

    fn env(&self) -> Box<dyn app::ApplicationEnv> {
        Box::new(RsprivateEnv::new(Arc::new(String::from("FOO"))))
    }

    fn register_methods(
        &self,
        client: client::ClientHandle,
        config: Arc<conf::Config>,
    ) -> Result<Vec<method::Method>, String> {
        Ok(vec![
            method::Method::new("opensrf.rsprivate.time", method::ParamCount::Zero, time),
            method::Method::new("opensrf.rsprivate.echo", method::ParamCount::Any, echo),
        ])
    }

    fn worker_factory(&self) -> app::ApplicationWorkerFactory {
        || { Box::new(RsprivateWorker::new()) }
    }
}

struct RsprivateWorker {
    env: Option<RsprivateEnv>,
    client: Option<client::ClientHandle>,
}

impl RsprivateWorker {
    pub fn new() -> Self {
        RsprivateWorker {
            env: None,
            client: None
        }
    }

    /// We must have a value here since absorb_env() is invoked on the worker.
    pub fn env(&self) -> &RsprivateEnv {
        self.env.as_ref().unwrap()
    }

    pub fn downcast(w: &mut Box<dyn app::ApplicationWorker>) -> Result<&mut RsprivateWorker, String> {
        match w.as_any_mut().downcast_mut::<RsprivateWorker>() {
            Some(eref) => Ok(eref),
            None => Err(format!("Cannot downcast")),
        }
    }
}

impl app::ApplicationWorker for RsprivateWorker {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Panics if we cannot downcast the env provided to the expected type.
    fn absorb_env(
        &mut self,
        client: client::ClientHandle,
        config: Arc<conf::Config>,
        env: Box<dyn app::ApplicationEnv>
    ) -> Result<(), String> {

        match env.as_any().downcast_ref::<RsprivateEnv>() {
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

    let app = RsprivateApplication::new();

    let mut server = server::Server::new(
        "private.localhost", // TODO command line
        conf::Config::from_file("conf/opensrf.yml").unwrap(),
        Box::new(app)
    );

    server.listen().unwrap();
}

fn echo(w: &mut Box<dyn app::ApplicationWorker>, ses: &mut session::ServerSession, method: &message::Method) -> Result<(), String> {
    let worker = RsprivateWorker::downcast(w)?;
    log::info!("We have a proper worker with {}", worker.env().some_global_thing);
    for p in method.params() {
        ses.respond(p.clone())?;
    }
    Ok(())
}

fn time(w: &mut Box<dyn app::ApplicationWorker>, ses: &mut session::ServerSession, method: &message::Method) -> Result<(), String> {
    let worker = RsprivateWorker::downcast(w);
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    ses.respond(json::from(dur.as_secs()))?;
    Ok(())
}

/*

fn sleep(_: ClientHandle, _: &mut ServerSession, method: &message::Method) -> Result<(), String> {

    // Param count may be zero
    let secs = match method.params().len() {
        x if x > 0 => method.params()[0].as_u8().unwrap_or(1),
        _ => 1,
    };

    log::debug!("sleep() waiting for {} seconds", secs);

    thread::sleep(Duration::from_secs(secs as u64));

    Ok(())
}
*/

