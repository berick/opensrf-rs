use std::sync::Arc;
use std::any::Any;
use super::client;
use super::conf;
use super::method;

// ApplicationWorkers may not be thread Send-able, but a ref to a
// function that generates ApplicationWorkers is.
pub type ApplicationWorkerFactory = fn() -> Box<dyn ApplicationWorker>;

pub trait ApplicationEnv: Any + Sync + Send {
    fn as_any(&self) -> &dyn Any;
}

pub trait ApplicationWorker: Any {
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Client created/connected by the worker thread at thread start
    /// The thread doesn't need it, so it passes ownership to the worker.
    fn absorb_env(
        &mut self,
        client: client::ClientHandle,
        config: Arc<conf::Config>,
        env: Box<dyn ApplicationEnv>
    ) -> Result<(), String>;

    fn thread_start(&mut self) -> Result<(), String>;
    fn thread_end(&mut self) -> Result<(), String>;
}

pub trait Application {

    /// Application service name, e.g. opensrf.settings
    fn name(&self) -> &str;

    fn register_methods(
        &self,
        // Client owned by the server
        client: client::ClientHandle,
        config: Arc<conf::Config>,
    ) -> Result<Vec<method::Method>, String>;

    fn worker_factory(&self) -> ApplicationWorkerFactory;
    fn env(&self) -> Box<dyn ApplicationEnv>;
}


