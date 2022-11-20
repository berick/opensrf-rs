use getopts;
use std::env;

pub use client::Client;
pub use conf::Config;
pub use logging::Logger;
pub use sclient::SettingsClient;
pub use session::SessionHandle;

pub mod addr;
pub mod app;
pub mod bus;
pub mod classified;
pub mod client;
pub mod conf;
pub mod logging;
pub mod message;
pub mod method;
pub mod params;
pub mod sclient;
pub mod server;
pub mod session;
pub mod util;
pub mod worker;

#[cfg(test)]
mod tests;

pub struct InitOptions {
    pub skip_logging: bool,
}

impl InitOptions {
    pub fn new() -> InitOptions {
        InitOptions {
            skip_logging: false,
        }
    }
}

const DEFAULT_OSRF_CONFIG: &str = "/openils/conf/opensrf_core.xml";

/// Read common command line parameters, parse the core config, apply
/// the primary connection type, and setup logging.
///
/// This does not connect to the bus.
pub fn init() -> Result<conf::Config, String> {
    let (config, _) = init_with_options(&mut getopts::Options::new())?;
    Ok(config)
}

pub fn init_with_options(
    opts: &mut getopts::Options,
) -> Result<(conf::Config, getopts::Matches), String> {
    init_with_more_options(opts, InitOptions::new())
}

/// Same as init(), but allows the caller to pass in a prepopulated set
/// of getopts::Options, which are then augmented with the standard
/// OpenSRF command line options.
pub fn init_with_more_options(
    opts: &mut getopts::Options,
    options: InitOptions,
) -> Result<(conf::Config, getopts::Matches), String> {
    let args: Vec<String> = env::args().collect();

    opts.optflag("l", "localhost", "Use Localhost");
    opts.optopt("h", "hostname", "hostname", "hostname");
    opts.optopt("c", "osrf-config", "OpenSRF Config", "OSRF_CONFIG");

    let params = match opts.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => {
            return Err(format!("Error parsing options: {e}"));
        }
    };

    let filename = match params.opt_get_default(
        "osrf-config", DEFAULT_OSRF_CONFIG.to_string()) {
        Ok(f) => f,
        Err(e) => {
            return Err(format!("Error reading osrf-config option: {e}"));
        }
    };

    let mut config = conf::ConfigBuilder::from_file(&filename)?.build()?;

    if params.opt_present("localhost") {
        config.set_hostname("localhost");
    }

    if !options.skip_logging {
        if let Err(e) = Logger::new(config.client().logging())?.init() {
            return Err(format!("Error initializing logger: {e}"));
        }
    }

    Ok((config, params))
}
