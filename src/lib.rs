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

const DEFAULT_OSRF_CONFIG: &str = "/openils/conf/opensrf_core.yml";

/// Read common command line parameters, parse the core config, apply
/// the primary connection type, and setup logging.
///
/// This does not connect to the bus.
pub fn init(connection_type: &str) -> Result<conf::Config, String> {
    let (config, _) = init_with_options(connection_type, &mut getopts::Options::new())?;
    Ok(config)
}

/// Same as init(), but allows the caller to pass in a prepopulated set
/// of getopts::Options, which are then augmented with the standard
/// OpenSRF command line options.
pub fn init_with_options(
    connection_type: &str,
    opts: &mut getopts::Options,
) -> Result<(conf::Config, getopts::Matches), String> {
    let args: Vec<String> = env::args().collect();

    opts.optflag("l", "localhost", "Use Localhost");
    opts.optopt("d", "domain", "domain", "domain");
    opts.optopt("c", "osrf-config", "OpenSRF Config", "OSRF_CONFIG");

    let params = match opts.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => {
            return Err(format!("Error parsing options: {e}"));
        }
    };

    let filename = match params.opt_get_default("osrf-config", DEFAULT_OSRF_CONFIG.to_string()) {
        Ok(f) => f,
        Err(e) => {
            return Err(format!("Error reading osrf-config option: {e}"));
        }
    };

    let mut config = conf::Config::from_file(&filename)?;

    let domain = match params.opt_str("domain") {
        Some(d) => d,
        _ => match params.opt_present("localhost") {
            true => "localhost".to_string(),
            _ => {
                return Err(format!("Router requires --localhost or --domain <domain>"));
            }
        },
    };

    if params.opt_present("localhost") {
        config.set_hostname("localhost");
    }

    config.set_primary_connection(connection_type, &domain)?;

    // At this point, we know connection_type is valid.
    let contype = config.get_connection_type(connection_type).unwrap();

    if let Err(e) = Logger::new(contype.logging())?.init() {
        return Err(format!("Error initializing logger: {e}"));
    }

    Ok((config, params))
}
