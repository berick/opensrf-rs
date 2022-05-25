mod bus;
pub mod classified;
pub mod client;
pub mod conf;
pub mod error;
pub mod message;
mod session;
pub mod util;
pub mod websocket;

#[cfg(test)]
mod tests;

/*
mod method;
mod worker;
mod server;
*/

/*

/// Initialize logging and read the configuration file.
pub fn init(args: Vec<String>) -> Result<Config, self::error::Error> {

    env_logger::init();

    if args.len() < 2 {
        return Err(Error::NoConfigFileError);
    }

    let mut config = self::Config::new();
    config.load_file(&args[1])?;

    Ok(config)
}
*/
