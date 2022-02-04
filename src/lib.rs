pub mod error;
pub mod util;
pub mod conf;
pub mod classified;
pub mod message;
mod bus;
mod session;
pub mod client;

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


