use super::error::Error;
use std::fs;
use yaml_rust::yaml;
use yaml_rust::YamlLoader;

#[derive(Debug, Clone)]
pub struct BusConfig {
    host: Option<String>,
    port: Option<u16>,

    /// Unix Socket path
    sock: Option<String>,
}

impl BusConfig {
    pub fn new() -> Self {
        BusConfig {
            host: None,
            port: None,
            sock: None,
        }
    }

    pub fn host(&self) -> &Option<String> {
        &self.host
    }

    pub fn port(&self) -> &Option<u16> {
        &self.port
    }

    pub fn sock(&self) -> &Option<String> {
        &self.sock
    }

    pub fn set_host(&mut self, host: &str) {
        self.host = Some(String::from(host));
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = Some(port);
    }

    pub fn set_sock(&mut self, sock: &str) {
        self.sock = Some(String::from(sock));
    }
}

/*
#[derive(Debug, Clone)]
enum LogFile {
    Syslog,
    File(String),
}

#[derive(Debug, Clone)]
enum LogLevel {
    Error = 1,
    Warning = 2,
    Info = 3,
    Debug = 4,
    Internal = 5,
}
*/

#[derive(Debug)]
pub struct ClientConfig {
    bus_config: BusConfig,
    /*
    log_file: LogFile,
    log_level: LogLevel,
    syslog_facility: Option<String>,
    actlog_facility: Option<String>,
    */
    settings_file: Option<String>,
}

impl ClientConfig {
    pub fn new() -> Self {
        ClientConfig {
            bus_config: BusConfig::new(),
            /*
            log_file: LogFile::Syslog,
            log_level: LogLevel::Info,
            syslog_facility: None,
            actlog_facility: None,
            */
            settings_file: None,
        }
    }

    pub fn bus_config(&self) -> &BusConfig {
        &self.bus_config
    }

    /// Load configuration from an XML file
    pub fn load_file(&mut self, config_file: &str) -> Result<(), Error> {
        let yaml_text = match fs::read_to_string(config_file) {
            Ok(t) => t,
            Err(e) => {
                eprintln!(
                    "Error reading configuration file: file='{}' {:?}",
                    config_file, e
                );
                return Err(Error::ClientConfigError);
            }
        };

        self.load_string(&yaml_text)
    }

    /// Load configuration from an XML string
    pub fn load_string(&mut self, yaml_text: &str) -> Result<(), Error> {
        let yaml_docs = match YamlLoader::load_from_str(yaml_text) {
            Ok(docs) => docs,
            Err(e) => {
                eprintln!("Error parsing configuration file: {:?}", e);
                return Err(Error::ClientConfigError);
            }
        };

        let root = &yaml_docs[0];

        self.set_logging_config(root)?;
        self.set_bus_config(root)?;

        Ok(())
    }

    fn set_logging_config(&mut self, yaml: &yaml::Yaml) -> Result<(), Error> {
        if let Some(filename) = yaml["logging"]["log4rs_config"].as_str() {
            if let Err(err) = log4rs::init_file(filename, Default::default()) {
                eprintln!("Error loading log4rs config: {}", err);
                return Err(Error::ClientConfigError);
            }
        } else {
            eprintln!("No log4rs configuration file set");
            return Err(Error::ClientConfigError);
        };

        Ok(())
    }

    fn set_bus_config(&mut self, yaml: &yaml::Yaml) -> Result<(), Error> {
        if let Some(p) = yaml["message_bus"]["port"].as_i64() {
            self.bus_config.set_port(p as u16);
        };

        if let Some(h) = yaml["message_bus"]["host"].as_str() {
            self.bus_config.set_host(h);
        };

        if let Some(s) = yaml["message_bus"]["sock"].as_str() {
            self.bus_config.set_sock(s);
        };

        Ok(())
    }
}
