use super::error::Error;
use std::fs;
use yaml_rust::yaml;
use yaml_rust::YamlLoader;

#[derive(Debug, Clone)]
pub struct BusConfig {
    domain: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
}

impl BusConfig {
    pub fn new() -> Self {
        BusConfig {
            domain: None,
            port: None,
            username: None,
            password: None,
        }
    }

    pub fn domain(&self) -> &Option<String> {
        &self.domain
    }

    pub fn port(&self) -> &Option<u16> {
        &self.port
    }

    pub fn username(&self) -> &Option<String> {
        &self.username
    }

    pub fn password(&self) -> &Option<String> {
        &self.password
    }

    pub fn set_domain(&mut self, domain: &str) {
        self.domain = Some(String::from(domain));
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = Some(port);
    }

    pub fn set_username(&mut self, username: &str) {
        self.username = Some(String::from(username));
    }

    pub fn set_password(&mut self, password: &str) {
        self.password = Some(String::from(password));
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
    settings_file: Option<String>,
    */
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
            settings_file: None,
            */
        }
    }

    pub fn bus_config(&self) -> &BusConfig {
        &self.bus_config
    }

    /// Load configuration from a YAML file
    pub fn load_file(&mut self, config_file: &str) -> Result<(), Error> {
        let yaml_text = match fs::read_to_string(config_file) {
            Ok(t) => t,
            Err(e) => {
                return Err(Error::ClientConfigError(format!(
                    "Error reading configuration file: file='{}' {:?}",
                    config_file, e
                )));
            }
        };

        self.load_string(&yaml_text)
    }

    /// Load configuration from a YAML string
    pub fn load_string(&mut self, yaml_text: &str) -> Result<(), Error> {
        let yaml_docs = match YamlLoader::load_from_str(yaml_text) {
            Ok(docs) => docs,
            Err(e) => {
                return Err(Error::ClientConfigError(format!(
                    "Error parsing configuration file: {:?}",
                    e
                )));
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
                return Err(Error::ClientConfigError(format!(
                    "Error loading log4rs config: {}",
                    err
                )));
            }
        } else {
            return Err(Error::ClientConfigError(format!(
                "No log4rs configuration file set"
            )));
        };

        Ok(())
    }

    fn set_bus_config(&mut self, yaml: &yaml::Yaml) -> Result<(), Error> {
        if let Some(p) = yaml["message_bus"]["port"].as_i64() {
            self.bus_config.set_port(p as u16);
        };

        if let Some(h) = yaml["message_bus"]["domain"].as_str() {
            self.bus_config.set_domain(h);
        };

        if let Some(s) = yaml["message_bus"]["username"].as_str() {
            self.bus_config.set_username(s);
        };

        if let Some(s) = yaml["message_bus"]["password"].as_str() {
            self.bus_config.set_password(s);
        };

        Ok(())
    }
}
