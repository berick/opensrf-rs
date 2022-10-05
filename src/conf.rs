use std::fs;
use yaml_rust::yaml;
use yaml_rust::YamlLoader;

const DEFAULT_BUS_PORT: u16 = 6379;
const DEFAULT_BUS_DOMAIN: &str = "localhost";

#[derive(Debug, Clone)]
pub struct BusConfig {
    domain: String,
    port: u16,
    username: String,
    password: String,
}

impl BusConfig {
    pub fn new(domain: &str, port: u16, username: &str, password: &str) -> Self {
        BusConfig {
            port,
            domain: domain.to_string(),
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn set_domain(&mut self, domain: &str) {
        self.domain = domain.to_string();
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }

    pub fn set_username(&mut self, username: &str) {
        self.username = username.to_string();
    }

    pub fn set_password(&mut self, password: &str) {
        self.password = password.to_string();
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

#[derive(Debug, Clone)]
pub struct ClientConfig {
    bus_config: BusConfig,
    multi_domain_support: bool,
    /*
    log_file: LogFile,
    log_level: LogLevel,
    syslog_facility: Option<String>,
    actlog_facility: Option<String>,
    settings_file: Option<String>,
    */
}

impl ClientConfig {
    pub fn multi_domain_support(&self) -> bool {
        self.multi_domain_support
    }

    pub fn enable_multi_domain_support(&mut self) {
        self.multi_domain_support = true;
    }

    pub fn bus_config(&self) -> &BusConfig {
        &self.bus_config
    }

    pub fn bus_config_mut(&mut self) -> &mut BusConfig {
        &mut self.bus_config
    }

    /// Load configuration from a YAML file
    pub fn from_file(config_file: &str) -> Result<Self, String> {
        let yaml_text = match fs::read_to_string(config_file) {
            Ok(t) => t,
            Err(e) => {
                return Err(format!(
                    "Error reading configuration file: file='{}' {:?}",
                    config_file, e
                ));
            }
        };

        ClientConfig::from_string(&yaml_text)
    }

    /// Load configuration from a YAML string
    pub fn from_string(yaml_text: &str) -> Result<Self, String> {
        let yaml_docs = match YamlLoader::load_from_str(yaml_text) {
            Ok(docs) => docs,
            Err(e) => {
                return Err(format!("Error parsing configuration file: {:?}", e));
            }
        };

        let root = &yaml_docs[0];

        let mut conf = ClientConfig {
            bus_config: ClientConfig::create_bus_config(root)?,
            multi_domain_support: false, // TODO
        };

        conf.set_logging_config(root)?;

        Ok(conf)
    }

    fn set_logging_config(&mut self, yaml: &yaml::Yaml) -> Result<(), String> {
        if let Some(filename) = yaml["logging"]["log4rs_config"].as_str() {
            if let Err(err) = log4rs::init_file(filename, Default::default()) {
                eprintln!("Error loading log4rs config: {}", err);
                return Err(format!("Error loading log4rs config: {}", err));
            }
        } else {
            return Err(format!("No log4rs configuration file set"));
        };

        Ok(())
    }

    fn create_bus_config(yaml: &yaml::Yaml) -> Result<BusConfig, String> {
        let port = match yaml["message_bus"]["port"].as_i64() {
            Some(p) => p as u16,
            None => DEFAULT_BUS_PORT,
        };

        let domain = match yaml["message_bus"]["domain"].as_str() {
            Some(d) => d,
            None => DEFAULT_BUS_DOMAIN,
        };

        let username = match yaml["message_bus"]["username"].as_str() {
            Some(u) => u,
            None => {
                return Err(format!("BusConfig requires a username"));
            }
        };

        let password = match yaml["message_bus"]["password"].as_str() {
            Some(u) => u,
            None => {
                return Err(format!("BusConfig requires a password"));
            }
        };

        Ok(BusConfig {
            port,
            domain: domain.to_string(),
            username: username.to_string(),
            password: password.to_string(),
        })
    }
}
