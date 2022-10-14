use std::fs;
use yaml_rust::yaml;
use yaml_rust::YamlLoader;
use syslog;
use std::str::FromStr;

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

// -------

#[derive(Debug, Clone)]
pub struct ServiceGroup {
    pub name: String,
    pub services: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BusAccount {
    pub name: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct BusDomain {
    pub domain: String,
    pub hosted_services: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct BusConnection {
    pub name: String,
    pub account: BusAccount,
    pub log_level: log::Level,
    pub log_facility: syslog::Facility,
    pub act_facility: Option<syslog::Facility>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub connections: Vec<BusConnection>,
    pub accounts: Vec<BusAccount>,
    pub domains: Vec<BusDomain>,
    pub service_groups: Vec<ServiceGroup>,
    pub log_protect: Vec<String>,
}

impl Config {

    /// Load configuration from a YAML file
    ///
    /// May panic on invalid values (e.g. invalid log level) or invalid
    /// Yaml config structures.
    pub fn from_file(filename: &str) -> Result<Self, String> {
        let op = fs::read_to_string(filename);

        if let Err(e) = op {
            return Err(format!(
                "Error reading configuration file: file='{}' {:?}",
                filename, e
            ));
        }

        Config::from_string(&op.unwrap())
    }

    pub fn from_string(yaml_text: &str) -> Result<Self, String> {

        let op = YamlLoader::load_from_str(yaml_text);

        if let Err(e) = op {
            return Err(format!("Error parsing configuration file: {:?}", e));
        }

        let docs = op.unwrap();
        let root = &docs[0];

        let mut conf = Config {
            accounts: Vec::new(),
            connections: Vec::new(),
            domains: Vec::new(),
            service_groups: Vec::new(),
            log_protect: Vec::new(),
        };

        conf.apply_service_groups(&root["service-groups"]);
        conf.apply_message_bus_config(&root["message-bus"])?;

        if let Some(arr) = root["log-protect"].as_vec() {
            for lp in arr {
                conf.log_protect.push(conf.unpack_yaml_string(lp)?);
            }
        }

        println!("CONF: {conf:?}");

        Ok(conf)
    }

    fn unpack_yaml_string(&self, thing: &yaml::Yaml) -> Result<String, String> {
        match thing.as_str() {
            Some(s) => Ok(s.to_string()),
            None => Err(format!("Cannot coerce into string: {thing:?}"))
        }
    }

    fn get_yaml_string_at(&self, thing: &yaml::Yaml, key: &str) -> Result<String, String> {
        self.unpack_yaml_string(&thing[key])
    }

    fn apply_message_bus_config(&mut self, bus_conf: &yaml::Yaml) -> Result<(), String> {

        self.apply_accounts(&bus_conf["accounts"])?;
        self.apply_domains(&bus_conf["domains"])?;
        self.apply_connections(&bus_conf["connections"])?;

        Ok(())
    }

    fn apply_service_groups(&mut self, groups: &yaml::Yaml) -> Result<(), String>{

        let hash = match groups.as_hash() {
            Some(h) => h,
            None => { return Ok(()); }
        };

        for (name, list) in hash {
            let name = self.unpack_yaml_string(name)?;
            let list = list.as_vec().unwrap();
            let services =
                list.iter().map(|s| s.as_str().unwrap().to_string()).collect();

            self.service_groups.push(ServiceGroup {
                name: name.to_string(),
                services: services
            });
        }

        Ok(())
    }

    fn apply_accounts(&mut self, accounts: &yaml::Yaml) -> Result<(), String> {
        for (name, value) in accounts.as_hash().unwrap() {
            self.accounts.push(
                BusAccount {
                    name: self.unpack_yaml_string(&name)?,
                    username: self.get_yaml_string_at(&value, "username")?,
                    password: self.get_yaml_string_at(&value, "password")?
                }
            );
        }

        Ok(())
    }

    fn apply_domains(&mut self, domains: &yaml::Yaml) -> Result<(), String> {

        let domains = match domains.as_vec() {
            Some(d) => d,
            None => {
                return Err(format!("message-bus 'domains' should be a list"));
            }
        };

        for domain_conf in domains {
            let name = self.get_yaml_string_at(&domain_conf, "name")?;

            let mut domain = BusDomain {
                domain: name.to_string(),
                hosted_services: None,
            };

            // "hosted-services" is optional
            if let Some(name) = domain_conf["hosted-services"].as_str() {
                match self.service_groups.iter().filter(|g| g.name.eq(name)).next() {
                    Some(group) => {
                        domain.hosted_services = Some(group.services.clone());
                    }
                    None => {
                        return Err(format!("No such service group: {name}"));
                    }
                }
            }

            self.domains.push(domain);
        }

        Ok(())
    }

    fn apply_connections(&mut self, connections: &yaml::Yaml) -> Result<(), String> {

        let hash = match connections.as_hash() {
            Some(h) => h,
            None => {
                return Err(format!("We have no connections!"));
            }
        };

        for (name, connection) in hash {

            let name = self.unpack_yaml_string(name)?;
            let act_name = self.get_yaml_string_at(&connection, "account")?;

            let acct = match self.accounts.iter().filter(|a| a.name.eq(&act_name)).next() {
                Some(a) => a,
                None => {
                    return Err(format!("No such account: {name}"));
                }
            };

            let level = self.get_yaml_string_at(&connection, "loglevel")?;
            let level = log::Level::from_str(&level).unwrap();

            let facility = self.get_yaml_string_at(&connection, "syslog-facility")?;
            let facility = syslog::Facility::from_str(&facility).unwrap();

            let mut actfac = None;
            if let Some(af) = connection["actlog-facility"].as_str() {
                actfac = Some(syslog::Facility::from_str(&af).unwrap());
            }

            let con = BusConnection {
                name,
                account: acct.clone(),
                log_level: level,
                log_facility: facility,
                act_facility: actfac,
            };

            self.connections.push(con);
        }

        Ok(())
    }
}


