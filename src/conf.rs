use std::fs;
use yaml_rust::yaml;
use yaml_rust::YamlLoader;
use syslog;
use std::str::FromStr;
use std::collections::HashMap;
//use std::default::Default;

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
    name: String,
    services: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BusAccount {
    name: String,
    username: String,
    password: String,
}

impl BusAccount {
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn password(&self) -> &str {
        &self.password
    }
}

#[derive(Debug, Clone)]
pub struct BusDomain {
    name: String,
    port: u16,
    hosted_services: Option<Vec<String>>,
}

impl BusDomain {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn port(&self) -> u16 {
        self.port
    }
}

#[derive(Debug, Clone)]
pub struct BusConnectionType {
    name: String,
    account: BusAccount,
    log_level: log::Level,
    log_facility: syslog::Facility,
    act_facility: Option<syslog::Facility>,
}

impl BusConnectionType {
    pub fn account(&self) -> &BusAccount {
        &self.account
    }
    pub fn log_level(&self) -> log::Level {
        self.log_level
    }
    pub fn log_facility(&self) -> syslog::Facility {
        self.log_facility
    }
    pub fn act_facility(&self) -> Option<syslog::Facility> {
        self.act_facility
    }
}

#[derive(Debug, Clone)]
pub struct BusConnection {
    domain: BusDomain,
    connection_type: BusConnectionType,
}

impl BusConnection {
    pub fn connection_type(&self) -> &BusConnectionType {
        &self.connection_type
    }

    pub fn domain(&self) -> &BusDomain {
        &self.domain
    }

    pub fn set_domain(&mut self, domain: &BusDomain) {
        self.domain = domain.clone();
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    connections: HashMap<String, BusConnectionType>,
    accounts: HashMap<String, BusAccount>,
    domains: Vec<BusDomain>,
    service_groups: HashMap<String, ServiceGroup>,
    log_protect: Vec<String>,
    primary_connection: Option<BusConnection>,
}

impl Config {

    /// Load configuration from a YAML file.
    ///
    /// May panic on invalid values (e.g. invalid log level) or unexpected
    /// Yaml config structures.
    pub fn from_file(filename: &str) -> Result<Self, String> {
        match fs::read_to_string(filename) {
            Ok(text) => Config::from_string(&text),
            Err(e) => Err(format!(
                "Error reading configuration file: file='{}' {:?}",
                filename, e
            ))
        }
    }

    pub fn from_string(yaml_text: &str) -> Result<Self, String> {

        let op = YamlLoader::load_from_str(yaml_text);

        if let Err(e) = op {
            return Err(format!("Error parsing configuration file: {:?}", e));
        }

        let docs = op.unwrap();
        let root = &docs[0];

        let mut conf = Config {
            accounts: HashMap::new(),
            connections: HashMap::new(),
            domains: Vec::new(),
            service_groups: HashMap::new(),
            log_protect: Vec::new(),
            primary_connection: None,
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

            let group = ServiceGroup {
                name: name.to_string(),
                services: services
            };

            self.service_groups.insert(name, group);
        }

        Ok(())
    }

    fn apply_accounts(&mut self, accounts: &yaml::Yaml) -> Result<(), String> {
        for (name, value) in accounts.as_hash().unwrap() {
            let name = self.unpack_yaml_string(&name)?;
            let acct = BusAccount {
                name: name.to_string(),
                username: self.get_yaml_string_at(&value, "username")?,
                password: self.get_yaml_string_at(&value, "password")?
            };
            self.accounts.insert(name, acct);
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
                name: name.to_string(),
                hosted_services: None,
                port: DEFAULT_BUS_PORT,
            };

            // "hosted-services" is optional
            if let Some(name) = domain_conf["hosted-services"].as_str() {
                match self.service_groups.get(name) {
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

            let acct = match self.accounts.get(&act_name) {
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

            let con = BusConnectionType {
                name: name.to_string(),
                account: acct.clone(),
                log_level: level,
                log_facility: facility,
                act_facility: actfac,
            };

            self.connections.insert(name, con);
        }

        Ok(())
    }

    pub fn set_primary_connection(&mut self,
        connection_type: &str, domain: &str) -> Result<&BusConnection, String> {

        let con = self.new_bus_connection(connection_type, domain)?;
        self.primary_connection = Some(con);
        Ok(self.primary_connection.as_ref().unwrap())
    }

    pub fn primary_connection(&self) -> Option<&BusConnection> {
        self.primary_connection.as_ref()
    }

    pub fn new_bus_connection(&self, contype: &str, domain: &str) -> Result<BusConnection, String> {

        let bus_domain = match self.domains.iter().filter(|d| d.name().eq(domain)).next() {
            Some(bd) => bd,
            None => {
                return Err(format!("No configuration for domain {domain}"));
            }
        };

        let con_type = match self.connections.get(contype) {
            Some(ct) => ct,
            None => {
                return Err(format!("No such connection type: {contype}"));
            }
        };

        Ok(BusConnection {
            domain: bus_domain.clone(),
            connection_type: con_type.clone(),
        })
    }

    pub fn multi_domain_support(&self) -> bool {
        self.domains.len() > 1
    }

    pub fn get_domain(&self, domain: &str) -> Option<&BusDomain> {
        self.domains.iter().filter(|d| d.name().eq(domain)).next()
    }
}


