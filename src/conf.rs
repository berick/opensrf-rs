use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use syslog;
use yaml_rust::yaml;
use yaml_rust::YamlLoader;
//use std::default::Default;

const DEFAULT_BUS_PORT: u16 = 6379;

/// A set of bus login credentials
#[derive(Debug, Clone)]
pub struct BusCredentials {
    username: String,
    password: String,
}

impl BusCredentials {
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn password(&self) -> &str {
        &self.password
    }
}

/// A routable bus domain.
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
    credentials: BusCredentials,
    log_level: log::LevelFilter,
    log_facility: syslog::Facility,
    act_facility: Option<syslog::Facility>,
}

impl BusConnectionType {
    pub fn credentials(&self) -> &BusCredentials {
        &self.credentials
    }
    pub fn log_level(&self) -> log::LevelFilter {
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
    credentials: HashMap<String, BusCredentials>,
    domains: Vec<BusDomain>,
    service_groups: HashMap<String, Vec<String>>,
    log_protect: Vec<String>,
    primary_connection: Option<BusConnection>,
}

impl Config {
    pub fn domains(&self) -> &Vec<BusDomain> {
        &self.domains
    }

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
            )),
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
            credentials: HashMap::new(),
            connections: HashMap::new(),
            domains: Vec::new(),
            service_groups: HashMap::new(),
            log_protect: Vec::new(),
            primary_connection: None,
        };

        conf.apply_service_groups(&root["service-groups"])?;
        conf.apply_message_bus_config(&root["message-bus"])?;

        if let Some(arr) = root["log-protect"].as_vec() {
            for lp in arr {
                conf.log_protect.push(conf.unpack_yaml_string(lp)?);
            }
        }

        Ok(conf)
    }

    fn unpack_yaml_string(&self, thing: &yaml::Yaml) -> Result<String, String> {
        match thing.as_str() {
            Some(s) => Ok(s.to_string()),
            None => Err(format!(
                "unpack_yaml_string() cannot coerce into string: {thing:?}"
            )),
        }
    }

    fn get_yaml_string_at(&self, thing: &yaml::Yaml, key: &str) -> Result<String, String> {
        self.unpack_yaml_string(&thing[key])
    }

    fn apply_message_bus_config(&mut self, bus_conf: &yaml::Yaml) -> Result<(), String> {
        self.apply_credentials(&bus_conf["credentials"])?;
        self.apply_domains(&bus_conf["domains"])?;
        self.apply_connections(&bus_conf["connections"])?;

        Ok(())
    }

    fn apply_service_groups(&mut self, groups: &yaml::Yaml) -> Result<(), String> {
        let hash = match groups.as_hash() {
            Some(h) => h,
            None => {
                return Ok(());
            }
        };

        for (name, list) in hash {
            let name = self.unpack_yaml_string(name)?;
            let list = list.as_vec().unwrap();
            let services = list
                .iter()
                .map(|s| s.as_str().unwrap().to_string())
                .collect();
            self.service_groups.insert(name, services);
        }

        Ok(())
    }

    fn apply_credentials(&mut self, credentials: &yaml::Yaml) -> Result<(), String> {
        for (name, value) in credentials.as_hash().unwrap() {
            let name = self.unpack_yaml_string(&name)?;
            let acct = BusCredentials {
                username: self.get_yaml_string_at(&value, "username")?,
                password: self.get_yaml_string_at(&value, "password")?,
            };
            self.credentials.insert(name, acct);
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
                    Some(list) => {
                        domain.hosted_services = Some(list.clone());
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
            let creds_name = self.get_yaml_string_at(&connection, "credentials")?;

            let creds = match self.credentials.get(&creds_name) {
                Some(a) => a,
                None => {
                    return Err(format!("No such credentials: {name}"));
                }
            };

            let level = self.get_yaml_string_at(&connection, "loglevel")?;
            let level = log::LevelFilter::from_str(&level).unwrap();

            let facility = self.get_yaml_string_at(&connection, "syslog-facility")?;
            let facility = syslog::Facility::from_str(&facility).unwrap();

            let mut actfac = None;
            if let Some(af) = connection["actlog-facility"].as_str() {
                actfac = Some(syslog::Facility::from_str(&af).unwrap());
            }

            let con = BusConnectionType {
                credentials: creds.clone(),
                log_level: level,
                log_facility: facility,
                act_facility: actfac,
            };

            self.connections.insert(name, con);
        }

        Ok(())
    }

    pub fn set_primary_connection(
        &mut self,
        connection_type: &str,
        domain: &str,
    ) -> Result<&BusConnection, String> {
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
