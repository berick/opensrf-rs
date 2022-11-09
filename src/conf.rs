use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use syslog;
use yaml_rust::yaml;
use yaml_rust::YamlLoader;

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

#[derive(Debug, Clone, PartialEq)]
pub enum BusNodeType {
    Private,
    Public
}

impl From<&String> for BusNodeType {
    fn from(t: &String) -> BusNodeType {
        match t.to_lowercase().as_str() {
            "private" => BusNodeType::Private,
            "public" => BusNodeType::Public,
            _ => panic!("Invalid node type: {}", t),
        }
    }
}

/// A routable bus domain.
#[derive(Debug, Clone)]
pub struct BusNode {
    name: String,
    port: u16,
    allowed_services: Option<Vec<String>>,
}

impl BusNode {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn allowed_services(&self) -> Option<&Vec<String>> {
        self.allowed_services.as_ref()
    }
}

/// A Message Bus Domain
///
/// Each domain consists of a public and private node.
#[derive(Debug, Clone)]
pub struct BusDomain {
    name: String,
    private_node: BusNode,
    public_node: BusNode,
}

impl BusDomain {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn private_node(&self) -> &BusNode {
        &self.private_node
    }
    pub fn public_node(&self) -> &BusNode {
        &self.public_node
    }
}

#[derive(Debug, Clone)]
pub enum LogFile {
    Syslog,
    Filename(String),
}

#[derive(Debug, Clone)]
pub struct LogOptions {
    log_level: Option<log::LevelFilter>,
    log_file: Option<LogFile>,
    syslog_facility: Option<syslog::Facility>,
    activity_log_facility: Option<syslog::Facility>,
}

impl LogOptions {
    pub fn syslog_facility(&self) -> Option<syslog::Facility> {
        self.syslog_facility
    }
    pub fn activity_log_facility(&self) -> Option<syslog::Facility> {
        self.activity_log_facility
    }
    pub fn log_file(&self) -> &Option<LogFile> {
        &self.log_file
    }
    pub fn log_level(&self) -> &Option<log::LevelFilter> {
        &self.log_level
    }
}


#[derive(Debug, Clone)]
pub struct BusConnectionType {
    node_type: BusNodeType,
    credentials: BusCredentials,
    logging: LogOptions,
}

impl BusConnectionType {
    pub fn node_type(&self) -> &BusNodeType {
        &self.node_type
    }
    pub fn credentials(&self) -> &BusCredentials {
        &self.credentials
    }
    pub fn logging(&self) -> &LogOptions {
        &self.logging
    }
}

#[derive(Debug, Clone)]
pub struct BusConnection {
    port: u16,
    domain_name: String,
    node_name: String,
    connection_type: BusConnectionType,
}

impl BusConnection {
    pub fn connection_type(&self) -> &BusConnectionType {
        &self.connection_type
    }
    pub fn domain_name(&self) -> &str {
        &self.domain_name
    }
    pub fn node_name(&self) -> &str {
        &self.node_name
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn set_node_name(&mut self, node_name: &str) {
        self.node_name = node_name.to_string();
    }
}


#[derive(Debug, Clone)]
pub struct Config {
    connections: HashMap<String, BusConnectionType>,
    credentials: HashMap<String, BusCredentials>,
    domains: Vec<BusDomain>,
    service_groups: HashMap<String, Vec<String>>,
    log_protect: Vec<String>,
    log_defaults: Option<LogOptions>,
    primary_connection: Option<BusConnection>,
    source: Option<yaml::Yaml>,
}

impl Config {
    pub fn into_shared(self) -> Arc<Config> {
        Arc::new(self)
    }

    /// Ref to the YAML structure whence we extracted our config values.
    pub fn source(&self) -> Option<&yaml::Yaml> {
        self.source.as_ref()
    }

    pub fn log_defaults(&self) -> Option<&LogOptions> {
        self.log_defaults.as_ref()
    }

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
            log_defaults: None,
            primary_connection: None,
            source: None,
        };

        conf.apply_service_groups(&root["service_groups"])?;
        conf.apply_log_defaults(&root["log_defaults"])?;
        conf.apply_credentials(&root["credentials"])?;
        conf.apply_domains(&root["domains"])?;
        conf.apply_connections(&root["connections"])?;

        if let Some(arr) = root["log_protect"].as_vec() {
            for lp in arr {
                conf.log_protect.push(conf.unpack_yaml_string(lp)?);
            }
        }

        conf.source = Some(root.to_owned());

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

    fn _get_yaml_number_at(&self, thing: &yaml::Yaml, key: &str) -> Result<i64, String> {
        match thing[key].as_i64() {
            Some(s) => Ok(s),
            None => Err(format!(
                "get_yaml_number_at cannot coerce into i64: {thing:?}"
            )),
        }
    }

    fn get_yaml_string_at(&self, thing: &yaml::Yaml, key: &str) -> Result<String, String> {
        self.unpack_yaml_string(&thing[key])
    }

    fn apply_log_defaults(&mut self, options: &yaml::Yaml) -> Result<(), String> {
        self.log_defaults = Some(self.build_log_config(options)?);
        Ok(())
    }

    fn build_log_config(&mut self, log_config: &yaml::Yaml) -> Result<LogOptions, String> {


        let mut options = LogOptions {
            log_level: None,
            syslog_facility: None,
            activity_log_facility: None,
            log_file: None,
        };

        let stub = options.clone();
        let defaults = self.log_defaults().unwrap_or(&stub);

        if let Some(file) = log_config["log_file"].as_str() {
            options.log_file = match file {
                "syslog" => Some(LogFile::Syslog),
                _ => Some(LogFile::Filename(file.to_string()))
            };
        } else {
            options.log_file = defaults.log_file().clone();
        }

        if let Some(level) = log_config["log_level"].as_str() {
            options.log_level = Some(log::LevelFilter::from_str(&level).unwrap());
        } else {
            options.log_level = defaults.log_level().clone();
        }

        if let Some(f) = &log_config["syslog_facility"].as_str() {
            options.syslog_facility = Some(syslog::Facility::from_str(&f).unwrap());
        } else {
            options.syslog_facility = defaults.syslog_facility().clone();
        }

        if let Some(f) = &log_config["activity_log_facility"].as_str() {
            options.activity_log_facility = Some(syslog::Facility::from_str(&f).unwrap());
        } else {
            options.activity_log_facility = defaults.activity_log_facility().clone();
        }

        Ok(options)
    }

    fn apply_service_groups(&mut self, groups: &yaml::Yaml) -> Result<(), String> {
        let hash = match groups.as_hash() {
            Some(h) => h,
            None => {
                log::warn!("Expected service groups to be a hash: {groups:?}");
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

            log::debug!("Registering service group {name}");
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

        if let Some(domains) = domains.as_vec() {
            for domain_conf in domains {
                let name = self.get_yaml_string_at(&domain_conf, "name")?;
                self.add_domain(name, &domain_conf)?;
            }

            return Ok(());
        }

        return Err(format!("message-bus 'domains' should be a list"));
    }

    fn add_domain(&mut self, name: String, domain_conf: &yaml::Yaml) -> Result<(), String> {

        let private_hash = &domain_conf["private_node"];
        let public_hash = &domain_conf["public_node"];

        let mut private_services: Option<Vec<String>> = None;
        let mut public_services: Option<Vec<String>> = None;

        if let Some(group) = private_hash["allowed_services"].as_str() {
            if let Some(list) = self.service_groups.get(group) {
                private_services = Some(list.clone());
            } else {
                return Err(format!("No such service group: {group}"));
            }
        }

        if let Some(group) = public_hash["allowed_services"].as_str() {
            if let Some(list) = self.service_groups.get(group) {
                public_services = Some(list.clone());
            } else {
                return Err(format!("No such service group: {group}"));
            }
        }

        let private_node = BusNode {
            name: self.get_yaml_string_at(&private_hash, "name")?,
            port: DEFAULT_BUS_PORT,
            allowed_services: private_services,
        };

        let public_node = BusNode {
            name: self.get_yaml_string_at(&public_hash, "name")?,
            port: DEFAULT_BUS_PORT,
            allowed_services: public_services,
        };

        let domain = BusDomain {
            name: name.to_string(),
            private_node,
            public_node,
        };

        self.domains.push(domain);

        Ok(())
    }

    fn apply_connections(&mut self, connections: &yaml::Yaml) -> Result<(), String> {

        if let Some(hash) = connections.as_hash() {
            for (name, connection) in hash {
                let name = self.unpack_yaml_string(name)?;
                self.add_connection(name, &connection)?;
            }

            return Ok(());
        }

        return Err(format!("We have no connections!"));
    }

    fn add_connection(&mut self, name: String, connection: &yaml::Yaml) -> Result<(), String> {
        let node_type = self.get_yaml_string_at(&connection, "node_type")?;

        // TODO merge log defaults
        let log_config = self.build_log_config(&connection)?;

        let creds_name = self.get_yaml_string_at(&connection, "credentials")?;
        let creds = match self.credentials.get(&creds_name) {
            Some(a) => a,
            None => {
                return Err(format!("No such credentials: {name}"));
            }
        };

        let con = BusConnectionType {
            node_type: (&node_type).into(),
            credentials: creds.clone(),
            logging: log_config,
        };

        self.connections.insert(name, con);

        Ok(())
    }

    pub fn primary_connection(&self) -> Option<&BusConnection> {
        self.primary_connection.as_ref()
    }

    pub fn new_bus_connection(&self, contype: &str, domain: &str) -> Result<BusConnection, String> {
        let bus_domain = match self.get_domain(domain) {
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

        let node = match con_type.node_type() {
            BusNodeType::Private => bus_domain.private_node(),
            _ => bus_domain.public_node(),
        };

        Ok(BusConnection {
            port: node.port(),
            domain_name: domain.to_string(),
            node_name: node.name().to_string(),
            connection_type: con_type.clone(),
        })
    }

    pub fn get_domain(&self, domain: &str) -> Option<&BusDomain> {
        self.domains.iter().filter(|d| d.name().eq(domain)).next()
    }

    /// Returns Option of ref to a BusNode by name.
    pub fn get_node(&self, node_name: &str) -> Option<&BusNode> {
        for domain in self.domains().iter() {
            if domain.private_node().name().eq(node_name) {
                return Some(domain.private_node());
            }
            if domain.public_node().name().eq(node_name) {
                return Some(domain.public_node());
            }
        }

        None
    }

    pub fn get_connection_type(&self, contype: &str) -> Option<&BusConnectionType> {
        self.connections.get(contype)
    }

    pub fn set_primary_connection(
        &mut self,
        connection_type: &str,
        domain_name: &str,
    ) -> Result<&BusConnection, String> {
        let con = self.new_bus_connection(connection_type, domain_name)?;
        self.primary_connection = Some(con);
        Ok(self.primary_connection.as_ref().unwrap())
    }
}
