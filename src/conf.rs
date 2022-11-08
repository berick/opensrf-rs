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
pub enum BusSubDomainType {
    Private,
    Public
}

impl From<&String> for BusSubDomainType {
    fn from(t: &String) -> BusSubDomainType {
        match t.to_lowercase().as_str() {
            "private" => BusSubDomainType::Private,
            "public" => BusSubDomainType::Public,
            _ => panic!("Invalid subdomain type: {}", t),
        }
    }
}

/// A routable bus domain.
#[derive(Debug, Clone)]
pub struct BusSubDomain {
    name: String,
    port: u16,
    allowed_services: Option<Vec<String>>,
}

impl BusSubDomain {
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
/// Each domain consists of a public and private sub-domain.
/// The public/private subdomains represent pointers to actual
/// bus instances.
#[derive(Debug, Clone)]
pub struct BusDomain {
    name: String,
    private: BusSubDomain,
    public: BusSubDomain,
}

impl BusDomain {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn private(&self) -> &BusSubDomain {
        &self.private
    }
    pub fn public(&self) -> &BusSubDomain {
        &self.public
    }
}

#[derive(Debug, Clone)]
pub enum LogFile {
    Syslog,
    Filename(String),
}

#[derive(Debug, Clone)]
pub struct BusConnectionType {
    subdomain_type: BusSubDomainType,
    credentials: BusCredentials,
    log_level: log::LevelFilter,
    log_file: LogFile,
    syslog_facility: Option<syslog::Facility>,
    activity_log_facility: Option<syslog::Facility>,
}

impl BusConnectionType {
    pub fn subdomain_type(&self) -> &BusSubDomainType {
        &self.subdomain_type
    }
    pub fn credentials(&self) -> &BusCredentials {
        &self.credentials
    }
    pub fn log_level(&self) -> log::LevelFilter {
        self.log_level
    }
    pub fn syslog_facility(&self) -> Option<syslog::Facility> {
        self.syslog_facility
    }
    pub fn activity_log_facility(&self) -> Option<syslog::Facility> {
        self.activity_log_facility
    }
    pub fn log_file(&self) -> &LogFile {
        &self.log_file
    }
}

#[derive(Debug, Clone)]
pub struct BusConnection {
    port: u16,
    domain: String,
    connection_type: BusConnectionType,
}

impl BusConnection {
    pub fn connection_type(&self) -> &BusConnectionType {
        &self.connection_type
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn set_domain(&mut self, domain: &str) {
        self.domain = domain.to_string();
    }
}

#[derive(Debug, Clone)]
pub enum ServiceLang {
    Perl,
    C,
    Rust,
}

impl From<&str> for ServiceLang {
    fn from(s: &str) -> ServiceLang {
        let lang = s.to_lowercase();
        match lang.as_str() {
            "perl" => ServiceLang::Perl,
            "c" => ServiceLang::C,
            "rust" => ServiceLang::Rust,
            _ => panic!("Invalid service launguage: {}", lang),
        }
    }
}

impl From<&ServiceLang> for &'static str {
    fn from(sl: &ServiceLang) -> &'static str {
        match *sl {
            ServiceLang::Perl => "perl",
            ServiceLang::C => "c",
            ServiceLang::Rust => "rust",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Service {
    name: String,
    lang: ServiceLang,
    keepalive: u32,
    min_workers: u32,
    max_workers: u32,
    min_idle_workers: u32,
    max_idle_workers: u32,
    max_requests: u32,
}

impl Service {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn lang(&self) -> &ServiceLang {
        &self.lang
    }
    pub fn keepalive(&self) -> u32 {
        self.keepalive
    }
    pub fn min_workers(&self) -> u32 {
        self.min_workers
    }
    pub fn max_workers(&self) -> u32 {
        self.max_workers
    }
    pub fn min_idle_workers(&self) -> u32 {
        self.min_idle_workers
    }
    pub fn max_idle_workers(&self) -> u32 {
        self.max_idle_workers
    }
    pub fn max_requests(&self) -> u32 {
        self.max_requests
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
    source: Option<yaml::Yaml>,
}

impl Config {
    pub fn to_shared(self) -> Arc<Config> {
        Arc::new(self)
    }

    /// Ref to the YAML structure whence we extracted our config values.
    pub fn source(&self) -> Option<&yaml::Yaml> {
        self.source.as_ref()
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
            primary_connection: None,
            source: None,
        };

        conf.apply_service_groups(&root["service_groups"])?;
        conf.apply_message_bus_config(&root)?;

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
                log::warn!("Expectee service groups to be a hash: {groups:?}");
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
        let domains = match domains.as_vec() {
            Some(d) => d,
            None => {
                return Err(format!("message-bus 'domains' should be a list"));
            }
        };

        for domain_conf in domains {
            let name = self.get_yaml_string_at(&domain_conf, "name")?;

            let private_hash = &domain_conf["private"];
            let public_hash = &domain_conf["public"];

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


            let private = BusSubDomain {
                name: self.get_yaml_string_at(&private_hash, "name")?,
                port: DEFAULT_BUS_PORT,
                allowed_services: private_services,
            };

            let public = BusSubDomain {
                name: self.get_yaml_string_at(&public_hash, "name")?,
                port: DEFAULT_BUS_PORT,
                allowed_services: public_services,
            };

            let domain = BusDomain {
                name: name.to_string(),
                private,
                public,
            };

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
            let subdomain_type = self.get_yaml_string_at(&connection, "subdomain")?;
            let creds_name = self.get_yaml_string_at(&connection, "credentials")?;

            let creds = match self.credentials.get(&creds_name) {
                Some(a) => a,
                None => {
                    return Err(format!("No such credentials: {name}"));
                }
            };

            let file = self.get_yaml_string_at(&connection, "log_file")?;

            let level = self.get_yaml_string_at(&connection, "log_level")?;
            let level = log::LevelFilter::from_str(&level).unwrap();

            let mut facility = None;
            if let Some(f) = &connection["syslog_facility"].as_str() {
                facility = Some(syslog::Facility::from_str(&f).unwrap());
            }

            let mut actlog_facility = None;
            if let Some(f) = &connection["activity_log_facility"].as_str() {
                actlog_facility = Some(syslog::Facility::from_str(&f).unwrap());
            }

            let con = BusConnectionType {
                subdomain_type: (&subdomain_type).into(),
                credentials: creds.clone(),
                log_level: level,
                syslog_facility: facility,
                activity_log_facility: actlog_facility,
                log_file: match file.as_str() {
                    "syslog" => LogFile::Syslog,
                    _ => LogFile::Filename(file)
                },
            };

            self.connections.insert(name, con);
        }

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

        let subdomain = match con_type.subdomain_type() {
            BusSubDomainType::Private => bus_domain.private(),
            _ => bus_domain.public(),
        };

        Ok(BusConnection {
            port: subdomain.port(),
            domain: subdomain.name().to_string(),
            connection_type: con_type.clone(),
        })
    }

    pub fn get_domain(&self, domain: &str) -> Option<&BusDomain> {
        self.domains.iter().filter(|d| d.name().eq(domain)).next()
    }

    /// Returns Option of ref to a BusSubDomain by name.
    pub fn get_subdomain(&self, subdomain: &str) -> Option<&BusSubDomain> {
        for domain in self.domains().iter() {
            if domain.private().name().eq(subdomain) {
                return Some(domain.private());
            }
            if domain.public().name().eq(subdomain) {
                return Some(domain.public());
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
