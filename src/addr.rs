use super::util;
use gethostname::gethostname;
use std::fmt;
use std::process;

const BUS_ADDR_NAMESPACE: &str = "opensrf";

/// Models a bus-level address providing access to indivual components
/// of each address.
///
/// Examples:
///
/// opensrf:service:$service
/// opensrf:client:$domain:$hostname:$pid:$random
/// opensrf:router:$domain
#[derive(Debug, Clone)]
pub struct BusAddress {
    /// Full raw address string
    full: String,

    /// Address prefix, eg. "opensrf"
    namespace: String,

    /// A top-level service address has no domain.
    domain: Option<String>,

    /// Only top-level service addresses have a service name
    service: Option<String>,

    is_client: bool,
    is_service: bool,
    is_router: bool,
}

impl fmt::Display for BusAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Address={}", &self.full)
    }
}

impl BusAddress {
    /// Create a new bus address for a client.
    pub fn new_for_client(domain: &str) -> Self {
        let full = format!(
            "{}:client:{}:{}:{}:{}",
            BUS_ADDR_NAMESPACE,
            domain,
            &gethostname().into_string().unwrap(),
            process::id(),
            &util::random_number(8)
        );

        BusAddress {
            full,
            namespace: BUS_ADDR_NAMESPACE.to_string(),
            domain: Some(domain.to_string()),
            service: None,
            is_client: true,
            is_service: false,
            is_router: false,
        }
    }

    /// Create a new bus address for a router.
    pub fn new_for_router(domain: &str) -> Self {
        let full = format!("{}:router:{}", BUS_ADDR_NAMESPACE, &domain);

        BusAddress {
            full,
            namespace: BUS_ADDR_NAMESPACE.to_string(),
            domain: Some(domain.to_string()),
            service: None,
            is_client: false,
            is_service: false,
            is_router: true,
        }
    }

    /// Create a new bus address for a router.
    pub fn new_for_service(service: &str) -> Self {
        let full = format!("{}:service:{}", BUS_ADDR_NAMESPACE, &service);

        BusAddress {
            full,
            namespace: BUS_ADDR_NAMESPACE.to_string(),
            domain: None,
            service: Some(service.to_string()),
            is_client: false,
            is_service: true,
            is_router: false,
        }
    }

    /// Creates a new BusAddress from a bus address string.
    pub fn new_from_string(full: &str) -> Result<Self, String> {
        let parts: Vec<&str> = full.split(':').collect();

        // We only really care about the first 3 parts of the address.
        if parts.len() < 3 {
            return Err(format!("BusAddress bad format: {}", full));
        }

        let namespace = parts[0].to_string();
        let purpose = parts[1];
        let sod = parts[2].to_string(); // service name or domain

        let mut addr = BusAddress {
            full: full.to_string(),
            namespace,
            domain: None,
            service: None,
            is_client: false,
            is_service: false,
            is_router: false,
        };

        if purpose.eq("service") {
            addr.service = Some(sod);
            addr.is_service = true;
        } else if purpose.eq("client") {
            addr.domain = Some(sod);
            addr.is_client = true;
        } else if purpose.eq("router") {
            addr.domain = Some(sod);
            addr.is_router = true;
        } else {
            return Err(format!("Unknown BusAddress purpose: {}", purpose));
        }

        Ok(addr)
    }
}

impl BusAddress {
    /// Full address string
    pub fn full(&self) -> &str {
        &self.full
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn domain(&self) -> Option<&str> {
        self.domain.as_deref()
    }
    pub fn service(&self) -> Option<&str> {
        self.service.as_deref()
    }
    pub fn is_client(&self) -> bool {
        self.is_client
    }
    pub fn is_service(&self) -> bool {
        self.is_service
    }
    pub fn is_router(&self) -> bool {
        self.is_router
    }
}
