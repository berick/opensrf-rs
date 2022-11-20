use std::sync::Arc;
use std::time::Duration;
use std::thread;
use getopts;
use opensrf::conf;
use opensrf::bus;
use opensrf::client;
use opensrf::logging::Logger;

struct BusWatch {
    config: Arc<conf::Config>,
    domain: String,
    bus: bus::Bus,
}

impl BusWatch {
    pub fn new(config: Arc<conf::Config>, domain: &str) -> Self {

        let busconf = match config.get_router_conf(domain) {
            Some(rc) => rc.client(),
            None => panic!("No router config for domain {}", domain),
        };

        let bus = match bus::Bus::new(busconf) {
            Ok(b) => b,
            Err(e) => panic!("Cannot connect bus: {}", e),
        };

        BusWatch {
            bus,
            config,
            domain: domain.to_string()
        }
    }

    pub fn watch(&mut self) {

        loop {

            let keys = match self.bus.keys("opensrf:*") {
                Ok(k) => k,
                Err(e) => panic!("Exiting on keys() error: {}", e),
            };

            for key in keys.iter() {
                match self.bus.llen(key) {
                    Ok(l) => println!("{key} length={l}"),
                    Err(e) => panic!("Exiting on failed llen command: {}", e),
                }
            }

            // TODO configurable wait time.
            thread::sleep(Duration::from_millis(2000));
        }
    }
}

fn main() {
    let mut ops = getopts::Options::new();

    ops.optmulti("d", "domain", "Domain", "DOMAIN");

    let init_ops = opensrf::InitOptions { skip_logging: true };

    let (config, params) = opensrf::init_with_options(&init_ops, &mut ops).unwrap();
    let config = config.into_shared();

    let mut domains = params.opt_strs("domain");

    if domains.len() == 0 {
        domains = config.routers()
            .iter().map(|r| r.client().domain().name().to_string()).collect();

        if domains.len() == 0 {
            panic!("Router requries at least one domain");
        }
    }

    println!("Starting buswatch for domains: {domains:?}");

    // Our global Logger is configured with the settings for the
    // router for the first domain found.
    let domain0 = &domains[0];
    let rconf = match config.get_router_conf(domain0) {
        Some(c) => c,
        None => panic!("No router configuration found for domain {}", domain0),
    };

    if let Err(e) = Logger::new(rconf.client().logging()).unwrap().init() {
        panic!("Error initializing logger: {}", e);
    }

    // A router for each specified domain runs within its own thread.
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();

    for domain in domains.iter() {
        let conf = config.clone();
        let domain = domain.clone();

        threads.push(thread::spawn(move || {
            let mut watcher = BusWatch::new(conf, &domain);
            watcher.watch();
        }));
    }

    // Block here while the routers are running.
    for thread in threads {
        thread.join().ok();
    }
}
