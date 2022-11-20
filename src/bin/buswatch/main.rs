use chrono::{Local, DateTime};
use chrono::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use std::thread;
use getopts;
use opensrf::conf;
use opensrf::bus;
use opensrf::logging::Logger;

const DEFAULT_WAIT_TIME_MILLIS: u64 = 1000;

struct BusWatch {
    config: Arc<conf::Config>,
    domain: String,
    bus: bus::Bus,
    wait_time: u64,
    start_time: DateTime<Local>,
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

        let wait_time = DEFAULT_WAIT_TIME_MILLIS;

        BusWatch {
            bus,
            config,
            wait_time,
            start_time: Local::now(),
            domain: domain.to_string()
        }
    }

    pub fn watch(&mut self) {

        let mut obj = json::object!{
            "domain": json::from(self.domain.as_str()),
            "start_time": json::from(format!("{}", self.start_time.format("%FT%T%z"))),
            "stats": {}
        };

        loop {

            thread::sleep(Duration::from_millis(self.wait_time));

            let keys = match self.bus.keys("opensrf:*") {
                Ok(k) => k,
                Err(e) => {
                    log::error!("Error in keys() command: {e}");
                    continue;
                }
            };

            if keys.len() == 0 {
                continue;
            }

            for key in keys.iter() {
                match self.bus.llen(key) {
                    Ok(l) => {
                        // The list may have cleared in the time between the
                        // time we called keys() and llen().
                        if l > 0 {
                            obj["stats"][key] = json::from(l);
                        }
                    }
                    Err(e) => {
                        log::error!("Error reading list length list={key} error={e}");
                        break;
                    }
                }
            }

            obj["current_time"] =
                json::from(format!("{}", Local::now().format("%FT%T%z")));

            println!("{}", obj.dump());
        }
    }
}

fn main() {
    let mut ops = getopts::Options::new();

    ops.optmulti("d", "domain", "Domain", "DOMAIN");

    let (config, params) = opensrf::init_with_options(&mut ops).unwrap();
    let config = config.into_shared();

    let mut domains = params.opt_strs("domain");

    if domains.len() == 0 {
        // Watch all routed domains by default.
        domains = config.routers()
            .iter().map(|r| r.client().domain().name().to_string()).collect();
        if domains.len() == 0 {
            panic!("Watcher requires at least on domain");
        }
    }

    println!("Starting buswatch for domains: {domains:?}");

    // A watcher for each domain runs within its own thread.
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();

    for domain in domains.iter() {
        let conf = config.clone();
        let domain = domain.clone();

        threads.push(thread::spawn(move || {
            let mut watcher = BusWatch::new(conf, &domain);
            watcher.watch();
        }));
    }

    // Wait for threads to complete.
    for thread in threads {
        thread.join().ok();
    }
}
