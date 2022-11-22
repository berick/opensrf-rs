use chrono::{Local, DateTime};
use chrono::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use std::thread;
use getopts;
use opensrf::conf;
use opensrf::bus;

const DEFAULT_WAIT_TIME_MILLIS: u64 = 2000;

// Redis lists are deleted every time the last value in the list is
// popped.  If a list key persists for many minutes, it means the list
// is never fully drained, suggesting the backend responsible for
// popping values from the list is not longer alive OR is perpetually
// under extremely heavy load.  Tell keys to delete themselves after
// this many seconds of being unable to drain the list.
const DEFAULT_KEY_EXPIRE_SECS: u64 = 1800; // 30 minutes

struct BusWatch {
    config: Arc<conf::Config>,
    domain: String,
    bus: bus::Bus,
    wait_time: u64,
    start_time: DateTime<Local>,
}

impl BusWatch {
    pub fn new(config: Arc<conf::Config>, domain: &str) -> Self {

        let mut busconf = match config.get_router_conf(domain) {
            Some(rc) => rc.client().clone(),
            None => panic!("No router config for domain {}", domain),
        };

        // We connect using info on our routers, but we want to login
        // with our own credentials from the main config.client()
        // object, which are subject to command-line username/ password
        // overrides.

        busconf.set_username(config.client().username());
        busconf.set_password(config.client().password());

        let bus = match bus::Bus::new(&busconf) {
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
            "start_time":
                json::from(format!("{}", self.start_time.format("%FT%T%z"))),
        };

        loop {

            thread::sleep(Duration::from_millis(self.wait_time));

            // Check all opensrf keys.
            // NOTE could break these up into router, service, and client keys.
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

            obj["stats"] = json::JsonValue::new_object();
            obj["errors"] = json::JsonValue::new_array();

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
                        let err = format!("Error reading LLEN list={key} error={e}");
                        log::error!("{err}");
                        obj["errors"].push(json::from(err)).ok();
                        break;
                    }
                }

                if let Err(e) = self.bus.set_key_timeout(key, DEFAULT_KEY_EXPIRE_SECS, "NX") {
                    obj["errors"].push(json::from(e));
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
