///! OpenSRF Syslog
use log;
use std::os::unix::net::UnixDatagram;
use std::process;
use syslog;

const SYSLOG_UNIX_PATH: &str = "/dev/log";

/// Main logging structure
///
/// NOTE this logs directly to the syslog UNIX path instead of going through
/// the syslog crate.  This approach gives us much more control.
pub struct Logger {
    loglevel: log::LevelFilter,
    facility: syslog::Facility,
    writer: Option<UnixDatagram>,
    _threads: bool,
}

impl Logger {
    pub fn new(loglevel: log::LevelFilter, facility: syslog::Facility) -> Self {
        Logger {
            loglevel,
            facility,
            _threads: false,
            writer: None,
        }
    }

    pub fn set_loglevel(&mut self, loglevel: log::LevelFilter) {
        self.loglevel = loglevel
    }

    pub fn set_facility(&mut self, facility: syslog::Facility) {
        self.facility = facility
    }

    /// Setup our global log handler.
    ///
    /// Attempts to connect to syslog unix socket if possible.
    pub fn init(mut self) -> Result<(), log::SetLoggerError> {
        match UnixDatagram::unbound() {
            Ok(socket) => match socket.connect(SYSLOG_UNIX_PATH) {
                Ok(()) => self.writer = Some(socket),
                Err(e) => {
                    eprintln!("Cannot connext to unix socket: {e}");
                }
            },
            Err(e) => {
                eprintln!("Cannot connext to unix socket: {e}");
            }
        }

        log::set_max_level(self.loglevel);
        log::set_boxed_logger(Box::new(self))?;

        Ok(())
    }

    /// Encode the facility and severity as the syslog priority.
    ///
    /// Essentially copied from the syslog crate.
    fn encode_priority(&self, severity: syslog::Severity) -> syslog::Priority {
        self.facility as u8 | severity as u8
    }
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        &metadata.level().to_level_filter() <= &self.loglevel
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let levelname = record.level().to_string();
        let target = if !record.target().is_empty() {
            record.target()
        } else {
            record.module_path().unwrap_or_default()
        };

        let severity = self.encode_priority(match levelname.to_lowercase().as_str() {
            "debug" | "trace" => syslog::Severity::LOG_DEBUG,
            "info" => syslog::Severity::LOG_INFO,
            "warn" => syslog::Severity::LOG_WARNING,
            _ => syslog::Severity::LOG_ERR,
        });

        let message = format!(
            "<{}>[{}:{}:{}:{}] {}",
            severity,
            levelname,
            process::id(),
            target,
            match record.line() {
                Some(l) => l,
                None => 0,
            },
            record.args()
        );

        if let Some(ref w) = self.writer {
            if w.send(message.as_bytes()).is_ok() {
                return;
            }
        }

        println!("{message}");
    }

    fn flush(&self) {}
}
