use std::fmt;
use regex::Regex;
use super::message;
use super::session;
use super::client;

const REGEX_MATCH_NOTHING: &str = "a^";

type MethodHandler = fn(client::ClientHandle, session::ServerSession, &message::Method) -> Result<(), String>;

#[derive(Debug, Copy, Clone)]
pub enum ParamCount {
    Any,
    Zero,
    Exactly(u8),
    AtLeast(u8),
    Range(u8, u8), // Inclusive
}

impl ParamCount {
    pub fn matches(pc: &ParamCount, count: u8) -> bool {
        match *pc {
            ParamCount::Any => { return true; }
            ParamCount::Zero => { return count == 0; }
            ParamCount::Exactly(c) => { return count == c; }
            ParamCount::AtLeast(c) => { return count >= c; }
            ParamCount::Range(s, e) => { return s <= count && e >= count; }
        }
    }
}

impl fmt::Display for ParamCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParamCount::Any => write!(f, "Any"),
            ParamCount::Zero => write!(f, "Zero"),
            ParamCount::Exactly(c) => write!(f, "Exactly {}", c),
            ParamCount::AtLeast(c) => write!(f, "AtLeast {}", c),
            ParamCount::Range(s, e) => write!(f, "Between {}..{}", s, e),
        }
    }
}

pub struct Method {
    /// Regex for matching to incoming API call names
    pub api_spec: &'static str,
    pub param_count: ParamCount,
    pub handler: MethodHandler
}

impl Method {

    pub fn param_count(&self) -> &ParamCount {
        &self.param_count
    }

    pub fn handler(&self) -> MethodHandler {
        self.handler
    }

    /// Returns true if the provided API name matches the api regex for
    /// this method.
    ///
    /// ```
    /// fn foo() -> Result<(), String> { Ok(()) }
    ///
    /// let m = opensrf::Method::new(
    ///     "opensrf.private.auto", opensrf::ParamCount::Exactly(1), foo).unwrap();
    ///
    /// assert!(m.api_name_matches("opensrf.private.auto.kazoo"));
    ///
    /// assert_eq!(m.api_name_matches("opensrf.private.kazoo"), false);
    /// ```
    pub fn api_name_matches(&self, api_name: &str) -> bool {

        let re = match Regex::new(&self.api_spec) {
            Ok(r) => r,
            Err(e) => {
                log::error!("Invalid API name spec regex: {e} => {}", &self.api_spec);
                return false;
            }
        };

        if re.is_match(api_name) {
            log::debug!("Found a method match for {api_name}");
            return true;
        }

        return false;
    }
}

