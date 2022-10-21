use std::fmt;
use regex::Regex;
// TODO
//type MethodHandler = fn(&mut RequestContext) -> Result<(), super::Error>;
type MethodHandler = fn() -> Result<(), String>;

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

#[derive(Debug)]
pub struct Method {
    /// Regex for matching to incoming API call names
    api_regex: Regex,
    param_count: ParamCount,
    handler: MethodHandler
}

impl Method {

    pub fn new(api_spec: &str,
        param_count: ParamCount, handler: MethodHandler) -> Result<Self, String> {

        let re = match Regex::new(api_spec) {
            Ok(r) => r,
            Err(e) => {
                return Err(format!(
                    "Invalid API name api_name: {} => {}", e, api_spec));
            }
        };

        Ok(Method {
            api_regex: re,
            param_count,
            handler,
        })
    }

    pub fn param_count(&self) -> &ParamCount {
        &self.param_count
    }

    pub fn handler(&self) -> MethodHandler {
        self.handler
    }

    /// Returns true if the provided API name matches the api_regex for
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
        if self.api_regex.is_match(api_name) {
            log::debug!("Found a method match for {api_name}");
            return true;
        }

        return false;
    }
}

