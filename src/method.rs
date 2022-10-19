use std::fmt;
// TODO
//type MethodHandler = fn(&mut RequestContext) -> Result<(), super::Error>;
type MethodHandler = fn() -> Result<(), String>;

#[derive(Copy, Clone)]
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
    api_spec: &'static str,
    param_count: ParamCount,
    handler: MethodHandler
}

impl Method {
    pub fn api_spec(&self) -> &str {
        &self.api_spec
    }

    pub fn param_count(&self) -> &ParamCount {
        &self.param_count
    }

    pub fn handler(&self) -> MethodHandler {
        self.handler
    }
}

