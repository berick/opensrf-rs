use json;
use redis;
use std::error;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    /// Invalid configuration file/value
    ClientConfigError(String),

    RequestTimeoutError,

    ConnectTimeoutError,

    BadResponseError,

    NoSuchThreadError,

    MethodNotFoundError,

    /// Error occurred during network communication
    BusError(redis::RedisError),

    /// Something in the code doesn't make sense
    InternalApiError(&'static str),

    /// General purpose JSON parsing, etc. error.
    JsonError(json::Error),
}

use self::Error::*;

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            BusError(ref err) => Some(err),
            JsonError(ref err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            BusError(ref err) => err.fmt(f),
            JsonError(ref err) => err.fmt(f),
            InternalApiError(s) => write!(f, "internal api error: {}", s),
            ClientConfigError(ref s) => write!(f, "configuration error: {}", s),
            RequestTimeoutError => write!(f, "request timed out"),
            ConnectTimeoutError => write!(f, "connect timed out"),
            BadResponseError => write!(f, "unexpected response received"),
            NoSuchThreadError => write!(f, "attempt to reference unknown session thread"),
            MethodNotFoundError => write!(f, "method not found"),
        }
    }
}

impl From<redis::RedisError> for Error {
    fn from(inner: redis::RedisError) -> Error {
        BusError(inner)
    }
}
