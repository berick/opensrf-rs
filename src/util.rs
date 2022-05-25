use json;
use rand::Rng;

/// Returns a random 12-char numeric string
pub fn random_12() -> String {
    let mut rng = rand::thread_rng();
    let num: u64 = rng.gen_range(100_000_000_000..1_000_000_000_000);
    format!("{:0width$}", num, width = 12)
}

pub fn random_16() -> String {
    let mut rng = rand::thread_rng();
    let num: u64 = rng.gen_range(100_000_000_000..1_000_000_000_000_000);
    format!("{:0width$}", num, width = 16)
}

/// Converts a JSON number or string to an isize if possible
pub fn json_isize(value: &json::JsonValue) -> Option<isize> {
    if let Some(i) = value.as_isize() {
        return Some(i);
    } else if let Some(s) = value.as_str() {
        if let Ok(i2) = s.parse::<isize>() {
            return Some(i2);
        }
    };

    None
}

/// Converts a JSON number or string to an usize if possible
pub fn json_usize(value: &json::JsonValue) -> Option<usize> {
    if let Some(i) = value.as_usize() {
        return Some(i);
    } else if let Some(s) = value.as_str() {
        if let Ok(i2) = s.parse::<usize>() {
            return Some(i2);
        }
    };

    None
}
