///! Various types that can be transformed into a set of API parameters,
/// which are a Vec<JsonValue> under the covers.
use super::client::Client;
use json::JsonValue;

pub struct ApiParams {
    params: Vec<JsonValue>,
}

impl ApiParams {
    pub fn serialize(&self, client: &Client) -> Option<Vec<JsonValue>> {
        if let Some(s) = client.singleton().borrow().serializer() {
            Some(self.params.iter().map(|v| s.pack(&v)).collect())
        } else {
            None
        }
    }

    pub fn deserialize(&self, client: &Client) -> Option<Vec<JsonValue>> {
        if let Some(s) = client.singleton().borrow().serializer() {
            Some(self.params.iter().map(|v| s.unpack(&v)).collect())
        } else {
            None
        }
    }

    pub fn params(&self) -> &Vec<JsonValue> {
        &self.params
    }
}

impl From<&Vec<JsonValue>> for ApiParams {
    fn from(v: &Vec<JsonValue>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| j.clone()).collect(),
        }
    }
}

impl From<Vec<JsonValue>> for ApiParams {
    fn from(v: Vec<JsonValue>) -> ApiParams {
        ApiParams {
            params: v
        }
    }
}

impl From<JsonValue> for ApiParams {
    fn from(v: JsonValue) -> ApiParams {
        ApiParams {
            params: vec![v],
        }
    }
}

impl From<&JsonValue> for ApiParams {
    fn from(v: &JsonValue) -> ApiParams {
        ApiParams {
            params: vec![v.clone()],
        }
    }
}

impl From<&str> for ApiParams {
    fn from(v: &str) -> ApiParams {
        ApiParams {
            params: vec![json::from(v.to_string())]
        }
    }
}

impl From<&Vec<&str>> for ApiParams {
    fn from(v: &Vec<&str>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| json::from(*j)).collect(),
        }
    }
}

impl From<&Vec<u8>> for ApiParams {
    fn from(v: &Vec<u8>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| json::from(*j)).collect(),
        }
    }
}

impl From<&Vec<i64>> for ApiParams {
    fn from(v: &Vec<i64>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| json::from(*j)).collect(),
        }
    }
}

impl From<&Vec<u64>> for ApiParams {
    fn from(v: &Vec<u64>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| json::from(*j)).collect(),
        }
    }
}

impl From<&String> for ApiParams {
    fn from(v: &String) -> ApiParams {
        ApiParams {
            params: vec![json::from(v.to_string())]
        }
    }
}



