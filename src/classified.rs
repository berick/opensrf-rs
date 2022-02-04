/// Encode / Decode JSON values with class names

const JSON_CLASS_KEY: &str = "__c";
const JSON_PAYLOAD_KEY: &str = "__p";

pub struct JsonWithClass {
    json: json::JsonValue,
    class: String,
}

impl JsonWithClass {

    pub fn json(&self) -> &json::JsonValue {
        &self.json
    }

    pub fn class(&self) -> &str {
        &self.class
    }

    /// Wraps a json value in class and payload keys.
    ///
    /// Non-recursive.
    pub fn encode(json: &json::JsonValue, class: &str) -> json::JsonValue {

        let mut hash = json::JsonValue::new_object();
        hash.insert(JSON_CLASS_KEY, class);
        hash.insert(JSON_PAYLOAD_KEY, json.clone());

        hash
    }

    /// Turns a json value into a JsonWithClass if it's a hash
    /// with the needed class and payload keys.
    ///
    /// Non-recursive.
    pub fn decode(json: &json::JsonValue) -> Option<JsonWithClass> {

        if json.is_object()
            && json.has_key(JSON_CLASS_KEY)
            && json.has_key(JSON_PAYLOAD_KEY)
            && json[JSON_CLASS_KEY].is_string() {

            Some(
                JsonWithClass {
                    class: json[JSON_CLASS_KEY].as_str().unwrap().to_string(),
                    json: json[JSON_PAYLOAD_KEY].clone(),
                }
            )
        } else {
            None
        }
    }
}


