use super::client::Client;
use super::conf::Config;

const SETTINGS_TIMEOUT: i32 = 10;

pub struct SettingsClient {
    client: Client,
    settings: Option<json::JsonValue>,
}

impl SettingsClient {
    pub fn new(client: Client) -> Self {
        SettingsClient {
            client,
            settings: None,
        }
    }

    pub fn set_host_config(&mut self, force: bool) -> Result<(), String> {

        if self.settings.is_some() && !force {
            return Ok(());
        }

        let mut ses = self.client.session("opensrf.settings");

        let mut req = ses.request(
            "opensrf.settings.default_config.get",
            vec![
                json::from(self.client.config().hostname()),
                json::from(force)
            ]
        )?;

        self.settings = req.recv(SETTINGS_TIMEOUT)?;

        if self.settings.is_none() {
            Err(format!("Settings server returned no response!"))
        } else {
            Ok(())
        }
    }

    pub fn settings(&self) -> &json::JsonValue {
        match &self.settings {
            Some(s) => &s,
            None => &json::JsonValue::Null
        }
    }
}


