use super::client;
use super::client::ClientRequest;
use super::client::ClientSession;
use super::error;
use super::message::Message;
use super::message::MessageType;
use super::message::Method;
use super::message::Payload;
use super::session::Request;
use super::session::Session;
use json::JsonValue;
use log::{debug, trace};
use std::collections::HashMap;
use tungstenite;
use url::Url;

struct WebsocketMessage {
    service: String,
    thread: String,
    osrf_msg: Vec<Message>,
}

impl WebsocketMessage {
    pub fn thread(&self) -> &str {
        &self.thread
    }
    pub fn service(&self) -> &str {
        &self.service
    }
    fn to_json_value(&self) -> JsonValue {
        let mut arr = JsonValue::new_array();
        for msg in self.osrf_msg.iter() {
            arr.push(msg.to_json_value()).ok();
        }
        json::object! {
            service: self.service(),
            thread: self.thread(),
            osrf_msg: json::from(arr),
        }
    }

    fn from_json_value(service: &str, value: &JsonValue) -> Option<WebsocketMessage> {
        let mut msg_vec: Vec<Message> = Vec::new();

        match &value["osrf_msg"] {
            JsonValue::Array(v) => {
                for m in v {
                    let msg = Message::from_json_value(&m).unwrap(); // TODO
                    msg_vec.push(msg);
                }
            }
            _ => {
                return None;
            } // TODO log
        }

        let thread = match value["thread"].as_str() {
            Some(t) => t.to_string(),
            None => {
                return None;
            } // TODO log
        };

        Some(WebsocketMessage {
            thread: thread,
            service: service.to_string(),
            osrf_msg: msg_vec,
        })
    }
}

pub struct WebsocketClient<'a> {
    uri: String,
    client: tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>,
    sessions: HashMap<String, Session>,
    pub serializer: Option<&'a dyn client::DataSerializer>,
}

impl WebsocketClient<'_> {
    pub fn new(uri: &str) -> Self {
        let (socket, response) =
            tungstenite::connect(Url::parse(uri).unwrap()).expect("Can't connect");

        debug!(
            "WS connected to server with HTTP code {}",
            response.status()
        );

        trace!("Response contains the following headers:");
        for (ref header, _value) in response.headers() {
            trace!("* {}", header);
        }

        WebsocketClient {
            client: socket,
            uri: uri.to_string(),
            sessions: HashMap::new(),
            serializer: None,
        }
    }

    pub fn send(&mut self, client_ses: &ClientSession, msg: Message) -> Result<(), error::Error> {
        let ses = self.sessions.get(client_ses.thread()).unwrap();

        let ws_msg = WebsocketMessage {
            service: ses.service.to_string(),
            thread: client_ses.thread().to_string(),
            osrf_msg: vec![msg],
        };

        let text = ws_msg.to_json_value().dump();

        trace!("WS::send() {}", text);

        self.client
            .write_message(tungstenite::Message::Text(text.into()))
            .unwrap();

        Ok(())
    }

    pub fn recv(&mut self, client_ses: &ClientSession) -> Result<Option<JsonValue>, error::Error> {
        trace!("WS::recv()...");

        let ses = self.sessions.get(client_ses.thread()).unwrap();

        let msg_text = self.client.read_message().expect("Error reading message");

        trace!("WS recv() got {}", msg_text);

        let value = match json::parse(msg_text.to_text().unwrap()) {
            // TODO avoid panic
            Ok(v) => v,
            Err(e) => {
                return Err(error::Error::JsonError(e));
            }
        };

        let ws_msg = match WebsocketMessage::from_json_value(&ses.service, &value) {
            Some(m) => m,
            None => {
                return Err(error::Error::BadResponseError);
            }
        };

        trace!("WS recv() {}", ws_msg.osrf_msg[0].to_json_value());

        // TODO unpack

        Ok(None)
    }

    pub fn session(&mut self, service: &str) -> ClientSession {
        let ses = Session::new(service);
        let client_ses = ClientSession::new(&ses.thread);

        self.sessions.insert(ses.thread.to_string(), ses);

        client_ses
    }

    pub fn request<T>(
        &mut self,
        client_ses: &ClientSession,
        method: &str,
        params: Vec<T>,
    ) -> Result<ClientRequest, error::Error>
    where
        T: Into<JsonValue>,
    {
        let ses = self.sessions.get_mut(client_ses.thread()).unwrap();

        ses.last_thread_trace += 1;

        let mut param_vec: Vec<JsonValue> = Vec::new();
        for param in params {
            param_vec.push(json::from(param));
        }

        let payload;

        if let Some(s) = self.serializer {
            let mut packed_params = Vec::new();
            for par in param_vec {
                packed_params.push(s.pack(&par));
            }
            payload = Payload::Method(Method::new(method, packed_params));
        } else {
            payload = Payload::Method(Method::new(method, param_vec));
        }

        let req = Message::new(MessageType::Request, ses.last_thread_trace, payload);

        self.send(client_ses, req)?;

        let ses = self.sessions.get_mut(client_ses.thread()).unwrap();

        trace!("request() adding request to {}", client_ses);

        ses.requests.insert(
            ses.last_thread_trace,
            Request {
                complete: false,
                thread: client_ses.thread().to_string(),
                thread_trace: ses.last_thread_trace,
            },
        );

        let mut r = ClientRequest::new(client_ses.thread(), ses.last_thread_trace);
        r.set_method(method);

        Ok(r)
    }
}
