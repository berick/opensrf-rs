use log::{trace, warn, error};
use std::collections::HashMap;
use std::time;
use std::fmt;
use std::net;
use json::JsonValue;
use tungstenite;
use url::Url;
use super::util;
use super::error;
use super::message::Message;
use super::message::MessageType;
use super::message::MessageStatus;
use super::message::Method;
use super::message::Payload;
use super::session::Request;
use super::session::Session;
use super::session::SessionType;

struct WebsocketMessage {
    service: String,
    thread: String,
    osrf_msg: Message,
}

impl WebsocketMessage {

    pub fn thread(&self) -> &str {
        &self.thread
    }
    pub fn service(&self) -> &str {
        &self.service
    }
    fn to_json_value(&self) -> JsonValue {
        json::object!{
            service: self.service(),
            thread: self.thread(),
            osrf_msg: self.osrf_msg.to_json_value().dump(), // XXX body is double encoded
        }
    }
}

pub struct WebsocketClient {
    uri: String,
    //client: tungstenite::client,
    client: tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>,
}

impl WebsocketClient {
    pub fn new(uri: &str) -> Self {

        let (mut socket, response) =
            tungstenite::connect(Url::parse(uri).unwrap()).expect("Can't connect");

        WebsocketClient {
            client: socket,
            uri: uri.to_string(),
        }
    }

    pub fn send(&mut self, msg: Message) -> Result<(), error::Error> {

        let ws_msg = WebsocketMessage {
            service: "open-ils.actor".to_string(),
            thread: util::random_12(),
            osrf_msg: msg
        };

        let text = ws_msg.to_json_value().dump();

        trace!("WS::send() {}", text);

        self.client.write_message(tungstenite::Message::Text(text.into())).unwrap();

        Ok(())
    }

    pub fn recv(&mut self) -> Result<Option<JsonValue>, error::Error> {

        let msg = self.client.read_message().expect("Error reading message");

        trace!("WS recv() got {}", msg);

        Ok(None)
    }
}



