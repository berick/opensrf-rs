use std::collections::HashMap;
use std::fmt;
use log::{trace, warn, error};
use super::*;

/// Internal, mutable session and request tracking structs

pub struct Request {
    pub complete: bool,
    pub thread: String,
    pub thread_trace: usize,
}

pub enum SessionType {
    Client,
    Server,
}

pub struct Session {

    pub session_type: SessionType,

    /// Each session is identified on the network by a random thread string.
    pub thread: String,

    pub connected: bool,

    /// Service name.
    ///
    /// For Servers, this is the name of the service we host.
    /// For Clients, this is the target of our conversation.
    pub service: String,

    /// Bus ID of ouf service
    pub service_addr: String,

    /// Worker-specific bus address for our session.
    /// Only set once we are communicating with a specific worker.
    pub remote_addr: Option<String>,

    /// Each new Request within a Session gets a new thread_trace.
    /// Replies have the same thread_trace as their request.
    pub last_thread_trace: usize,

    /// Backlog of unprocessed messages received for this session.
    pub backlog: Vec<message::Message>,

    pub requests: HashMap<usize, Request>,
}

impl Session {

    pub fn new(service: &str) -> Self {

        let ses = Session {
            session_type: SessionType::Client,
            service: String::from(service),
            connected: false,
            remote_addr: None,
            service_addr: String::from("service:") + service,
            last_thread_trace: 0,
            thread: util::random_16(),
            backlog: Vec::new(),
            requests: HashMap::new(),
        };

        trace!("Creating session service={} thread={}", service, ses.thread);

        ses
    }

    /// Adds a response to the session backlog so it can
    /// be recv()'d later.
    ///
    /// Ignore responses to unknown requests or those that have
    /// already been marked complete.
    pub fn add_to_backlog(&mut self, msg: message::Message) {
        if let Some(r) = self.requests.get(&msg.thread_trace()) {
            if !r.complete {
                self.backlog.push(msg);
            }
        }
    }

    pub fn reset(&mut self) {
        self.remote_addr = None;
        self.connected = false;
    }

    pub fn remote_addr(&self) -> &str {
        if let Some(ref ra) = self.remote_addr {
            ra
        } else {
            &self.service_addr
        }
    }

    /// Returns true if the provided request has pending replies
    pub fn has_pending_replies(&self, thread_trace: usize) -> bool {
        self.backlog.iter().any(|r| r.thread_trace() == thread_trace)
    }
}

