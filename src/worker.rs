use super::{Config, Client, Method, ParamCount, ClientHandle};
use super::server::{WorkerState, WorkerStateEvent};
use super::message;
use super::addr::BusAddress;
use super::conf;
use std::fmt;
use std::thread;
use std::time;
use std::sync::mpsc;
use std::collections::HashMap;
use std::sync::Arc;
use std::cell::{Ref, RefMut};

/// A Worker runs in its own thread and responds to API requests.
pub struct Worker {
    service: String,

    client: ClientHandle,

    // True if the caller has requested a stateful conversation.
    connected: bool,

    // True if we have informed our parent that we are busy.
    marked_active: bool,

    methods: &'static [Method],

    // Method requests that we've seen whose API names match
    // the name spec of one of our configured methods.
    known_methods: HashMap<String, &'static Method>,

    config: Arc<Config>,

    // Keep a local copy for convenience
    service_conf: conf::Service,

    // Thread for our in-progress conversation.
    cur_thread: Option<String>,

    worker_id: u64,

    // Channel for sending worker state info to our parent.
    to_parent_tx: mpsc::SyncSender<WorkerStateEvent>,
}

impl fmt::Display for Worker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Worker ({})", self.worker_id)
    }
}

impl Worker {

    pub fn new(
        service: String,
        worker_id: u64,
        config: Arc<Config>,
        methods: &'static [Method],
        to_parent_tx: mpsc::SyncSender<WorkerStateEvent>
    ) -> Result<Worker, String> {

        let sconf = match config.services().iter().filter(|s| s.name().eq(&service)).next() {
            Some(sc) => sc.clone(),
            None => {
                return Err(format!("No configuration for service {service}"));
            }
        };

        let client = Client::new(config.clone())?;

        Ok(Worker {
            service,
            worker_id,
            config,
            methods,
            client,
            to_parent_tx,
            cur_thread: None,
            connected: false,
            marked_active: false,
            service_conf: sconf,
            known_methods: HashMap::new(),
        })
    }

    // Configuration for the service we are hosting.
    //
    // Assumes we have a config for our service and panics if none is found.
    fn service_conf(&self) -> &conf::Service {
        &self.service_conf
    }

    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }

    /// Shortcut for mutable client ref.
    fn client(&mut self) -> RefMut<Client> {
        self.client.client_mut()
    }

    pub fn listen(&mut self) {
        let selfstr = format!("{self}");

        let mut requests: u32 = 0;
        let max_reqs = self.service_conf().max_requests();
        let service_addr = BusAddress::new_for_service(&self.service).full().to_string();
        let local_addr = self.client().address().to_string();
        let keepalive = self.service_conf().keepalive() as i32;

        // Setup the stream for our service.  This is the top-level
        // service address where new requests arrive.
        if let Err(e) = self.client().bus_mut().setup_stream(Some(&service_addr)) {
            log::error!("{selfstr} cannot setup service stream at {service_addr}: {e}");
            return;
        }

        while requests < max_reqs {

            let timeout: i32;
            let sent_to: &str;

            if self.connected {
                // We're in the middle of a stateful conversation.
                // Listen for messages sent specifically to our bus
                // address and only wait up to keeplive seconds for
                // subsequent messages.
                sent_to = &local_addr;
                timeout = keepalive;

            } else {

                if let Err(e) = self.reset() {
                    log::error!("{selfstr} could not reset {e}.  Exiting");
                    break;
                }

                // Wait indefinitely for top-level service messages.
                sent_to = &service_addr;
                timeout = -1;
            }

            log::trace!("{selfstr} calling recv() timeout={} sent_to={}", timeout, sent_to);

            let recv_op = self.client().bus_mut().recv(timeout, Some(sent_to));

            match recv_op {

                Err(e) => {
                    log::error!("{selfstr} recv() in listen produced an error: {e}");

                    // If an error occurs here, we can get stuck in a tight
                    // loop that's hard to break.  Add a sleep so we can
                    // more easily control-c out of the loop.
                    thread::sleep(time::Duration::from_secs(1));
                    self.connected = false;
                },

                Ok(recv_op) => {

                    match recv_op {

                        None => {
                            // See if the caller failed to send a follow-up
                            // request within the keepalive timeout.
                            if self.connected {
                                log::warn!("{selfstr} timeout waiting on request while connected");
                                self.connected = false;
                                // TODO send a timeout message
                            }
                        },

                        Some(msg) => {
                            if let Err(e) = self.handle_transport_message(&msg) {
                                log::error!("{selfstr} error handling message: {e}");
                                self.connected = false;
                            }
                        }
                    }
                }
            }

            // If we are connected, we remain Active and avoid counting
            // subsequent requests within this stateful converstation
            // toward our overall request count.
            if self.connected { continue; }

            // Some scenarios above (e.g. malformed message) do not result
            // in being in an Active state.
            if self.marked_active {
                self.marked_active = false;
                if let Err(_) = self.notify_state(WorkerState::Idle) {
                    // If we can't notify our parent, it means the parent
                    // thread is no longer running.  Get outta here.
                    log::error!("{selfstr} could not notify parent of Idle state. Exiting.");
                    break;
                }
            }

            requests += 1;
        }

        log::trace!("{self} exiting on max requests or early break");

        if let Err(_) = self.notify_state(WorkerState::Done) {
            log::error!("{self} failed to notify parent of Done state");
        }
    }

    fn handle_transport_message(&mut self, tmsg: &message::TransportMessage) -> Result<(), String> {
        let sender = BusAddress::new_from_string(tmsg.from())?;
        for msg in tmsg.body().iter() {
            self.handle_message(tmsg.thread(), &sender, &msg)?;
        }
        Ok(())
    }

    // Clear our local message bus and reset state maintenance values.
    fn reset(&mut self) -> Result<(), String> {
        self.connected = false;
        self.cur_thread = None;
        self.client().clear()
    }

    fn handle_message(&mut self, thread: &str, sender: &BusAddress, msg: &message::Message) -> Result<(), String> {
        match msg.mtype() {
            message::MessageType::Disconnect => {
                log::trace!("{self} received a DISCONNECT");
                self.reset()?;
                Ok(())
            }
            message::MessageType::Connect => {
                log::trace!("{self} received a CONNECT");

                if self.connected {
                    return self.reply_bad_request(sender, msg, "Worker is already connected");
                }

                self.connected = true;
                self.cur_thread = Some(thread.to_string());

                // TODO send connect-ok message
                Ok(())
            }
            message::MessageType::Request => {
                log::trace!("{self} received a REQUEST");

                if self.connected {
                    let ct = self.cur_thread.as_ref().unwrap(); // Set at Connect
                    if ct.ne(thread) {
                        return self.reply_bad_request(sender, msg,
                            "Request thread does not match in-progress thread");
                    }
                } else {
                    self.cur_thread = Some(thread.to_string());
                }
                self.handle_request(sender, msg)
            }
            _ => {
                self.reply_bad_request(sender, msg, "Unexpected message type")
            }
        }
    }

    fn handle_request(&mut self, sender: &BusAddress, msg: &message::Message) -> Result<(), String> {

        let request = match msg.payload() {
            message::Payload::Method(m) => m,
            _ => {
                return self.reply_bad_request(
                    sender, msg, "Request sent without payload");
            }
        };

        // We have a well-formed Request message.
        // Tell the parent we're going to be busy (unless we already have).
        if !self.marked_active {
            if let Err(e) = self.notify_state(WorkerState::Active) {
                return Err(format!(
                    "{self} failed to notify parent of Active state. Exiting. {e}"));
            }
            self.marked_active = true;
        }

        // Log the API call
        log::debug!("CALL: {} {}", request.method(),
            request.params().iter().map(|p| p.dump()).collect::<Vec<_>>().join(", "));

        // Before we begin processing a service-level request, clear our
        // local message bus to avoid encountering any stale messages
        // lingering from the previous conversation.
        if !self.connected {
            self.client().clear()?;
        }

        let method = match self.find_method(request.method()) {
            Some(m) => m,
            None => {
                // TODO send method-not-found response
                return Err(format!(
                    "API name '{}' not found for service {}",
                    request.method(), self.service
                ));
            }
        };

        // Make sure the number of params sent by the caller matches the
        // parameter count for the method.
        if !ParamCount::matches(method.param_count(), request.params().len() as u8) {
            return self.reply_bad_request(sender, msg,
                &format!(
                    "Invalid param count sent: method={} sent={} needed={}",
                    request.method(), request.params().len(), method.param_count()
                )
            );
        }

        if let Err(err) = (method.handler())(self.client.clone(), &request) {
            // TODO reply internal server error
            return Err(format!("{self} method {} failed with {err}", request.method()));
        }

        Ok(())
    }

    fn find_method(&mut self, api_name: &str) -> Option<&'static Method> {

        if let Some(m) = self.known_methods.get(api_name) {
            log::trace!("{self} found known good method for {api_name}");
            return Some(*m);
        }


        // Look for a registered method whose API spec matches the
        // api name for the requested method.
        for m in self.methods.iter() {
            if m.api_name_matches(api_name) {
                // Store the found method under the requested name for
                // faster lookup on subsequent calls.
                self.known_methods.insert(api_name.to_string(), &m);
                return Some(&m);
            }
        }

        log::error!("{self} no method found matching api name: {api_name}");

        None
    }


    fn reply_bad_request(&mut self, sender: &BusAddress,
        msg: &message::Message, text: &str) -> Result<(), String> {

        self.connected = false;

        // TODO
        /*
        let msg = message::Message::new(
            sender, &self.addr, req_id,
            message::MessageType::BadRequest,
            message::Payload::Error(String::from(msg))
        );

        self.bus.send(&msg)
        */
        Ok(())
    }

    /// Notify the parent process of this worker's active state.
    fn notify_state(&self, state: WorkerState) ->
        Result<(), mpsc::SendError<WorkerStateEvent>> {

        log::trace!("{self} notifying parent of state change => {state:?}");

        self.to_parent_tx.send(
            WorkerStateEvent {
                worker_id: self.worker_id(),
                state: state
            }
        )
    }
}
