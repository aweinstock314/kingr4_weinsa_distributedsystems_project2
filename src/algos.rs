// Algos.rs (TODO - rename)
// Functionality for maintaining, updating filesystem.
// Also helps determine what message content should be broadcast to whom.

use broadcasts::BroadcastAlgorithm;
use serde::{Serialize, Deserialize};
use serde_json;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::sync::mpsc;


pub trait HandleMessage {
    type Pid;
    type Message;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)>; //destination + message for output of handlemessage
}

// System:
// Used by control thread (main).
// Keeps track of - logical filesystem, process message logs.
// Also carries broadcast to invoke, reciever channel use.
// Templated over (a) broadcast implementation in broadcasts.rs.
pub struct System<B:BroadcastAlgorithm<UnderlyingMessage = SystemRequestMessage>> {
    broadcast: B, // broadcast algorithm, to access/send messages
    files: HashMap<String, String>, // Maps filename -> filecontent
    receiver: mpsc::Receiver<SystemRequestMessage>,
    log: Vec<B::Message>, // Virtual log of messages sent/recieved since last save-to-disk
    disk_log: Option<File>, // File loaded on startup, written version of virtual log
}

// Implementation of System constructor, message handler (which updates the filesystem conditionally)
impl<B:BroadcastAlgorithm<UnderlyingMessage = SystemRequestMessage>> System<B> where 
    <System<B> as HandleMessage>::Pid: Display,
    B::Message: Clone + Serialize + Deserialize {
     pub fn new(broadcast: B, pid: <Self as HandleMessage>::Pid) -> System<B> {
        let (transmit, receive) = mpsc::channel();
        let logname = format!("log_{}.txt", pid);
        let disk_log = OpenOptions::new()
            .append(true).read(true).create(true)
            .open(&logname)
            .expect("Error in System constructor, could not open log file on disk.");

        // Set up new system image.
        let mut s = System {
            broadcast: broadcast,
            files: HashMap::new(),
            receiver: receive,
            log: Vec::new(),
            disk_log: None,
        };
        s.broadcast.set_on_deliver(Box::new(move |msg| {
            debug!("Got {:?} via ZAB", msg);
            transmit.send(msg.clone()).unwrap();
        }));

        // If logfile exists, read data from it, reconstruct system from it as though recovering from crash
        // (hackey) Use own handlemessage protocol to update filesystem. Flag to not update log while we are recovering from log.
        // Messages from peers/client will not be interleaved, as they can't reference this process' System until after new().
        let log_data = fs::metadata(&logname).expect("Error in System constructor. Could not retrieve metadata from disk log.");//.unwrap_or_else(|e| exit_err(e, 2));
        if log_data.len() > 0 {
            let log_entries = BufReader::new(&disk_log);
            for entry in log_entries.lines() {
                let line = entry.unwrap();
                let msg = serde_json::from_str(&line).expect("Error in System constructor. Encountered invalid json while recovering from log."); // json -> msg object
                s.handle_message(&msg);
            }
        }
        s.disk_log = Some(disk_log);
        // Return a reference to the system.
        s
    }

    // Takes in messages from clients: create fname | delete fname | read fname | append fname value
    // Invokes ZAB
    // Forwards client message to the leader as a p2p message with the related data.
    pub fn handle_client_message(&mut self, m: ClientToServerMessage) -> (Vec<(<Self as HandleMessage>::Pid, <Self as HandleMessage>::Message)>, Vec<ServerToClientMessage>) {
        let mut peer_messages = vec![];
        let mut client_messages = vec![];

        match m {
            ClientToServerMessage::Create(filename) => {
                peer_messages.extend(self.broadcast.broadcast(&SystemRequestMessage::Create(filename)));
            },
            ClientToServerMessage::Delete(filename) => {
                peer_messages.extend(self.broadcast.broadcast(&SystemRequestMessage::Delete(filename)));
            },
            ClientToServerMessage::Append(filename, data) => {
                peer_messages.extend(self.broadcast.broadcast(&SystemRequestMessage::Append(filename, data)));
            },
            ClientToServerMessage::Read(filename) => {
                if let Some(content) = self.files.get(&filename) {
                    client_messages.push(ServerToClientMessage::HumanDisplay(format!("Received data from {}: {}",filename, content)));
                } else {
                    client_messages.push(ServerToClientMessage::HumanDisplay(format!("{} does not exist.", filename)));
                }
            }
        }
        (peer_messages, client_messages)
    }
}

// Message enums:
// Used to distinguish message types between different parties

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerToClientMessage {
    HumanDisplay(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientToServerMessage {
    Create(String),
    Delete(String),
    Append(String, String),
    Read(String),
}

// (create | delete | append)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SystemRequestMessage {
    Create(String),
    Delete(String),
    Append(String, String),
}

impl<B: BroadcastAlgorithm<UnderlyingMessage=SystemRequestMessage>> System<B> {
    fn handle_sysrequest(&mut self, msg: SystemRequestMessage) {
        match msg {
            SystemRequestMessage::Create(filename) => {
                self.files.entry(filename).or_insert("".into());
            },
            SystemRequestMessage::Delete(filename) => {
                self.files.remove(&filename);
                // TODO - error handling (file does not exist)
            },
            SystemRequestMessage::Append(filename, data) => {
                if let Some(content) = self.files.get_mut(&filename) {
                    content.push_str(data.as_str());
                } else {
                    // TODO - error handling
                }
            },
        }
    }
    fn drain_receiver(&mut self) {
        while let Ok(msg) = self.receiver.try_recv() {
            // Edit virtual log as specified:
            self.handle_sysrequest(msg);
        }
    }
}

// HandleMessage:
// Message bookkeeping interface - receives message data, pulls new message data for the handler to send/
// Returns to_send***
impl<B: BroadcastAlgorithm<UnderlyingMessage=SystemRequestMessage>> HandleMessage for System<B> where
    B::Message: Clone + Serialize + Deserialize {
    type Pid = B::Pid;
    type Message = B::Message;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> {
        if let Some(ref mut disk_log) = self.disk_log {
            // Add message to virtual log.
            self.log.push(m.clone());

            // Add message to disk log. (AT THE MOMENT - do so after every message, rather than flushing the log periodically.)
            // Possible TODO - update ofr efficiency.
            write!(disk_log, "{}\n", serde_json::to_string(&m).unwrap())
                .expect("Error in System::HandleMessage. Could not write to disk."); // msg object -> json s
        }
        // Deliver message (edit files) as necessary.
        let to_send = self.broadcast.handle_message(m);
        // TODO: persist ZAB state (recovery happens wrong without it due to msg_count being reset)
        self.drain_receiver();
        to_send
    }
}
