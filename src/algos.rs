// Algos.rs (TODO - rename)
// Functionality for maintaining, updating filesystem. 
// Also helps determine what message content should be broadcast to whom.

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::mpsc;
use broadcasts::BroadcastAlgorithm;
use std::collections::{HashMap};
use serde_json;


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
pub struct System<B:BroadcastAlgorithm<UnderlyingMessage = PeerToPeerMessage>> {
	broadcast: B, // broadcast algorithm, to access/send messages
    files: HashMap<String, String>, // Maps filename -> filecontent
    receiver: mpsc::Receiver<PeerToPeerMessage>, 
    log: Vec<PeerToPeerMessage>, // Virtual log of messages sent/recieved since last save-to-disk. Maps 
    disk_log: File,// File loaded on startup, written version of virtual log
}

// Implementation of System constructor, message handler (which updates the filesystem conditionally)
impl<B:BroadcastAlgorithm<UnderlyingMessage = PeerToPeerMessage>> System<B> { 
	 pub fn new(broadcast: B) -> System<B>{ 
    	let(transmit, receive) = mpsc::channel();
    	let logname = format!("log{}.txt", 0); // TODO - PID
    	let file = OpenOptions::new().append(true).read(true).create(true).open(logname).expect("Error in System constructor, could not open log file on disk.");

    	let mut s = System {
    		broadcast: broadcast,
			files: HashMap::new(),
			receiver: receive,
			log: Vec::new(),
			disk_log: file,
		};
		s.broadcast.set_on_deliver(Box::new(move |msg|{
			transmit.send(msg.clone()).unwrap();
			}));
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
				peer_messages.extend(self.broadcast.broadcast(&PeerToPeerMessage::Create(filename)));
			},
			ClientToServerMessage::Delete(filename) => {
				peer_messages.extend(self.broadcast.broadcast(&PeerToPeerMessage::Delete(filename)));
			},
			ClientToServerMessage::Append(filename, data) => {
				peer_messages.extend(self.broadcast.broadcast(&PeerToPeerMessage::Append(filename, data)));
			},
			ClientToServerMessage::Read(filename) => {
				if let Some(content) = self.files.get(&filename){
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

pub enum ServerToClientMessage {
	HumanDisplay(String),
}

pub enum ClientToServerMessage {
	Create(String),
	Delete(String),
	Append(String, String),
	Read(String),
}

// (create | delete | append)
#[derive(Clone, Serialize, Deserialize)]
pub enum PeerToPeerMessage {
	Create(String),
	Delete(String),
	Append(String, String),
}

// HandleMessage: 
// Message bookkeeping interface - receives message data, pulls new message data for the handler to send/
// Returns to_send***
impl<B: BroadcastAlgorithm<UnderlyingMessage=PeerToPeerMessage>> HandleMessage for System<B> {
	type Pid = B::Pid;
	type Message = B::Message;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> {
    	// Deliver message (edit files) as necessary. 
    	let to_send = self.broadcast.handle_message(m);
    	while let Ok(msg) = self.receiver.try_recv(){
    		// Add message to virtual log. 
    		self.log.push(msg.clone());

    		// Add message to disk log. (AT THE MOMENT - do so after every message, rather than flushing the log periodically.)
    		// Possible TODO - update ofr efficiency. 
    		write!(self.disk_log, "{}\n", serde_json::to_string(&msg).unwrap()).expect("Error in System::HandleMessage. Could not write to disk.");

    		// Edit virtual log as specified: 
    		match msg{
    			PeerToPeerMessage::Create(filename) => {
    				self.files.entry(filename).or_insert("".into());		
					// Write log to disk
    			},
    			PeerToPeerMessage::Delete(filename) => {
    				self.files.remove(&filename);
    				// TODO - error handling (file does not exist)
    			},
    			PeerToPeerMessage::Append(filename, data) => {
    				if let Some(content) = self.files.get_mut(&filename){
    					content.push_str(data.as_str());
    				} else {
    					// TODO - error handling
    				}

    			},
    		}
    	}

    	to_send
    }
}