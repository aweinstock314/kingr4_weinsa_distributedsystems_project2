//FOR updating filesystem
use std::sync::mpsc;
use broadcasts::BroadcastAlgorithm;
use std::collections::{HashMap};

pub trait HandleMessage {
    type Pid;
    type Message;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)>; //destination + message for output of handlemessage 
}

// To draw from for message sending, etc.
// Implemented as a data structure for ease of testing, portability on EC2 instances 
//template over whatever broadcast it's using, then use that to invoke broadcast
// has map of peer IDs for reference 
// TODO: client to server enum
// An enum in Rust is a type that represents data that is one of several possible variants. Each variant in the enum can optionally have data associated with it
// Connections list for broadcast? 
// Filesystem feeds pid/file data to algo for handling 
// Used by control thread (main)
pub struct System<B:BroadcastAlgorithm<UnderlyingMessage = PeerToPeerMessage>> {
	broadcast: B, 
    files: HashMap<String, String>, // Files that exist in the system - <name, content> 
    receiver: mpsc::Receiver<PeerToPeerMessage>,
}


impl<B:BroadcastAlgorithm<UnderlyingMessage = PeerToPeerMessage>> System<B> { //"Default" for constructors w.o args
	 pub fn new(broadcast: B) -> System<B>{ //constructor so we define the delivery (trigger update)
    	let(transmit, receive) = mpsc::channel(); // channel as a "local socket". used instead of mutex, similar concept
    	let mut s = System {
    		broadcast: broadcast,
			files: HashMap::new(),
			receiver: receive,
		};
		s.broadcast.set_on_deliver(Box::new(move |msg|{
			transmit.send(msg.clone()).unwrap();
			}));
		s
    }

	// Takes in messages from clients 
	//		create fname | delete fname | read fname | append fname value 
	// 		Invokes ZAB
	//			Forwards client message to the leader as a p2p message with the related data.
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
#[derive(Clone)]
pub enum PeerToPeerMessage {
	Create(String),
	Delete(String),
	Append(String, String),
}

// Message bookkeeping interface - receives message data, pulls new message data for the handler to send
// Can distinguish between message types at network level
impl<B: BroadcastAlgorithm<UnderlyingMessage=PeerToPeerMessage>> HandleMessage for System<B> {
	type Pid = B::Pid;
	type Message = B::Message;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> {
    	//if create, put a file in system, etc
    	let to_send = self.broadcast.handle_message(m);
    	while let Ok(msg) = self.receiver.try_recv(){
    		match msg{
    			PeerToPeerMessage::Create(filename) => {
    				self.files.entry(filename).or_insert("".into());
    			},
    			PeerToPeerMessage::Delete(filename) => {
    				self.files.remove(&filename);
    				// TODO - error handling (file does not exist)

    			},
    			PeerToPeerMessage::Append(filename, data) => {
    				if let Some(content) = self.files.get_mut(&filename){
    					content.push_str(data.as_str());
    				} else{
    					// TODO - error handling
    				}

    			},
    		}
    	}

    	to_send
    }
}