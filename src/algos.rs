//FOR updating filesystem
use std::sync::mpsc;
use broadcasts::BroadcastAlgorithm;
use std::collections::{HashSet, HashMap};
use std::hash::Hash; 


// Algos.rs 
// Catchall for algo implementation, logic to oversee network & 
// execute ZAB
// "pure code that does all the scheduling stuff"
// Make use of broadcast to get pid/message pairs to broadcast, broad

// HandleMessage trait for protocols that can handle messages
// Compartmentalize: logic for how and when to send and receive data
// Manage queues of messages and filesystem updates to maintain FIFO, two phase broadcast/commit 
// Need explicit indices on messages 


// 2-phase commit + FIFO RB
// BROADCAST of VALUE (v): 
/*
    On broadcast: generate transaction (value, (epoch, counter)) 
        Send proposal to all followers.

    On recv msg (val, (ep, c)):
        Queue val
        Send acknowledgement to leader
        Wait for commit

    On recv ack: 
        (Safety - if leader:) 
            If majority, sendall commit + msg (value, (epoch, counter))

*/
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
pub struct System<B:BroadcastAlgorithm<UnderlyingMessage = PeerToPeerMessage>>{
	broadcast: B, 
    files: HashMap<String, String>, // Files that exist in the system - <name, content> 
    receiver: mpsc::Receiver<PeerToPeerMessage>,
}


impl <B:Default+BroadcastAlgorithm<UnderlyingMessage = PeerToPeerMessage>> System<B>{ //"Default" for constructors w.o args
	 fn new() -> System<B>{ //constructor so we define the delivery (trigger update)
    	let(transmit, receive) = mpsc::channel(); // channel as a "local socket". used instead of mutex, similar concept
    	let mut s = System{
    		broadcast:B::default(),
			files: HashMap::new(),
			receiver: receive,
		};
		s.broadcast.set_on_deliver(Box::new(move |msg|{
			transmit.send(msg.clone()).unwrap();
			}));
		s
    }

    //create/delete/append filename, return confirmatoin in form of "%filename %task" + "ed"
	/*fn update(task, filename) -> message(clientpid, confirmation string)) {

	}

	// Takes in messages from clients 
	//		create fname | delete fname | read fname | append fname value | exit (possibly? might be able to gracefully handle dropped sockets
	// 		Invokes ZAB
	//			Forwards client message to the leader as a p2p message with the related data.
    fn handleClientMessage( /* takes client2server msg */ ) -> /* (peer2peer pids & msgs, server2client msgs) */ {
	
    }*/
}

// (create | delete | append)
#[derive(Clone)]
pub enum PeerToPeerMessage{
	Create(String),
	Delete(String),
	Append(String, String),
}

// Message bookkeeping interface - receives message data, pulls new message data for the handler to send
// Can distinguish between message types at network level
impl<B:BroadcastAlgorithm<UnderlyingMessage = PeerToPeerMessage>> HandleMessage for System<B>{
	type Pid = B::Pid;
	type Message = B::Message;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> {
    	//if create, put a file in system, etc
    	let toSend = self.broadcast.handle_message(m);
    	while let Ok(msg) = self.receiver.try_recv(){
    		match msg{
    			PeerToPeerMessage::Create(filename) => {
    				self.files.entry(filename).or_insert("".into());
    			},
    			PeerToPeerMessage::Delete(filename) =>{
    				self.files.remove(&filename);
    				// TODO - error handling (file does not exist)

    			},
    			PeerToPeerMessage::Append(filename, data) => {
    				if let Some(content) = self.files.get_mut(&filename){
    					content.push_str(data.as_str());
    				}
    				else{
    					// TODO - error handling
    				}

    			},
    		}
    	}
    	
    	toSend
    }
}