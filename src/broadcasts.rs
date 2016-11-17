// Broadcasts.rs 
// Broadcast functionality - interfaces with networking level.
// Contains pointwise sendall, methods for sending different types of messages 

//for algorithimic message handling (ZAB, etc) 

use algos::HandleMessage;
use std::collections::HashSet;
use std::hash::Hash;
use std::marker::PhantomData;


// Trait = interface 
// Define: message set, sequence track 
//   Deliver when: recv commit, counter is "in the right order"
// Anything that implements broadcast also needs to handlemessage
// Subtrait of handlemessage, implementations of Broadcast need to also implement HM
pub trait BroadcastAlgorithm: HandleMessage {
    type UnderlyingMessage;
    fn set_on_deliver(&mut self, Box<FnMut(&Self::UnderlyingMessage)>);
    fn broadcast(&mut self, &Self::UnderlyingMessage) -> Vec<(Self::Pid, Self::Message)>;
}

// pointwise sendall implementation
pub struct SendAll<Pid, Msg> {
    deliver: Option<Box<FnMut(&Msg)>>,
    ownpid: Pid,
    neighbors: HashSet<Pid>,
    _msgtype: PhantomData<Msg>,
}

// Impl define struct methods
impl<Pid, Msg> SendAll<Pid, Msg> {
    pub fn new(ownpid: Pid, neighbors: HashSet<Pid>) -> SendAll<Pid, Msg> {
        SendAll {
            deliver: None,
            ownpid: ownpid,
            neighbors: neighbors,
            _msgtype: PhantomData,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SendAllMessage<Pid, Msg> {
    sender: Pid,
    underlying: Msg,
}

// For sendall
// Takes broadcast and message as arguments, returns a vector of pid/message tuples 
// implementation of handlemessage for broadcast algo
impl<Pid, Msg> HandleMessage for SendAll<Pid, Msg> {
    type Pid = Pid;
    type Message = SendAllMessage<Pid, Msg>;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> {
        if let Some(ref mut f) = self.deliver {
            f(&m.underlying);
        }
        vec![]
    }
}

impl<Pid: Eq+Hash+Copy, Msg: Clone> BroadcastAlgorithm for SendAll<Pid, Msg> {
    type UnderlyingMessage = Msg;
    fn set_on_deliver(&mut self, f: Box<FnMut(&Self::UnderlyingMessage)>) {
        self.deliver = Some(f);
    }
    fn broadcast(&mut self, m: &Self::UnderlyingMessage) -> Vec<(Self::Pid, Self::Message)> {
        let ownpid = self.ownpid;
        self.neighbors.iter().map(move |&neighbor| { 
            let msg = SendAllMessage {
                sender: ownpid,
                underlying: m.clone(),
            };
            (neighbor, msg)
        }).collect()
    }
}


// struct for ZAB/FIFO that stores relevant state stuff (counters, etc.) 
//      process counter to send on messages
//      ack counter
//      next queue (of process counters)
//      process queue (to deliver FIFO)
   // processes: HashMap<String, String>, // Processes to communicate with
    //leader: usize // leader PID 

//need default impl for broadcast (zab)

// whatever zab broadcast handling logic function: 
    // Takes peer to peer messages and returns confirmation for server2client 
    //   peer -> peer msgs (if sessions are non-atomic): broadcast of individual (create | delete | append)
        // If leader:
        //      If message is ack, and ack counter is >= total processes/2:
        //          send commit to all in processes
        //      If message is ack, and ack counter is <= total processes/2:
        //          ackcounter++
        //      If message is direct send from a peer:
        //          broadcast:send( system:getSendAll(content)) 
        // Else:
        //      If message is sendall from leader:
        //          store in message queue 
        //          return ack
        //      If message is commit from leader: (assume that message has already been delivered?)
        //          loop through message queue to see if messagecounter is next
        //          if next, deliver message (filesystem::update)
//client message handling for zab: 
//      // leader = FileSystem:GetLeader()
        //      If leader is not self:
        //          fwd message to leader using broadcast:sendmessage
        //      Else:
        //          loop through result of system:getall and broadcast:send to each (or broadcast:sendall message, since that already deals with it?)