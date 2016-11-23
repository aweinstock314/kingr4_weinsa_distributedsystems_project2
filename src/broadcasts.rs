// Broadcasts.rs 
// Broadcast functionality - interfaces with networking level.
// Contains pointwise sendall, methods for sending different types of messages 

//for algorithimic message handling (ZAB, etc) 

use algos::HandleMessage;
use std::collections::{HashSet, VecDeque, BTreeSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::collections::{HashMap};


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

// ZAB struct:
// Stores bookkeeping data for Zookeeper Atomic Broadcast HandleMessage implementation.
pub struct Zab<Pid, Msg> {
    msg_count: usize, // a counter sent along with each message, for keeping track of system FIFO. (WHERE IS THIS DEFINED?)
    ack_count: HashMap<usize, usize>, // a counter of acknowledgments recieved from peers <messagecount, ackcount>
    next_msg: BTreeSet<usize>, // FIFO vector of message counters. Top of the queue = next message that should be delivered.
    msg_q: VecDeque<( usize, Msg)>,//VecDeque<ZabMessage<Pid, Msg>>, // Queued messages, waiting to be delievered in broadcast FIFO order. Will be delivered when next_msg.pop() = msg.counter
    leader: Pid, // stored PID of leader process 
    own_pid: Pid,
    processes: HashSet<Pid>,
    deliver: Box<FnMut(&Msg)>,
}

impl<Pid, Msg> Zab<Pid, Msg> {
    pub fn new(processes: HashSet<Pid>, deliver: Box<FnMut(&Msg)>, initial_leader: Pid, own_pid: Pid) -> Zab<Pid, Msg> {
        Zab {
            msg_count: 0,
            ack_count: HashMap::new(),
            next_msg: BTreeSet::new(),
            msg_q: VecDeque::new(),
            leader: initial_leader,
            own_pid: own_pid,
            processes: processes,
            deliver: deliver,
        }
    }
}

impl<Pid:Clone+Copy+Eq+Hash, Msg:Clone> Zab<Pid, Msg> {
    fn internal_broadcast(&mut self, z: ZabTypes<Msg>) -> Vec<(Pid, ZabMessage<Pid,Msg>)> {
        let m = ZabMessage{ sender: self.own_pid, mtype: z, count: self.msg_count };
        self.processes.iter().map(|pid| (*pid, m.clone())).collect()
    }
}


// Message being used in ZAB
// Contains a senderPid, an underlying message, and a message type (ack or commit)
// UnderlyingMessage encapsulates the message content to be delivered ***
#[derive(Clone)]
pub struct ZabMessage<Pid, Message>{
    sender: Pid,
    mtype: ZabTypes<Message>,
    count: usize,
}

// enum of message types for ZAB
// processes send acknowledgment or commit 
#[derive(Clone)]
pub enum ZabTypes<Message>{
    Forwarded(Message),
    SendAll(Message),
    Commit,
    Ack,
    Election,
    Heartbeat,
}

// Zab implementation of HandleMessage:
// Invokes deliver conditionally, based on ZAB broadcast (FIFO RB + 2PC)
// Deliver is then passed to the System struct in algos.rs, which updates the filesystem. 
//---------------------------------------------------------------------------------------
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
    //          if next, deliver message

// TODO - prettify. Match instead of "if let ="
impl<Pid: Eq+Hash+Copy, Msg: Clone> HandleMessage for Zab<Pid, Msg> {
    type Pid = Pid;
    type Message = ZabMessage<Pid, Msg>;

    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> {
        let mut to_send = Vec::new();

        if self.own_pid == self.leader {
            // Manage ack from processes.
            if let ZabTypes::Ack = m.mtype {
                let ac = {
                    let ac = self.ack_count.entry(m.count).or_insert(0);
                    *ac += 1;
                    *ac 
                };
                if ac > (self.processes.len()/2) + 1 { // TODO - double check majority arithmetic?     
                    // send commit to all, including self. (will follow protocol below) 
                    to_send.extend(self.internal_broadcast(ZabTypes::Commit));
                    self.msg_count+=1;
                }
                if ac == self.processes.len() {
                    self.ack_count.remove(&m.count);
                }
            } 
            // Broadcast SendAll message to followers if the message has been forwarded from the client.
            if let ZabTypes::Forwarded(ref underlying) = m.mtype {
                to_send.extend(self.internal_broadcast(ZabTypes::SendAll(underlying.clone())));
                self.msg_count+=1;
            }
        } else {
            // If process is not the leader, manage FIFO & commit-based delivery locally.
            if let ZabTypes::SendAll(ref underlying) = m.mtype {
                // store in message queue
                self.msg_q.push_back((m.count, underlying.clone()));

                // send ack to leader 
                to_send.extend(self.internal_broadcast(ZabTypes::Ack));
                self.msg_count+=1;
            }
            // If a process sends a message to a non-leader peer, it is for leader election.
            // leader election protocol
            if let ZabTypes::Election = m.mtype {
                // TODO
            }
        }
        // Both leder and follower processes respond to commit
        if let ZabTypes::Commit = m.mtype { 
            // loop through msg_q to find the next counter.
            // if matches the front of next, invoke deliver.
            let mut to_subtract = BTreeSet::new(); // collects things to subtract from the next Q 
            while let Some(next) = self.next_msg.iter().next(){
                let mut to_remove = None;
                for (i,&(c, ref u)) in self.msg_q.iter().enumerate() {
                    if c == *next {
                        to_remove = Some(i);
                        to_subtract.insert(*next);
                        (self.deliver)(&u);
                    }
                }
                if let Some(i) = to_remove {
                    self.msg_q.swap_remove_back(i);
                }
            }
            self.next_msg = &self.next_msg - &to_subtract;
        }

        to_send
    }
}



impl<Pid: Eq+Hash+Copy, Msg: Clone> BroadcastAlgorithm for Zab<Pid, Msg> {
    type UnderlyingMessage = Msg;
    fn set_on_deliver(&mut self, f: Box<FnMut(&Self::UnderlyingMessage)>) {
        self.deliver = f;
    }
    fn broadcast(&mut self, m: &Self::UnderlyingMessage) -> Vec<(Self::Pid, Self::Message)> {
        vec![(self.leader, ZabMessage{ sender: self.own_pid, mtype: ZabTypes::Forwarded(m.clone()), count: 0})]
        //TODO tell leader things somehow 
    }
}
