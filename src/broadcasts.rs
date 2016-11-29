// Broadcasts.rs 
// Broadcast functionality - interfaces with networking level.
// Contains pointwise sendall, methods for sending different types of messages 

//for algorithimic message handling (ZAB, etc) 

use algos::HandleMessage;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Debug;
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

// Leader election functionality. Uses Bully Algorithm:
// PIDs are arbitrary, leaders are semi-arbitrary. FUTURE COMPLEXITY TODO - leader preference based on log size
// Will always elect highest available PID.
#[derive(Debug)]
pub struct BullyState<Pid: Eq+Hash> {
    own_pid: Pid,
    pub leader_pid: Option<Pid>, // This being None implies holding_election = true
    tick_counter: usize,
    recvd_okay: bool,
    recvd_coord: bool,
    processes: HashSet<Pid>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BullyMessage<Pid> {
    pub sender: Pid,
    pub mtype: BullyTypes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BullyTypes {
    Election, // init leader election
    Coordinator, // from new leader, to update others' leader PID
    Okay, // ack election 
    Tick, // to increment internal counter when waiting on Okay
}

impl<Pid: Debug+Eq+Hash+Copy+Ord> HandleMessage for BullyState<Pid> {
    type Pid = Pid;
    type Message = BullyMessage<Pid>;
    
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> { 
        if let BullyTypes::Tick = m.mtype {} else {
            debug!("BullyState::handle_message({:?}, {:?})", self, m);
        }
        match m.mtype {
            BullyTypes::Tick => {
                return self.tick();
            },
            // on receipt of election acknowledgement 
            BullyTypes::Okay => {
                self.recvd_okay = true;
            },
            // on receipt of election notification
            BullyTypes::Election => {
                let msg = BullyMessage { sender: self.own_pid, mtype: BullyTypes::Okay };
                let mut tosend = vec![(m.sender, msg.clone())];
                if let None = self.leader_pid {
                    tosend.extend(self.init());
                }
                return tosend;
            },
            // on receipt of coord message from new leader
            BullyTypes::Coordinator => {
                self.recvd_coord = true;
                self.leader_pid = Some(m.sender);
            },
        }
        vec![]
    }
}

impl<Pid: Debug+Eq+Hash+Copy+Ord> BullyState<Pid> {
    pub fn new(pid:Pid, processes:HashSet<Pid>, initial_leader:Pid) -> BullyState<Pid> {
        let b = BullyState {
            own_pid: pid,
            processes: processes,
            leader_pid: None,
            recvd_coord: false,
            recvd_okay: false,
            tick_counter: 0,
        };
        b
    }

    // initalize an election - send Election message to processes with higher Pids
    //  To be invoked when a leader election is started (when a heartbeat detection fails)
    pub fn init(&mut self) -> Vec<(Pid, BullyMessage<Pid>)> {
        info!("BullyState::init({:?})", self);
        self.recvd_coord = false;
        self.recvd_okay = false;
        self.tick_counter = 0;
        self.leader_pid = None;
        let m = BullyMessage { sender: self.own_pid, mtype: BullyTypes::Election };
        self.processes.iter().filter_map(|&pid| if pid > self.own_pid {Some((pid, m.clone()))} else {None}).collect()
    }

    pub fn tick(&mut self) -> Vec<(Pid, BullyMessage<Pid>)> {
        if let None = self.leader_pid {
            self.tick_counter += 1;
            // T time units
            if self.tick_counter >= 1 {
                // if haven't heard from anyone else, assume nobody higher exists to be leader. send coord to (lower) others. 
                if self.recvd_okay == false {
                    self.leader_pid = Some(self.own_pid);
                    let msg = BullyMessage { sender: self.own_pid, mtype: BullyTypes::Coordinator };
                    return self.processes.iter().filter_map(|&pid| if pid < self.own_pid {Some((pid, msg.clone()))} else {None}).collect();
                } 
            }
            // T' time units
            if self.tick_counter >= 2 {
                // if it hits this, it should have recieved a coord message, but did not. start a new election.
                if self.recvd_coord == false { 
                    return self.init();
                }
            }
        }
        vec![]
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Serialize, Deserialize, PartialEq)]
pub struct Zxid {
    epoch: usize, // Incremented after every leader election. 
    counter: usize, // A counter sent along with each message, to ensure that messages are delivered in FIFO order. Unique to each epoch. 
}

impl Zxid {
    pub fn new() -> Zxid {
        Zxid {
            epoch: 0,
            counter: 0,
        }
    }
}
// ZAB struct: Stores bookkeeping data for Zookeeper Atomic Broadcast HandleMessage implementation.
pub struct Zab<Pid:Eq+Hash, Msg> {
    zxid: Zxid,
    ack_count: HashMap<Zxid, usize>, // a counter of acknowledgments recieved from peers <zxid, ackcount>
    next_msg: Zxid, // next_msg = (e, c) => the next expected message has zxid (e, c)
    msg_q: HashMap<Zxid, Msg>, // Queued messages, waiting to be delievered in broadcast FIFO order. Will be delivered when next_msg.pop() = msg.counter
    pub leader: BullyState<Pid>, // stored PID of leader process 
    own_pid: Pid,
    processes: HashSet<Pid>,
    deliver: Box<FnMut(&Msg)>,
}

impl<Pid: Debug+Eq+Hash, Msg: Debug> Debug for Zab<Pid, Msg> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Zab")
            .field("zxid", &self.zxid)
            .field("ack_count", &self.ack_count)
            .field("next_msg", &self.next_msg)
            .field("msg_q", &self.msg_q)
            .field("leader", &self.leader)
            .field("own_pid", &self.own_pid)
            .field("processes", &self.processes)
            .finish()
    }
}

impl<Pid: Debug+Copy+Eq+Hash+Ord, Msg> Zab<Pid, Msg> {
    pub fn new(processes: HashSet<Pid>, deliver: Box<FnMut(&Msg)>, initial_leader: Pid, own_pid: Pid) -> Zab<Pid, Msg> {
        Zab {
            zxid: Zxid::new(),
            ack_count: HashMap::new(),
            next_msg: Zxid::new(),
            msg_q: HashMap::new(),
            leader: BullyState::new(own_pid, processes.clone(), initial_leader),
            own_pid: own_pid,
            processes: processes,
            deliver: deliver,
        }
    }
}

impl<Pid:Clone+Copy+Eq+Hash, Msg:Clone> Zab<Pid, Msg> {
    fn internal_broadcast(&mut self, initiator: Pid, z: ZabTypes<Pid, Msg>) -> Vec<(Pid, ZabMessage<Pid,Msg>)> {
        let m = ZabMessage { sender: self.own_pid, initiator: initiator, mtype: z, count: self.zxid };
        self.processes.iter().map(|pid| (*pid, m.clone())).collect()
    }
}


// Message being used in ZAB
// Contains a senderPid, an underlying message, and a message type (ack or commit)
// UnderlyingMessage encapsulates the message content to be delivered ***

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZabMessage<Pid, Message> {
    pub sender: Pid, // sender of this specific message
    pub initiator: Pid, // sender of this chain of messages (i.e. who the client connected to)
    pub mtype: ZabTypes<Pid, Message>,
    pub count: Zxid,
}

// enum of message types for ZAB
// processes send acknowledgment or commit 

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZabTypes<Pid, Message> {
    Forwarded(Message),
    SendAll(Message),
    Commit,
    Ack,
    Election(BullyMessage<Pid>),
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

impl<Pid: Eq+Hash+Copy+Debug+Ord, Msg: Clone+Debug> HandleMessage for Zab<Pid, Msg> {
    type Pid = Pid;
    type Message = ZabMessage<Pid, Msg>;

    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> {
        if let ZabTypes::Election(BullyMessage {mtype: BullyTypes::Tick,..}) = m.mtype {} else {
            debug!("In Zab::handle_message\ncurrent state: {:?}\nmessage: {:?}", self, m);
        }

        let mut to_send = Vec::new();

        // If it recieves a leader election protocol, delegate
        if let ZabTypes::Election(ref underlying) = m.mtype {
            let was_holding_election = self.leader.leader_pid == None;
            let bully_messages = self.leader.handle_message(underlying);
            let still_holding_election = self.leader.leader_pid == None;

            let election_finished = was_holding_election && !still_holding_election;
            /*let mut election_finished = false;
            if let BullyTypes::Coordinator = underlying.mtype {
                election_finished = true;
            }
            election_finished |= bully_messages.iter().any(|&(_, ref m)| {
                if let BullyTypes::Coordinator = m.mtype {
                    true
                } else {
                    false
                }
            });*/

            to_send.extend(bully_messages.into_iter().map(|(pid, m_)| (pid, ZabMessage {sender: self.own_pid, initiator: m.initiator,  mtype: ZabTypes::Election(m_), count: self.zxid} )));
            if election_finished {
                info!("Election finished, new leader is {:?}", self.leader.leader_pid);
                self.zxid.epoch += 1;
                self.zxid.counter = 0;
                self.next_msg = self.zxid;
            }
            // Don't interrupt the current election
            if let None = self.leader.leader_pid {
                return to_send;
            }
        } 
        match self.leader.leader_pid { // if there's no leader, hold an election
            None => {
                self.leader.init().into_iter().map(|(pid, m)| {
                    (pid, ZabMessage {sender: self.own_pid, initiator: self.own_pid, mtype: ZabTypes::Election(m), count: self.zxid} )
                }).collect() 
            },
            Some(leader) => {
                if self.own_pid == leader {
                    // Manage ack from processes.
                    if let ZabTypes::Ack = m.mtype {
                        let ac = {
                            let ac = self.ack_count.entry(m.count).or_insert(0);
                            *ac += 1;
                            *ac 
                        };
                        if ac > (self.processes.len()/2) + 1 { // TODO - double check majority arithmetic?     
                            // send commit to all, including self. (will follow protocol below) 
                            to_send.extend(self.internal_broadcast(m.initiator, ZabTypes::Commit));
                        }
                        // if we've recieved Ack from everyone (except the leader) for this message, we can save memory by cleaning up the ack_count entry
                        if ac == self.processes.len() - 1 {
                            self.ack_count.remove(&m.count);
                        }
                    } 
                    // Broadcast SendAll message to followers if the message has been forwarded from the client.
                    if let ZabTypes::Forwarded(ref underlying) = m.mtype {
                        self.msg_q.insert(m.count, underlying.clone());
                        to_send.extend(self.internal_broadcast(m.initiator, ZabTypes::SendAll(underlying.clone())));
                    }
                } else {
                    // If process is not the leader, manage FIFO & commit-based delivery locally.
                    if let ZabTypes::SendAll(ref underlying) = m.mtype {
                        // store in message queue
                        self.msg_q.insert(m.count, underlying.clone());

                        // send ack to leader 
                        to_send.extend(self.internal_broadcast(m.initiator, ZabTypes::Ack));
                    }
                }
                // Both leader and follower processes respond to commit
                if let ZabTypes::Commit = m.mtype { 
                    // loop through msg_q to find the next counter.
                    // if it matches the expected next message for its sender, invoke deliver.
                    while let Some(u) = self.msg_q.remove(&self.next_msg) {
                        (self.deliver)(&u);
                        self.next_msg.counter += 1;
                    }
                    self.zxid.counter += 1; // advance the local message counter on commit 
                }
                if let ZabTypes::Election(BullyMessage {mtype: BullyTypes::Tick,..}) = m.mtype {} else {
                    debug!("State at end of Zab::handle_message: {:?}", self);
                }
                to_send
            }
        }
    }
}

impl<Pid: Eq+Hash+Copy+Debug+Ord, Msg: Clone+Debug> BroadcastAlgorithm for Zab<Pid, Msg> {
    type UnderlyingMessage = Msg;
    fn set_on_deliver(&mut self, f: Box<FnMut(&Self::UnderlyingMessage)>) {
        self.deliver = f;
    }
    fn broadcast(&mut self, m: &Self::UnderlyingMessage) -> Vec<(Self::Pid, Self::Message)> {
        if let Some(leader) = self.leader.leader_pid {
            vec![(leader, ZabMessage { sender: self.own_pid, initiator: self.own_pid, mtype: ZabTypes::Forwarded(m.clone()), count: self.zxid })]
        } else { // if there's no leader, hold an election
            self.leader.init().into_iter().map(|(pid, m)| (pid, ZabMessage {sender: self.own_pid, initiator: self.own_pid, mtype: ZabTypes::Election(m), count: self.zxid} )).collect()
        }
    }
}
