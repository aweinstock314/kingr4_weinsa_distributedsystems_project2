use algos::HandleMessage;
use std::collections::HashSet;
use std::hash::Hash;
use std::marker::PhantomData;

pub trait BroadcastAlgorithm: HandleMessage {
    type UnderlyingMessage;
    fn set_on_deliver(&mut self, Box<FnMut(&Self::UnderlyingMessage)>);
    fn broadcast(&mut self, &Self::UnderlyingMessage) -> Vec<(Self::Pid, Self::Message)>;
}

pub struct SendAll<Pid, Msg> {
    deliver: Option<Box<FnMut(&Msg)>>,
    ownpid: Pid,
    neighbors: HashSet<Pid>,
    _msgtype: PhantomData<Msg>,
}

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
