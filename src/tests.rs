use algos::HandleMessage;
use broadcasts::{BroadcastAlgorithm, SendAll};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

// TestScriptEntry and run_script form a testing framework
enum TestScriptEntry<S: HandleMessage> {
    Closure(Box<Fn(&mut HashMap<S::Pid, &mut S>) -> Vec<(S::Pid, S::Message)>>),
    AdvanceRound,
}

fn run_script<S: HandleMessage>(script: &[TestScriptEntry<S>], states: &mut HashMap<S::Pid, &mut S>) where
    S::Pid: Debug + Clone + Hash + Eq, S::Message: Debug + Clone {
    let mut tosend = vec![];
    for entry in script { match entry {
        &TestScriptEntry::Closure(ref f) => {
            tosend.extend_from_slice(&f(states));
            println!("Current state of tosend: {:?}", tosend);
        },
        &TestScriptEntry::AdvanceRound => {
            let mut new_tosend = vec![];
            for (pid, msg) in tosend.drain(..) {
                let mut state = states.get_mut(&pid).expect(&format!("No such pid: {:?}", pid));
                let msgs = state.handle_message(&msg);
                new_tosend.extend_from_slice(&msgs);
            }
            tosend = new_tosend;
        }
    }}
}

// PingPong{State,Message} form a simple protocol to demonstrate the testing framework
struct PingPongState {
    pid: usize,
    pingcounter: usize,
    pongcounter: usize,
}
#[derive(Debug, Clone)]
enum PingPongMessage { Ping(usize), Pong }

impl PingPongState {
    fn new(pid: usize) -> PingPongState {
        PingPongState {
            pid: pid, pingcounter: 0, pongcounter: 0,
        }
    }
}

impl HandleMessage for PingPongState {
    type Pid = usize;
    type Message = PingPongMessage;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> {
        println!("{}: Got a {:?}", self.pid, m);
        match m {
            &PingPongMessage::Ping(replypid) => {
                println!("{}: replying to {}", self.pid, replypid);
                self.pingcounter += 1;
                vec![(replypid, PingPongMessage::Pong)]
            },
            &PingPongMessage::Pong => {
                self.pongcounter += 1;
                vec![]
            },
        }
    }
}

#[test]
fn ping_pong_protocol() {
    let mut state1 = PingPongState::new(1);
    let mut state2 = PingPongState::new(2);
    let mut states = HashMap::new();
    states.insert(1, &mut state1);
    states.insert(2, &mut state2);
    let makeassert = |a,b,c,d| { TestScriptEntry::Closure(Box::new(move |s: &mut HashMap<usize, &mut PingPongState>| {
        let s1 = s.get(&1).unwrap();
        let s2 = s.get(&2).unwrap();
        assert_eq!(s1.pingcounter, a);
        assert_eq!(s1.pongcounter, b);
        assert_eq!(s2.pingcounter, c);
        assert_eq!(s2.pongcounter, d);
        vec![]
    }))};
    run_script::<PingPongState>({
        use self::TestScriptEntry::*;
        &[
        Closure(Box::new(|_| vec![(2, PingPongMessage::Ping(1))])),
        makeassert(0,0,0,0),
        AdvanceRound,
        makeassert(0,0,1,0),
        AdvanceRound,
        makeassert(0,1,1,0),
        ]}, &mut states);
}

#[test]
fn test_sendall() {
    let mut state1 = PingPongState::new(1);
    let mut state2 = PingPongState::new(2);
    let mut state3 = PingPongState::new(3);

    let mut bcast1 = SendAll::new(1usize, [2,3].into_iter().map(|&i| i).collect());
    let mut bcast2 = SendAll::new(2usize, [1,3].into_iter().map(|&i| i).collect());
    let mut bcast3 = SendAll::new(3usize, [1,2].into_iter().map(|&i| i).collect());
    bcast1.set_on_deliver(Box::new(move |m| { state1.handle_message(m); }));
    bcast2.set_on_deliver(Box::new(move |m| { state2.handle_message(m); }));
    bcast3.set_on_deliver(Box::new(move |m| { state3.handle_message(m); }));

    let mut states = HashMap::new();
    states.insert(1, &mut bcast1);
    states.insert(2, &mut bcast2);
    states.insert(3, &mut bcast3);
    run_script(&[
        TestScriptEntry::Closure(Box::new(|states: &mut HashMap<usize, &mut SendAll<_,_>>| {
            states.get_mut(&1).unwrap().broadcast(&PingPongMessage::Ping(1))
        })),
        TestScriptEntry::AdvanceRound,
    ], &mut states);
}
