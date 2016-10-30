use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

// HandleMessage trait for protocols that can handle messages
trait HandleMessage {
    type Pid;
    type Message;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)>;
}

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
struct PingPongState { pid: usize }
#[derive(Debug, Clone)]
enum PingPongMessage { Ping(usize), Pong }

impl HandleMessage for PingPongState {
    type Pid = usize;
    type Message = PingPongMessage;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)> {
        println!("{}: Got a {:?}", self.pid, m);
        if let &PingPongMessage::Ping(replypid) = m {
            println!("{}: replying to {}", self.pid, replypid);
            vec![(replypid, PingPongMessage::Pong)]
        } else {
            vec![]
        }
    }
}

#[test]
fn ping_pong_protocol() {
    let mut state1 = PingPongState { pid: 1 };
    let mut state2 = PingPongState { pid: 2 };
    let mut states = HashMap::new();
    states.insert(1, &mut state1);
    states.insert(2, &mut state2);
    run_script::<PingPongState>({
        use self::TestScriptEntry::*;
        &[
        Closure(Box::new(|_| vec![(2, PingPongMessage::Ping(1))])),
        AdvanceRound,
        AdvanceRound,
        ]}, &mut states);
}
