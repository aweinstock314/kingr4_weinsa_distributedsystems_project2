# Planning session 1
Three futures listening over:
-	Incoming connections from peers - TCP
-	Incoming connections from clients - TCP
-	Server internal messages - Futures::Stream::Channel or stream built on std::sync::mpsc::channel

In order to send messages or update application state, peer & client connections send internal messages to the third (future listener)
Ticks/Heartbeats (http://yager.io/Distributed/Distributed.html) can be implemented by having an additional thread sending ticks to the third. 

Have a type for application messages that composes underlying protocols & possibly has some administrative messages of its own. (Create, delete, etc.) 

Abstract algo logic into functions that return vectors of {PID, message} intead of sending. 


For the testing framework, the idea is that each protocol has a generic `(handleMessage :: Message -> m [(Pid, Message)])`, as well as a few protocol-specific entry points (e.g. `lock :: m [(Pid, Message)]`) (where m indicates access to some protocol-specific local state).
A test script would be be a list of events like `(ProtocolSpecificEntry a | AdvanceTime | RestrictPerTimeStepChannelCapacity (Pid, Pid) Int | Crash Pid | Recover Pid | ...)`.
Interpreting a test script could be done by making a `HashMap<Pid, ProtocolState>` for the global state, and a `HashMap<(Pid,Pid), (usize, Vec<Message>)>` for the channels, and then updating these appropriately via `handle_message` for each message.
`ProtocolSpecificEntry` may just be an escape hatch into some sort of closure that returns a vector of {PID, Message}.

e.g. testing
```rust
pub trait MutexAlgorithm<Resource, Message> {
    fn request(&mut self) -> (mpsc::Receiver<Resource>, Vec<(Pid, Message)>);
    fn release(&mut self) -> Vec<(Pid, Message)>;
    fn handle_message(&mut self, &PeerContext, Message) -> Vec<(Pid, Message)>;
}
```
may look like:
```rust
run_script([
    ProtocolSpecificEntry(|states| {
        let (recv, tosend) = states[0].request();
        drop(recv)
        tosend
    }),
    AdvanceTime,
    ProtocolSpecificEntry(|state| states[0].release()),
    AdvanceTime,
    ], [pid1_state, pid2_state]
);
```
# Planning session 2
## types/architecture
### All parts:
- client -> server msgs: create fname | delete fname | read fname | append fname value | exit (possibly? might be able to gracefully handle dropped sockets)
- server -> client msgs: humandisplay string
- ZAB msgs: proposal (epoch, counter) | ack | commit (epoch, counter) | recover (details depend on part of project)

### Assume-no-failures initial part:
- peer -> peer msgs (if sessions are atomic): broadcast of list of client -> server msgs
- peer -> peer msgs (if sessions are non-atomic): broadcast of individual (create | delete | append)

### Follower failure tolerance:
- add heatbeats to detect failures (p2p msgs unioned with "heartbeat" literal)
- replace broadcast with ZAB in P2P msgs
- give ZAB recovery a "dummy" leader election protocol that hard-codes the initial leader?

### Leader failure tolerance:
- Make ZAB recovery use proper leader election algorithm

## general notes
Seperate client program that sends json blobs of the "client -> server msg" type in response to text on stdin

Find out whether client sessions are supposed to be atomic (e.g. if A issues "create foo; delete foo", and B issues "create foo", is it valid to put B's create in the middle of A's session, and give B an "already exists" error).

Generalize project1 `ApplicationMessage{Reader,Writer}` to Serde types, use for client/server msgs.

Need networking code for all the parts. (Should networking code be build with heartbeats in mind to start with?)
## specific action-items
- create client (networking code and CLI interface)
- create server-side networking code
- translate some of the above types to code, as needed
- implement filesystem struct/msg handler (polymorphic over broadcast)
- come up with leader election trait & dummy leader election protocol
- implement ZAB (polymorphic over leader election)
- implement real leader election protocol (Bully algo, probably)

## schedule-ish?
- Avi should try to get most of the networking in the first week or two
- Rachel should try to fill out the filesystem struct and associated types over the first week
- Avi should make the leader election trait/dummy protocol after the networking code (end of 2nd week)
- Rachel can implement ZAB over the LE trait in the 2nd/3rd week
- Bully algo by whoever has more time at the end
