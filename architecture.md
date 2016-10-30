Three futures listening over:
-	Incoming connections from peers - TCP
-	Incoming connections from clients - TCP
-	Server internal messages - Futures::Stream::Channel or stream built on std::sync::mpsc::channel

In order to send messages or update application state, peer & client connections send internal messages to the third (future listener)
Ticks/Heartbeats (http://yager.io/Distributed/Distributed.html) can be implemented by having an additional thread sending ticks to the third. 

Have a type for application messages that composes underlying protocols & possibly has some administrative messages of its own. (Create, delete, etc.) 

Abstract algo logic into functions that return vectors of {PID, message} intead of sending. 


For the testing framework, the idea is that each protocol has a generic (handleMessage :: Message -> m [(Pid, Message)]), as well as a few protocol-specific entry points (e.g. lock :: m [(Pid, Message)]) (where m indicates access to some protocol-specific local state).
A test script would be be a list of events like (ProtocolSpecificEntry a | AdvanceTime | RestrictPerTimeStepChannelCapacity (Pid, Pid) Int | Crash Pid | Recover Pid | ...).
Interpreting a test script could be done by making a HashMap<Pid, ProtocolState> for the global state, and a HashMap<(Pid,Pid), (usize, Vec<Message>)> for the channels, and then updating these appropriately via handle_message for each message.
ProtocolSpecificEntry may just be an escape hatch into some sort of closure that returns a vector of {PID, Message}.

