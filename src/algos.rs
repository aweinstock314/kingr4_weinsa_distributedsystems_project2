// HandleMessage trait for protocols that can handle messages
pub trait HandleMessage {
    type Pid;
    type Message;
    fn handle_message(&mut self, m: &Self::Message) -> Vec<(Self::Pid, Self::Message)>;
}

/*
struct FileSystem {
    files: HashMap<String, String>
    // also have bcast impl, peers, etc
}

// TODO: client to server enum

impl FileSystem {
    fn process( /* takes client2server msg */ ) -> /* (peer2peer pids & msgs, server2client msgs) */ {
    }
}
*/

// also have impl of HandleMessage for peer2peer things
