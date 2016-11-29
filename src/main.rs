#![feature(conservative_impl_trait)]
#![feature(proc_macro)]

pub extern crate argparse;
pub extern crate bincode;
pub extern crate byteorder;
pub extern crate either;
pub extern crate env_logger;
#[macro_use] pub extern crate futures;
pub extern crate futures_cpupool;
#[macro_use] pub extern crate lazy_static;
#[macro_use] pub extern crate log;
#[macro_use] pub extern crate nom;
pub extern crate serde;
#[macro_use] pub extern crate serde_derive;
pub extern crate serde_json;
pub extern crate tokio_core;

pub use argparse::{ArgumentParser, Collect, Store};
pub use bincode::SizeLimit;
pub use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
pub use either::Either;
pub use futures::stream::Stream;
pub use futures::sync::mpsc as fmpsc;
pub use futures::{Async, Future, IntoFuture, Poll, Sink};
pub use futures_cpupool::CpuPool;
pub use nom::IResult;
pub use serde::{Serialize, Deserialize};
pub use std::collections::{HashMap, HashSet, VecDeque};
pub use std::error::Error;
pub use std::fs::File;
pub use std::io::BufReader;
pub use std::io::prelude::*;
pub use std::iter::Iterator;
pub use std::net::{IpAddr, SocketAddr};
pub use std::str::FromStr;
pub use std::sync::{mpsc, Mutex, MutexGuard};
pub use std::time::Duration;
pub use std::{fmt, io, mem, net, str, thread};
pub use tokio_core::io::{Framed, Io, ReadHalf, WriteHalf};
pub use tokio_core::net::{TcpListener, TcpStream};
pub use tokio_core::reactor::{Core, Handle, Timeout};

pub type Pid = usize;
pub type Nodes = HashMap<Pid, (SocketAddr, u16)>;

pub mod algos;
pub mod broadcasts;
pub mod framing_helpers;
pub mod parsers;
#[cfg(test)] mod tests;

pub use algos::*;
pub use broadcasts::*;
pub use framing_helpers::*;
pub use parsers::*;

pub struct Unfold<F, T, Fut>(F, Option<Either<T, Fut>>);
impl<F: FnMut(T) -> Fut, Fut: Future<Item=Option<(T, U)>, Error=E>, T, U, E> Stream for Unfold<F, T, Fut> {
    type Item = U;
    type Error = E;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut f = &mut self.0;
        let tmp = mem::replace(&mut self.1, None).expect("Unfold.1 was None");
        let mut fut = tmp.either(
            move |state| f(state),
            move |fut| fut
        );
        match fut.poll() {
            Ok(Async::Ready(Some((state, res)))) => {
                self.1 = Some(Either::Left(state));
                Ok(Async::Ready(Some(res)))
            },
            Ok(Async::Ready(None)) => {
                Ok(Async::Ready(None))
            }
            Ok(Async::NotReady) => {
                self.1 = Some(Either::Right(fut));
                Ok(Async::NotReady)
            }
            Err(e) => {
                Err(e)
            },
        }
    }
}
pub fn unfold<F: FnMut(T) -> Fut, Fut: Future<Item=Option<(T, U)>, Error=E>, T, U, E>(initial: T, f: F) -> Unfold<F, T, Fut> {
    Unfold(f, Some(Either::Left(initial)))
}

/*fn channel<T: 'static+Send>() -> (mpsc::Sender<T>, impl Stream<Item=T, Error=mpsc::RecvError>, Box<Future<Item=(), Error=futures::stream::SendError<T, mpsc::RecvError>>+Send>) {
    let (t1, r1) = mpsc::channel();
    let (t2, r2) = futures::stream::channel();
    let forwarder = unfold(t2, move |sender: futures::stream::Sender<T, mpsc::RecvError>| {
        match r1.recv() {
            Ok(msg) => sender.send(Ok(msg)).map(|newsender| (newsender, Some(()))).boxed(),
            e @ Err(mpsc::RecvError) => sender.send(e).map(|newsender| (newsender, None)).boxed(),
        }
    });
    let forwarder = forwarder.for_each(|()| Ok(()));
    (t1, r2, forwarder.boxed())
}*/

/*type ApplicationSource<T> = SerdeFrameReader<LengthPrefixedReader<TcpStream>, T, Vec<u8>>;
type ApplicationSink<T> = SerdeFrameWriter<LengthPrefixedWriter<TcpStream>, T, Vec<u8>>;

fn split_sock<D: Deserialize, S: Serialize>(sock: TcpStream) ->
    impl Future<Item=(SerdeFrameReader<LengthPrefixedReader<TcpStream>, D, Vec<u8>>,
                      SerdeFrameWriter<LengthPrefixedWriter<TcpStream>, S, Vec<u8>>),
                Error=io::Error> {
    let rw = futures::lazy(move || {
        futures::finished(sock.split())
    });
    let rw = rw.map(|(r, w)| (
        SerdeFrameReader::new(LengthPrefixedReader::new(r, SizeLimit::Bounded(0x10000))),
        SerdeFrameWriter::new(LengthPrefixedWriter::new(w))
    ));
    rw
}*/
type ApplicationSource<T, U> = futures::stream::SplitStream<Framed<TcpStream, PostcomposeCodec<SerdeJSONCodec<T, U>, COBSCodec>>>;
type ApplicationSink<T, U> = futures::stream::SplitSink<Framed<TcpStream, PostcomposeCodec<SerdeJSONCodec<T, U>, COBSCodec>>>;

fn split_sock<S: Serialize, D: Deserialize>(sock: TcpStream) ->
    (futures::stream::SplitStream<Framed<TcpStream, PostcomposeCodec<SerdeJSONCodec<S, D>, COBSCodec>>>,
    futures::stream::SplitSink<Framed<TcpStream, PostcomposeCodec<SerdeJSONCodec<S, D>, COBSCodec>>>) {
    let codec = postcompose_codec(SerdeJSONCodec::new(), COBSCodec::new(b'\n'));
    let (w, r) = sock.framed(codec).split();
    (r, w)
}

/*fn make_ticker<F: FnMut() -> Fut, Fut: IntoFuture<Item=(), Error=io::Error>>(h: &Handle, dur: Duration, f: F) -> Box<Future<Item=(), Error=io::Error>> where
    <Fut as IntoFuture>::Future: Send {
    /*Timeout::new(dur, h).into_future().and_then(|t| { t.and_then(|t| {
        f().into_future().and_then(|()| make_ticker(h, dur, f))
    })});*/
    unfold(Timeout::new(dur, h), |t| {
        t.into_future().and_then(|t| { t.and_then(move |t| {
            f().into_future().and_then(|()| {
                Ok(Some((Timeout::new(dur, h), ())))
            })
        })})
    }).for_each(|()| Ok(())).boxed()
}*/

fn main() {
    env_logger::init().expect("Failed to initialize logging framework.");
    let args: Vec<String> = std::env::args().collect();
    debug!("Main args: {:?}", args);
    let mut subcommand: String = "".into();
    let mut nodes_fname: String = "nodes.txt".into();
    let mut subargs = vec![];
    {
        let nodes_descr = format!("File to load the node hosts/ports from (default {})", nodes_fname);
        let mut ap = ArgumentParser::new();
        ap.stop_on_first_argument(true);
        ap.set_description("Filesystem backed by Zookeeper's Atomic Broadcast by Rachel King and Avi Weinstock for Distributed Systems class");
        ap.refer(&mut subcommand).add_argument("subcommand", Store, "{client,server}").required();
        ap.refer(&mut nodes_fname).add_option(&["-n", "--nodes-file"], Store, &nodes_descr);
        ap.refer(&mut subargs).add_argument("args", Collect, "Arguments for subcommand");
        if let Err(code) = ap.parse(args.clone(), &mut io::stdout(), &mut io::stderr()) {
            std::process::exit(code);
        }
    }
    let mut newargs = vec![subcommand.clone()];
    newargs.extend(subargs);
    match &*subcommand {
        "client" => client_main(newargs, nodes_fname),
        "server" => server_main(newargs, nodes_fname),
        s => {
            println!("subcommand must be either 'client' or 'server' ({:?} is invalid)", s);
        }
    }
}

fn client_main(args: Vec<String>, nodes_fname: String) {
    debug!("Client args: {:?}", args);
    let mut pid: Pid = 0;
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Subcommand for connecting to servers");
        ap.refer(&mut pid).add_argument("pid", Store, "Process ID of the server to connect to").required();
        if let Err(code) = ap.parse(args, &mut io::stdout(), &mut io::stderr()) {
            std::process::exit(code);
        }
    }

    // (pid -> ip) mapping
    let nodes = run_parser_on_file(&nodes_fname, parse_nodes).expect(&format!("Couldn't parse {}", nodes_fname));
    debug!("nodes: {:?}", nodes);

    let server_info = nodes.get(&pid).expect(&format!("Couldn't find an entry for pid {} in {} ({:?})", pid, nodes_fname, nodes));
    let server_addr = SocketAddr::new(server_info.0.ip(), server_info.1);
    debug!("server_addr: {:?}", server_addr);

    let (transmit, receive) = fmpsc::channel::<ClientToServerMessage>(10);

    thread::spawn(move || {
        let mut core = Core::new().expect("Failed to initialize event loop.");

        let server = TcpStream::connect(&server_addr, &core.handle());

        let handle = core.handle();
        let fut = server.and_then(move |sock| {
            let (r, w) = split_sock(sock);
            let r = r.map_err(|e| { println!("An error occurred: {:?}", e) });
            handle.spawn(r.for_each(|m| {
                match m {
                    ServerToClientMessage::HumanDisplay(s) => println!("Got message: {}", s),
                }
                Ok(())
            }));
            w.send_all(receive.map_err(|()| str_to_ioerror("hello"))).and_then(|_| {
                Ok(())
            })
        }).map_err(|e| {
            warn!("Error in TCP loop: {:?}", e);
            e
        });
        core.run(fut).expect("Failed to run event loop.");
    });

    let print_help = || {
        println!("Options:");
        println!("create filename");
        println!("delete filename");
        println!("append filename newline_terminated_data");
        println!("read filename");
    };
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    loop {
        let mut line = "".into();
        let _ = stdout.flush();
        print!("client> ");
        let _ = stdout.flush();
        if let Err(e) = stdin.read_line(&mut line) {
            warn!("Error reading from stdin: {:?}", e);
            break;
        };
        debug!("line was {:?}", line);
        if let Some(o1) = line.find(|c: char| c.is_whitespace()) {
            let i1 = o1+1;
            let cmd = &line[..i1-1];
            trace!("cmd: {:?}", cmd);
            if let Some(o2) = line[i1..].find(|c: char| c.is_whitespace()) {
                let i2 = i1 + o2 + 1;
                let filename = &line[i1..i2-1];
                trace!("filename: {:?}", filename);
                let msg = match cmd {
                    "create" => ClientToServerMessage::Create(filename.into()),
                    "delete" => ClientToServerMessage::Delete(filename.into()),
                    "read" => ClientToServerMessage::Read(filename.into()),
                    "append" => {
                        if let Some(o3) = line[i2..].find('\n') {
                            let i3 = i2 + o3 + 1;
                            let data = &line[i2..i3-1];
                            trace!("data: {:?}", data);
                            ClientToServerMessage::Append(filename.into(), data.into())
                        } else {
                            continue
                        }
                    },
                    s => {
                        println!("Invalid command: {:?}", s);
                        print_help();
                        continue
                    }
                };
                debug!("msg: {:?}", msg);
                if let Err(e) = transmit.clone().send(msg).wait() {
                    warn!("Error sending across the channel: {:?}", e);
                    break;
                }
            } else {
                print_help();
                continue;
            }
        }
    }
}

fn server_main(args: Vec<String>, nodes_fname: String) {
    debug!("Server args: {:?}", args);
    let mut pid: Pid = 0;
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Subcommand for serving a virtual filesystem and synchronizing with peers");
        ap.refer(&mut pid).add_argument("pid", Store, "This node's process ID").required();
        if let Err(code) = ap.parse(args, &mut io::stdout(), &mut io::stderr()) {
            std::process::exit(code);
        }
    }
    debug!("{}, {}", pid, nodes_fname);

    // (pid -> ip) mapping
    let nodes = run_parser_on_file(&nodes_fname, parse_nodes).expect(&format!("Couldn't parse {}", nodes_fname));
    debug!("nodes: {:?}", nodes);
    // (ip -> pid) mapping
    let nodes_rev: HashMap<SocketAddr, Pid> = nodes.iter().map(|(&k, &v)| (v.0, k)).collect();
    debug!("nodes_rev: {:?}", nodes_rev);

    let own_addr = *nodes.get(&pid).expect(&format!("Couldn't find an entry for pid {} in {} ({:?})", pid, nodes_fname, nodes));
    debug!("own_addr: {:?}", own_addr);

    /*let (transmit, receive, forward) = channel();

    let cpupool = CpuPool::new(4);
    let forwarder = cpupool.spawn(forward);*/
    let (transmit, receive) = fmpsc::channel(1000);

    let mut core = Core::new().expect("Failed to initialize event loop.");

    let ticker = {
        let transmit = transmit.clone();
        //let f = move || { transmit.send(ControlMessage::Tick).map(|_| ()).map_err(|e| io::Error::new(io::ErrorKind::Other, e)) };
        let dur = Duration::from_millis(500);
        let h = core.handle();
        //make_ticker(&core.handle(), dur, f)
        unfold((Timeout::new(dur, &h.clone()), h, transmit), move |(t, h, transmit)| {
            t.into_future().and_then(move |t| { t.and_then(move |_| {
                /*f().into_future().and_then(move |()| {
                    Ok(Some(((Timeout::new(dur, &h.clone()), h, f), ())))
                })*/
                transmit.clone().send(ControlMessage::Tick).map(|_| ()).map_err(|e| io::Error::new(io::ErrorKind::Other, e)).and_then(move |()| {
                    Ok(Some(((Timeout::new(dur, &h.clone()), h, transmit), ())))
                })
            })})
        }).for_each(|()| Ok(()))

    };
    core.handle().spawn(ticker.map_err(|e| {
        warn!("Error in ticker: {:?}", e);
    }));

    struct ControlThread {
        handle: Handle,
        transmit: fmpsc::Sender<ControlMessage>,
        client_id: usize,
        peer_pids: HashSet<usize>,
        peer_heartbeats_and_writers: HashMap<usize, (usize, fmpsc::Sender<PeerToPeerMessage>)>,
        //peer_wqueue: VecDeque<PeerToPeerMessage>,
        client_writers: HashMap<usize, ApplicationSink<ServerToClientMessage, ClientToServerMessage>>,
        client_wqueue: VecDeque<ServerToClientMessage>,
        filesystem: System<Zab<usize, SystemRequestMessage>>,
        nodes: Nodes,
        ownpid: usize,
    }

    impl ControlThread {
        fn client_send(&mut self, pid: usize, m: ServerToClientMessage) -> Box<Future<Item=(),Error=()>+Send> {
            if let Some(w) = self.client_writers.remove(&pid) {
                let fut = w.send(m);
                let transmit = self.transmit.clone();
                let fut = fut.and_then(move |w| {
                    transmit.send(ControlMessage::FinishedClientWrite(pid, w)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                });
                //self.handle.spawn(fut.map_err(|_| ()));
                fut.map(|_| ()).map_err(|_| ()).boxed()
            } else {
                self.client_wqueue.push_back(m);
                futures::finished(()).boxed()
            }
        }
        fn insert_writer(&mut self, pid: Pid, w: ApplicationSink<PeerToPeerMessage, PeerToPeerMessage>, errmsg: &'static str) {
            let (tx, rx) = fmpsc::channel(10);
            /*if let Some((heartbeat, old_tx)) = ct.peer_heartbeats_and_writers.insert(pid, (0, tx)) {
                trace!("already had an entry for peer {} with heartbeat {}", pid, heartbeat);
                // TODO: how should this be handled without leaking the old socket/channel?
            }*/
            self.peer_heartbeats_and_writers.entry(pid).or_insert((0, tx));
            self.handle.spawn(w.send_all(rx.map_err(move |()| str_to_ioerror(errmsg))).map(|_| ()).map_err(|_| ()));
        }
        fn send_p2p(&self, pid: Pid, m: PeerToPeerMessage) {
            if let Some(&(_, ref sender)) = self.peer_heartbeats_and_writers.get(&pid) {
                let transmit = self.transmit.clone();
                self.handle.spawn(sender.clone().send(m.clone()).map(|_| ()).or_else(move |e| {
                    warn!("Error sending p2p({:?}) message: {:?}", m, e);
                    transmit.send(ControlMessage::ConsiderDead(pid)).map(|_| ()).map_err(|_| unreachable!())
                }));
            } else {
                // TODO: this branch could be hit if there's a disconnection window, maybe this 
                //  should do retry logic if pid is in (peer_heartbeats_and_writers - processes)?
                warn!("Algorithm tried to send {:?} to nonexistant pid {}", m, pid)
            }
        }
        fn send_election(&self, pid: Pid, m: BullyMessage<Pid>) {
            self.send_p2p(pid, PeerToPeerMessage::System(ZabMessage {
                sender: self.ownpid,
                initiator: self.ownpid,
                mtype: ZabTypes::Election(m),
                count: Zxid::new(),
            }));
        }
    }

    let controlthread = {
        const HARDCODED_LEADER: Pid = 1;
        let processes: HashSet<Pid> = nodes.iter().map(|(&k, _)| k).collect();
        let deliver = Box::new(|_: &SystemRequestMessage| {});
        let zab = Zab::new(processes.clone(), deliver, HARDCODED_LEADER, pid);
        let mut ct = ControlThread {
            handle: core.handle(),
            transmit: transmit.clone(),
            client_id: 0,
            peer_pids: processes,
            nodes: nodes,
            peer_heartbeats_and_writers: HashMap::new(),
            client_writers: HashMap::new(),
            client_wqueue: VecDeque::new(),
            filesystem: System::new(zab, pid),
            ownpid: pid,
        };
        receive.for_each(move |controlmsg| {
            match controlmsg {
                ControlMessage::Tick | ControlMessage::P2P(_, PeerToPeerMessage::HeartbeatPing) =>
                    trace!("Got controlmsg: {:?}", controlmsg),
                _ => debug!("Got controlmsg: {:?}", controlmsg),
            }

            match controlmsg {
                ControlMessage::P2PStart(sock, maybe_theirpid) => {
                    let transmit = ct.transmit.clone();
                    let buf = vec![0; 8];
                    //let ownpid = ct.ownpid;
                    let fut = if let Some(theirpid) = maybe_theirpid {
                        info!("Made a peer connection to pid {}", theirpid);
                        let fut = Ok((sock, theirpid));
                        fut.into_future().boxed()
                    } else {
                        let fut = tokio_core::io::read_exact(sock, buf);
                        let fut = fut.and_then(move |(sock, buf)| {
                            let theirpid = LittleEndian::read_u64(&buf[0..8]) as usize;
                            info!("Got a peer connection from pid {}", theirpid);
                            //LittleEndian::write_u64(&mut buf[0..8], ownpid as u64);
                            //tokio_core::io::write_all(sock, buf).map(move |(sock, _)| (sock, theirpid)).boxed()
                            Ok((sock, theirpid))
                        });
                        fut.boxed()
                    };
                    let fut = fut.and_then(|(sock, theirpid)| {
                        transmit.send(ControlMessage::NewPeer(theirpid, split_sock(sock))).map(|_| ()).map_err(|_| unreachable!())
                    });
                    ct.handle.spawn(fut.map_err(|e| {
                        warn!("Error in P2PStart: {:?}", e);
                    }));
                },
                ControlMessage::ClientStart(sock) => {
                    let split = split_sock(sock);
                    let cid = ct.client_id;
                    ct.client_id += 1;
                    let transmit = ct.transmit.clone();
                    let fut = futures::finished(split).and_then(move |(r, w)| {
                        //Ok((r, write_frame(w, ServerToClientMessage::HumanDisplay("Hello Client!".into()))))
                        transmit.send(ControlMessage::NewClient(cid, (r, w))).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                    });
                    ct.handle.spawn(fut.map(|_| ()).map_err(|_| ()));
                    //handle.spawn(tokio_core::io::write_all(sock, b"Hello Client\n").map(|_| ()).map_err(|_| ()));
                },
                ControlMessage::NewClient(pid, (r, w)) => {
                    //ct.client_readers.insert(pid, r);
                    let transmit = ct.transmit.clone();
                    /*let tstream = futures::stream::unfold(transmit, |t| Some(Ok((t.clone(), t))));
                    let readerstream = r.zip(tstream).and_then(move |(m, t)| {
                        t.send(ControlMessage::C2S(m)).then(move |x| {
                            if let Err(e) = x {
                                println!("Error sending a C2S: {:?}", e);
                                return Err(str_to_ioerror("foo"));
                            }
                            Ok(())
                        })
                    }).map_err(|_| ()).for_each(|_| Ok(()));*/
                    let readerstream = r.into_future().and_then(move |r| {
                        unfold((transmit, r), move |(t, (r_cur, r_next))| {
                            if let Some(m) = r_cur {
                                let f = t.send(ControlMessage::C2S(pid, m)).then(move |x| {
                                    match x {
                                        Err(e) => {
                                            warn!("Error sending a C2S: {:?}", e);
                                            Ok(None).into_future().boxed()
                                        },
                                        Ok(t) => {
                                            r_next.into_future().and_then(|(a,b)| Ok(Some( ((t, (a,b)), ()) ))).into_future().boxed()
                                        }
                                    }
                                }).boxed();
                                f
                            } else {
                                Ok(None).into_future().boxed()
                            }
                        }).for_each(|_| Ok(()))
                    }).map(|_| ());
                    ct.handle.spawn(readerstream.map_err(|_| ()));
                    ct.client_writers.insert(pid, w);
                    let mut s: String = "Hello Client!".into();
                    for _ in 0..512 { s.push('A'); }
                    let fut = ct.client_send(pid, ServerToClientMessage::HumanDisplay(s));
                    ct.handle.spawn(fut);
                },
                ControlMessage::NewPeer(pid, (r, w)) => {
                    ct.insert_writer(pid, w, "NewPeer (rx -> w)");
                    // TODO: generalize readerstream's construction into a new combinator (mapM-with-threaded-loopstate?)
                    let transmit = ct.transmit.clone();
                    let readerstream = r.into_future().and_then(move |r| {
                        unfold((transmit, r), move |(t, (r_cur, r_next))| {
                            if let Some(m) = r_cur {
                                let f = t.send(ControlMessage::P2P(pid, m.clone())).then(move |x| {
                                    match x {
                                        Err(e) => {
                                            warn!("Error sending a P2P({:?}): {:?}", m, e);
                                            Ok(None).into_future().boxed()
                                        },
                                        Ok(t) => {
                                            r_next.into_future().and_then(|(a,b)| Ok(Some( ((t, (a,b)), ()) ))).into_future().boxed()
                                        }
                                    }
                                }).boxed();
                                f
                            } else {
                                Ok(None).into_future().boxed()
                            }
                        }).for_each(|_| Ok(()))
                    }).map(|_| ());
                    ct.handle.spawn(readerstream.map_err(|_| ()));
                },
                ControlMessage::FinishedClientWrite(pid, w) => {
                    ct.client_writers.insert(pid, w);
                    if let Some(m) = ct.client_wqueue.pop_front() {
                        let fut = ct.client_send(pid, m);
                        ct.handle.spawn(fut);
                    }
                },
                ControlMessage::C2S(pid, msg) => {
                    let (peermsgs, response) = ct.filesystem.handle_client_message(msg);
                    let mut fut = futures::finished(()).boxed();
                    for m in response {
                        let step = ct.client_send(pid, m);
                        fut = fut.and_then(|_| step).boxed();
                    }
                    ct.handle.spawn(fut);
                    for (pid, m) in peermsgs {
                        ct.send_p2p(pid, PeerToPeerMessage::System(m));
                    }
                },
                ControlMessage::P2P(pid, msg) => {
                    match msg {
                        PeerToPeerMessage::System(m) => {
                            let tosend = ct.filesystem.handle_message(&m);
                            debug!("tosend: {:?}", tosend);
                            for (pid, m) in tosend {
                                ct.send_p2p(pid, PeerToPeerMessage::System(m));
                            }
                        },
                        PeerToPeerMessage::HeartbeatPing => {
                            if let Some(&mut (ref mut heartbeat, _)) = ct.peer_heartbeats_and_writers.get_mut(&pid) {
                                trace!("heard from {}, resetting their heartbeat from {} to 0", pid, *heartbeat);
                                *heartbeat = 0;
                            } else {
                                warn!("Recieved a heartbeat from someone ({}) we're not connected to", pid);
                            }
                        },
                    }
                },
                ControlMessage::Tick => {
                    let connected_pids: HashSet<Pid> = ct.peer_heartbeats_and_writers.iter().map(|(&k, _)| k).collect();
                    trace!("num_connected: {}; total: {}", connected_pids.len(), ct.peer_pids.len());
                    trace!("connected_pids: {:?}; peer_pids: {:?}", connected_pids, ct.peer_pids);
                    let to_connect = &ct.peer_pids - &connected_pids;
                    trace!("to_connect: {:?}", to_connect);
                    for &pid in to_connect.iter() {
                        trace!("trying to connect to {:?}", pid);
                        let transmit = ct.transmit.clone();
                        let ownpid = ct.ownpid;
                        let fut = TcpStream::connect(&ct.nodes[&pid].0, &ct.handle);
                        let fut = fut.and_then(move |sock| {
                            let mut buf = vec![0; 8];
                            LittleEndian::write_u64(&mut buf[0..8], ownpid as u64);
                            trace!("writing pid {} towards {}", ownpid, pid);
                            tokio_core::io::write_all(sock, buf).map(|(s, _)| s)
                        });
                        let fut = fut.and_then(move |sock| {
                            transmit.send(ControlMessage::P2PStart(sock, Some(pid))).map_err(|_| unreachable!())
                        });
                        ct.handle.spawn(fut.map(|_| ()).map_err(move |e| {
                            warn!("Error connecting to peer {:?}: {:?}", pid, e);
                        }));
                    }
                    for (&pid, &mut (ref mut heartbeat, ref mut sender)) in ct.peer_heartbeats_and_writers.iter_mut() {
                        trace!("incrementing heartbeat for {} (old value: {})", pid, *heartbeat);
                        *heartbeat += 1;
                        if *heartbeat % 3 == 0 {
                            trace!("pinging {} (heartbeat {})", pid, *heartbeat);
                            let transmit = ct.transmit.clone();
                            ct.handle.spawn(sender.clone().send(PeerToPeerMessage::HeartbeatPing).map(|_| ()).or_else(move |e| {
                                warn!("Error sending heartbeat: {:?}", e);
                                transmit.send(ControlMessage::ConsiderDead(pid)).map(|_| ()).map_err(|_| unreachable!())
                            }));
                        }
                        if *heartbeat % 10 == 0 {
                            info!("haven't heard from {} in 10 ticks, considering them dead", pid);
                            ct.handle.spawn(ct.transmit.clone().send(ControlMessage::ConsiderDead(pid)).map(|_| ()).map_err(|_| unreachable!()));
                        }
                    }
                    let m = BullyMessage {
                        sender: ct.ownpid,
                        mtype: BullyTypes::Tick,
                    };
                    /*ct.send_election(ct.ownpid, m);*/
                    ct.filesystem.broadcast.leader.handle_message(&m);
                },
                ControlMessage::ConsiderDead(pid) => {
                    ct.peer_heartbeats_and_writers.remove(&pid);
                    if let Some(leader) = ct.filesystem.broadcast.leader.leader_pid {
                        if leader == pid {
                            let to_send = ct.filesystem.broadcast.leader.init();
                            for (pid, m) in to_send {
                                ct.send_election(pid, m);
                            }
                        }
                    }

                },
            }
            Ok(())
        })
    };

    let p2p_bindaddr = SocketAddr::new(IpAddr::from_str("0.0.0.0").unwrap(), own_addr.0.port());
    let p2p_listener = TcpListener::bind(&p2p_bindaddr, &core.handle()).expect("Failed to bind listener.");
    info!("Listening for peer connections on {:?}", p2p_bindaddr);
    let p2p = {
        let handle = core.handle();
        let transmit = transmit.clone();
        p2p_listener.incoming().for_each(move |(sock, peer)| {
            info!("Got a connection from {:?}", peer);
            //handle.spawn(transmit.send(ControlMessage::P2PStart(format!("{:?}", peer))).map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
            //handle.spawn(tokio_core::io::write_all(sock, b"Hello Peer\n").map(|_| ()).map_err(|_| ()));
            handle.spawn(transmit.clone().send(ControlMessage::P2PStart(sock, None)).map(|_| ()).map_err(|_| ()));
            Ok(())
        })
    };

    let client_bindaddr = SocketAddr::new(IpAddr::from_str("0.0.0.0").unwrap(), own_addr.1);
    let client_listener = TcpListener::bind(&client_bindaddr, &core.handle()).expect("Failed to bind listener.");
    info!("Listening for client connections on {:?}", client_bindaddr);
    let client = {
        let handle = core.handle();
        client_listener.incoming().for_each(move |(sock, peer)| {
            debug!("Got a connection from {:?}", peer);
            //handle.spawn(transmit.send(ControlMessage::ClientStart(sock)).map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
            handle.spawn(transmit.clone().send(ControlMessage::ClientStart(sock)).map(|_| ()).map_err(|_| ()));
            Ok(())
        })
    };


    let combinedfuture = p2p.map_err(|e| {
            warn!("p2p listener error: {:?}", e);
        }).join(client.map_err(|e| {
            warn!("client listener error: {:?}", e);
        }))/*.join(forwarder.map_err(|e| {
            warn!("forwarder error: {:?}", e);
        }))*/.join(controlthread.map_err(|e| {
            warn!("controlthread error: {:?}", e);
        }));
    core.run(combinedfuture).expect("Failed to run event loop.");
}

enum ControlMessage {
    P2PStart(TcpStream, Option<Pid>),
    ClientStart(TcpStream),
    NewClient(Pid, (ApplicationSource<ServerToClientMessage, ClientToServerMessage>,
                      ApplicationSink<ServerToClientMessage, ClientToServerMessage>)),
    NewPeer(Pid, (ApplicationSource<PeerToPeerMessage, PeerToPeerMessage>,
                    ApplicationSink<PeerToPeerMessage, PeerToPeerMessage>)),
    FinishedClientWrite(usize, ApplicationSink<ServerToClientMessage, ClientToServerMessage>),
    C2S(Pid, ClientToServerMessage),
    P2P(Pid, PeerToPeerMessage),
    ConsiderDead(Pid),
    Tick,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PeerToPeerMessage {
    System(ZabMessage<Pid, SystemRequestMessage>),
    HeartbeatPing,
}

impl fmt::Debug for ControlMessage {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ControlMessage::P2PStart(ref s, ref b) => fmt.debug_tuple("P2PStart").field(s).field(b).finish(),
            &ControlMessage::ClientStart(ref t) => fmt.debug_tuple("ClientStart").field(t).finish(),
            &ControlMessage::NewClient(ref pid, _) => fmt.debug_tuple("NewClient").field(pid).finish(),
            &ControlMessage::NewPeer(ref pid, _) => fmt.debug_tuple("NewPeer").field(pid).finish(),
            &ControlMessage::FinishedClientWrite(ref pid, _) => fmt.debug_tuple("FinishedClientWrite").field(pid).finish(),
            &ControlMessage::C2S(ref pid, ref m) => fmt.debug_tuple("C2S").field(pid).field(m).finish(),
            &ControlMessage::P2P(ref pid, ref m) => fmt.debug_tuple("P2P").field(pid).field(m).finish(),
            &ControlMessage::ConsiderDead(ref pid) => fmt.debug_tuple("ConsiderDead").field(pid).finish(),
            &ControlMessage::Tick => fmt.debug_tuple("Tick").finish(),
        }
    }
}
