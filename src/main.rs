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

pub use argparse::{ArgumentParser, Store};
pub use bincode::SizeLimit;
pub use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
pub use either::Either;
pub use futures::stream::Stream;
pub use futures::{Async, Future, Poll};
pub use futures_cpupool::CpuPool;
pub use nom::IResult;
pub use serde::{Serialize, Deserialize};
pub use std::collections::{HashMap, HashSet, VecDeque};
pub use std::error::Error;
pub use std::fs::File;
pub use std::io::BufReader;
pub use std::io::prelude::*;
pub use std::net::{IpAddr, SocketAddr};
pub use std::str::FromStr;
pub use std::sync::{mpsc, Mutex, MutexGuard};
pub use std::{fmt, io, mem, net, str, thread};
pub use std::iter::Iterator;
pub use tokio_core::io::{FramedIo, Io, ReadHalf, WriteHalf};
pub use tokio_core::net::{TcpListener, TcpStream};
pub use tokio_core::reactor::{Core, Handle};

pub type Pid = usize;
pub type Nodes = HashMap<Pid, (SocketAddr, u16)>;

pub mod algos;
pub mod broadcasts;
pub mod framing_helpers;
pub mod parsers;
#[cfg(test)] mod tests;

pub use algos::*;
pub use framing_helpers::*;
pub use parsers::*;

pub struct Unfold<F, T, Fut>(F, Option<Either<T, Fut>>);
impl<F: FnMut(T) -> Fut, Fut: Future<Item=(T, Option<U>), Error=E>, T, U, E> Stream for Unfold<F, T, Fut> {
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
            Ok(Async::Ready((state, res))) => {
                self.1 = Some(Either::Left(state));
                Ok(Async::Ready(res))
            },
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
pub fn unfold<F, T, Fut>(initial: T, f: F) -> Unfold<F, T, Fut> {
    Unfold(f, Some(Either::Left(initial)))
}

fn channel<T: 'static+Send>() -> (mpsc::Sender<T>, impl Stream<Item=T, Error=mpsc::RecvError>, Box<Future<Item=(), Error=futures::stream::SendError<T, mpsc::RecvError>>+Send>) {
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
}

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
}

fn main() {
    env_logger::init().expect("Failed to initialize logging framework.");

    let mut pid: Pid = 0;
    let mut nodes_fname: String = "nodes.txt".into();
    {
        let nodes_descr = format!("File to load the node hosts/ports from (default {})", nodes_fname);
        let mut ap = ArgumentParser::new();
        ap.set_description("Filesystem backed by Zookeeper's Atomic Broadcast by Rachel King and Avi Weinstock for Distributed Systems class");
        ap.refer(&mut pid).add_argument("pid", Store, "This node's process id").required();
        ap.refer(&mut nodes_fname).add_option(&["-n", "--nodes-file"], Store, &nodes_descr);
        ap.parse_args_or_exit();
    }
    debug!("{}, {}", pid, nodes_fname);

    // (pid -> ip) mapping
    let nodes = run_parser_on_file(&nodes_fname, parse_nodes).expect(&format!("Couldn't parse {}", nodes_fname));
    debug!("nodes: {:?}", nodes);
    // (ip -> pid) mapping
    let nodes_rev: HashMap<SocketAddr, Pid> = nodes.iter().map(|(&k, &v)| (v.0, k)).collect();
    debug!("nodes_rev: {:?}", nodes_rev);

    let own_addr = nodes.get(&pid).expect(&format!("Couldn't find an entry for pid {} in {} ({:?})", pid, nodes_fname, nodes));
    debug!("own_addr: {:?}", own_addr);

    let (transmit, receive, forward) = channel();

    let cpupool = CpuPool::new(4);
    let forwarder = cpupool.spawn(forward);

    let mut core = Core::new().expect("Failed to initialize event loop.");

    struct ControlThread {
        handle: Handle,
        transmit: mpsc::Sender<ControlMessage>,
        client_id: usize,
        client_readers: HashMap<usize, SerdeFrameReader<LengthPrefixedReader<TcpStream>, ClientToServerMessage, Vec<u8>>>,
        client_writers: HashMap<usize, SerdeFrameWriter<LengthPrefixedWriter<TcpStream>, ServerToClientMessage, Vec<u8>>>,
        client_wqueue: VecDeque<ServerToClientMessage>,
    }

    impl ControlThread {
        fn client_send(&mut self, pid: usize, m: ServerToClientMessage) {
            if let Some(w) = self.client_writers.remove(&pid) {
                /*let fut = futures::lazy(move || futures::finished(w));
                let fut = fut.and_then(move |w| {
                    println!("Hello");
                    write_frame(w, m)
                });*/
                let fut = write_frame(w,m);
                let transmit = self.transmit.clone();
                let fut = fut.and_then(move |w| {
                    transmit.send(ControlMessage::FinishedClientWrite(pid, w)).unwrap();
                    Ok(())
                });
                self.handle.spawn(fut.map_err(|_| ()));
            } else {
                self.client_wqueue.push_back(m);
            }
        }
    }

    let controlthread = {
        let mut ct = ControlThread {
            handle: core.handle(),
            transmit: transmit.clone(),
            client_id: 0,
            client_readers: HashMap::new(),
            client_writers: HashMap::new(),
            client_wqueue: VecDeque::new(),
        };
        receive.for_each(move |controlmsg| {
            println!("got controlmsg: {:?}", controlmsg);
            match controlmsg {
                ControlMessage::P2PStart(s) => {}
                ControlMessage::ClientStart(sock) => {
                    let split = split_sock(sock);
                    let cid = ct.client_id;
                    ct.client_id += 1;
                    let transmit = ct.transmit.clone();
                    let fut = split.and_then(move |(r, w)| {
                        //let r: SerdeFrameReader<LengthPrefixedReader<TcpStream>, ClientToServerMessage, Vec<u8>> = r;
                        //let w: SerdeFrameWriter<LengthPrefixedWriter<TcpStream>, ServerToClientMessage, Vec<u8>> = w;
                        println!("1");
                        transmit.send(ControlMessage::NewClient(cid, (r, w))).unwrap();
                        Ok(())
                        //Ok((r, write_frame(w, ServerToClientMessage::HumanDisplay("Hello Client!".into()))))
                    });
                    ct.handle.spawn(fut.map(|_| ()).map_err(|_| ()));
                    //handle.spawn(tokio_core::io::write_all(sock, b"Hello Client\n").map(|_| ()).map_err(|_| ()));
                },
                ControlMessage::NewClient(pid, (r, w)) => {
                    ct.client_readers.insert(pid, r);
                    ct.client_writers.insert(pid, w);
                    ct.client_send(pid, ServerToClientMessage::HumanDisplay("Hello Client!".into()));
                },
                ControlMessage::FinishedClientWrite(pid, w) => {
                    ct.client_writers.insert(pid, w);
                    if let Some(m) = ct.client_wqueue.pop_front() {
                        ct.client_send(pid, m);
                    }
                }
            }
            Ok(())
        })
    };

    let p2p_bindaddr = SocketAddr::new(IpAddr::from_str("0.0.0.0").unwrap(), own_addr.0.port());
    let p2p_listener = TcpListener::bind(&p2p_bindaddr, &core.handle()).expect("Failed to bind listener.");
    debug!("Listening for peer connections on {:?}", p2p_bindaddr);
    let p2p = {
        let handle = core.handle();
        let transmit = transmit.clone();
        p2p_listener.incoming().for_each(move |(sock, peer)| {
            trace!("Got a connection from {:?}", peer);
            try!(transmit.send(ControlMessage::P2PStart(format!("{:?}", peer))).map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
            handle.spawn(tokio_core::io::write_all(sock, b"Hello Peer\n").map(|_| ()).map_err(|_| ()));
            Ok(())
        })
    };

    let client_bindaddr = SocketAddr::new(IpAddr::from_str("0.0.0.0").unwrap(), own_addr.1);
    let client_listener = TcpListener::bind(&client_bindaddr, &core.handle()).expect("Failed to bind listener.");
    debug!("Listening for client connections on {:?}", client_bindaddr);
    let client = {
        let handle = core.handle();
        client_listener.incoming().for_each(move |(sock, peer)| {
            trace!("Got a connection from {:?}", peer);
            try!(transmit.send(ControlMessage::ClientStart(sock)).map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
            Ok(())
        })
    };


    let combinedfuture = p2p.map_err(|e| {
            warn!("p2p listener error: {:?}", e);
        }).join(client.map_err(|e| {
            warn!("client listener error: {:?}", e);
        })).join(forwarder.map_err(|e| {
            warn!("forwarder error: {:?}", e);
        })).join(controlthread.map_err(|e| {
            warn!("controlthread error: {:?}", e);
        }));
    core.run(combinedfuture).expect("Failed to run event loop.");
}

enum ControlMessage {
    P2PStart(String),
    ClientStart(TcpStream),
    NewClient(usize, (SerdeFrameReader<LengthPrefixedReader<TcpStream>, ClientToServerMessage, Vec<u8>>,
                      SerdeFrameWriter<LengthPrefixedWriter<TcpStream>, ServerToClientMessage, Vec<u8>>)),
    FinishedClientWrite(usize, SerdeFrameWriter<LengthPrefixedWriter<TcpStream>, ServerToClientMessage, Vec<u8>>),
}

unsafe impl Sync for ControlMessage {}

impl fmt::Debug for ControlMessage {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ControlMessage::P2PStart(ref s) => fmt.debug_tuple("P2PStart").field(s).finish(),
            &ControlMessage::ClientStart(ref t) => fmt.debug_tuple("ClientStart").field(t).finish(),
            &ControlMessage::NewClient(ref pid, _) => fmt.debug_tuple("NewClient").field(pid).finish(),
            &ControlMessage::FinishedClientWrite(ref pid, _) => fmt.debug_tuple("FinishedClientWrite").field(pid).finish(),
        }
    }
}
