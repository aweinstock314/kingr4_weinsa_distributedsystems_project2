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

fn channel<T: 'static+Send>() -> (mpsc::Sender<T>, impl Stream<Item=T, Error=mpsc::RecvError>, Box<Future<Item=(), Error=futures::stream::SendError<T, mpsc::RecvError>>>) {
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

    let mut core = Core::new().expect("Failed to initialize event loop.");
    let bindaddr = SocketAddr::new(IpAddr::from_str("0.0.0.0").unwrap(), own_addr.0.port());
    let listener = TcpListener::bind(&bindaddr, &core.handle()).expect("Failed to bind listener.");
    let helloserver = {
        let handle = core.handle();
        debug!("Listening on {:?}", bindaddr);
        listener.incoming().for_each(move |(sock, peer)| {
            trace!("Got a connection from {:?}", peer);
            handle.spawn(tokio_core::io::write_all(sock, b"Hello\n").map(|_| ()).map_err(|_| ()));
            Ok(())
        })
    };
    core.run(helloserver).expect("Failed to run event loop.");
}
