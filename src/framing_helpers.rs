use super::*;
//use serde::{Serialize, Deserialize};
use std::marker::PhantomData;

// Potential Future Work: COBS framer: https://en.wikipedia.org/wiki/Consistent_Overhead_Byte_Stuffing
pub struct LengthPrefixedFramerState {
    sofar: usize,
    buf: Vec<u8>,
}
pub struct LengthPrefixedFramer<I> {
    reader: LengthPrefixedReader<I>,
    writer: LengthPrefixedWriter<I>,
}

pub struct LengthPrefixedReader<I> {
    underlying: ReadHalf<I>,
    readstate: Option<LengthPrefixedFramerState>,
    sizebound: SizeLimit,
}
impl<I> LengthPrefixedReader<I> {
    pub fn new(i: ReadHalf<I>, bound: SizeLimit) -> Self {
        LengthPrefixedReader {
            underlying: i,
            readstate: None,
            sizebound: bound,
        }
    }
}

pub struct LengthPrefixedWriter<I> {
    underlying: WriteHalf<I>,
    writestate: Option<LengthPrefixedFramerState>,
}
impl<I> LengthPrefixedWriter<I> {
    pub fn new(i: WriteHalf<I>) -> Self {
        LengthPrefixedWriter {
            underlying: i,
            writestate: None,
        }
    }
}

impl<I: Io> FramedIo for LengthPrefixedReader<I> {
    type In = Vec<u8>;
    type Out = Vec<u8>;
    fn poll_read(&mut self) -> Async<()> {
        self.underlying.poll_read()
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        let mut tmp = mem::replace(&mut self.readstate, None);
        let (restore, result); // borrow checker workaround
        if let Some(ref mut st) = tmp {
            let newbytes = try!(self.underlying.read(&mut st.buf[st.sofar..]));
            st.sofar += newbytes;
            if st.sofar == st.buf.len() {
                restore = false;
                result = Ok(Async::Ready(mem::replace(&mut st.buf, Vec::new())));
            } else {
                restore = true;
                result = Ok(Async::NotReady);
            }
        } else {
            let size = try!(self.underlying.read_u64::<LittleEndian>()) as usize;
            if let SizeLimit::Bounded(maxsize) = self.sizebound {
                if size >= maxsize as usize {
                    warn!("warning: in LengthPrefixedReader, received an input of size {} (bound is {:?})", size, self.sizebound);
                    return Err(io::Error::new(io::ErrorKind::Other, "LengthPrefixedReader: bound exceeded"));
                }
            }
            let mut st = LengthPrefixedFramerState { sofar: 0, buf: vec![0u8; size], };
            let newbytes = try!(self.underlying.read(&mut st.buf[st.sofar..]));
            st.sofar += newbytes;
            if st.sofar == st.buf.len() {
                result = Ok(Async::Ready(mem::replace(&mut st.buf, Vec::new())));
            } else {
                result = Ok(Async::NotReady);
                self.readstate = Some(st);
            }
            restore = false;
        }
        if restore {
            self.readstate = tmp;
        }
        result
    }
    fn poll_write(&mut self) -> Async<()> {
        panic!("poll_write on LengthPrefixedReader");
    }
    fn write(&mut self, _: Self::In) -> Poll<(), io::Error> {
        panic!("write on LengthPrefixedReader");
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        panic!("flush on LengthPrefixedReader");
    }
}


impl<I: Io> FramedIo for LengthPrefixedWriter<I> {
    type In = Vec<u8>;
    type Out = Vec<u8>;
    fn poll_read(&mut self) -> Async<()> {
        panic!("poll_read on LengthPrefixedWriter");
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        panic!("read on LengthPrefixedWriter");
    }
    fn poll_write(&mut self) -> Async<()> {
        self.underlying.poll_write()
    }
    fn write(&mut self, req: Self::In) -> Poll<(), io::Error> {
        let mut tmp = mem::replace(&mut self.writestate, None);
        let (restore, result); // borrow checker workaround
        if let Some(ref mut st) = tmp {
            // if we're in the middle of a write, do that before starting the new request
            let newbytes = try!(self.underlying.write(&st.buf[st.sofar..]));
            st.sofar += newbytes;
            restore = if st.sofar == st.buf.len() { false } else { true };
            result = Ok(Async::NotReady);
        } else {
            // if we aren't busy, start the write
            try!(self.underlying.write_u64::<LittleEndian>(req.len() as u64));
            let mut st = LengthPrefixedFramerState { sofar: 0, buf: req, };
            let newbytes = try!(self.underlying.write(&st.buf[st.sofar..]));
            st.sofar += newbytes;
            if st.sofar == st.buf.len() {
                result = Ok(Async::Ready(()));
            } else {
                // if we didn't finish the write in one shot, stash the state
                self.writestate = Some(st);
                result = Ok(Async::NotReady);
            }
            restore = false;
        }
        if restore {
            self.writestate = tmp;
        }
        result
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        self.underlying.flush().map(Async::Ready)
    }
}

impl<I: Io> FramedIo for LengthPrefixedFramer<I> {
    type In = Vec<u8>;
    type Out = Vec<u8>;
    fn poll_read(&mut self) -> Async<()> {
        self.reader.poll_read()
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        self.reader.read()
    }
    fn poll_write(&mut self) -> Async<()> {
        self.writer.poll_write()
    }
    fn write(&mut self, req: Self::In) -> Poll<(), io::Error> {
        self.writer.write(req)
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        self.writer.flush()
    }
}

pub struct ReadFrame<I>(Option<I>);
pub fn read_frame<I>(i: I) -> ReadFrame<I> {
    ReadFrame(Some(i))
}
impl<I: FramedIo<In=In, Out=Out>, In, Out> Future for ReadFrame<I> {
    type Item = (I, Out);
    type Error = io::Error;
    fn poll(&mut self) -> Poll<(I, Out), io::Error> {
        let oldself = mem::replace(&mut self.0, None);
        let (res, newself) = if let Some(mut r) = oldself {
            if let Async::NotReady = r.poll_read() {
                (Ok(Async::NotReady), Some(r))
            } else { match r.read() {
                Ok(Async::Ready(x)) => (Ok(Async::Ready((r, x))), None),
                Ok(Async::NotReady) => (Ok(Async::NotReady), Some(r)),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (Ok(Async::NotReady), Some(r)),
                Err(e) => (Err(e), None),
            }}
        } else {
            (Err(io::Error::new(io::ErrorKind::Other, "ReadFrame.0 should never be None")), None)
        };
        self.0 = newself;
        res
    }
}

pub struct WriteFrame<I, In>(Option<(I, In)>);
pub fn write_frame<I, In>(i: I, req: In) -> WriteFrame<I, In> {
    WriteFrame(Some((i, req)))
}
impl<I: FramedIo<In=In, Out=Out>, In, Out> Future for WriteFrame<I, In> where In: Clone {
    type Item = I;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<I, io::Error> {
        let oldself = mem::replace(&mut self.0, None);
        let (res, newself) = if let Some((mut w, x)) = oldself {
            if let Async::NotReady = w.poll_write() {
                (Ok(Async::NotReady), Some((w, x)))
            } else { match w.write(x.clone() /* This clone will go away once FramedIo::write returns the original on NotReady */) {
                Ok(Async::Ready(())) => (Ok(Async::Ready(w)), None),
                Ok(Async::NotReady) => (Ok(Async::NotReady), Some((w, x))),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (Ok(Async::NotReady), Some((w, x))),
                Err(e) => (Err(e), None),
            }}
        } else {
            (Err(io::Error::new(io::ErrorKind::Other, "WriteFrame.0 should never be None")), None)
        };
        self.0 = newself;
        res
    }
}

pub struct SerdeFrameReader<F: FramedIo<In=In,Out=Vec<u8>>, T: Deserialize, In>(F, PhantomData<T>);
pub struct SerdeFrameWriter<F: FramedIo<In=Vec<u8>,Out=Out>, T: Serialize, Out>(F, PhantomData<T>);

impl<F: FramedIo<In=In,Out=Vec<u8>>, T: Deserialize, In> SerdeFrameReader<F, T, In> {
    pub fn new(f: F) -> SerdeFrameReader<F, T, In> {
        SerdeFrameReader(f, PhantomData)
    }
}

impl<F: FramedIo<In=Vec<u8>,Out=Out>, T: Serialize, Out> SerdeFrameWriter<F, T, Out> {
    pub fn new(f: F) -> SerdeFrameWriter<F, T, Out> {
        SerdeFrameWriter(f, PhantomData)
    }
}

impl<F: FramedIo<In=In,Out=Vec<u8>>, T: Deserialize, In> FramedIo for SerdeFrameReader<F, T, In> {
    type In = ();
    type Out = T;
    fn poll_read(&mut self) -> Async<()> {
        self.0.poll_read()
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        let buf = try_ready!(self.0.read());
        // TODO: polymorphism over deserializers
        serde_json::from_str(&String::from_utf8_lossy(&buf))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
            .map(|r| Async::Ready(r))
    }
    fn poll_write(&mut self) -> Async<()> {
        panic!("poll_write on SerdeFrameReader");
    }
    fn write(&mut self, _: Self::In) -> Poll<(), io::Error> {
        panic!("write on SerdeFrameReader");
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        panic!("flush on SerdeFrameReader");
    }
}

impl<F: FramedIo<In=Vec<u8>,Out=Out>, T: Serialize, Out> FramedIo for SerdeFrameWriter<F, T, Out> {
    type In = T;
    type Out = ();
    fn poll_read(&mut self) -> Async<()> {
        panic!("poll_read on SerdeFrameWriter");
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        panic!("read on SerdeFrameWriter");
    }
    fn poll_write(&mut self) -> Async<()> {
        self.0.poll_write()
    }
    fn write(&mut self, req: Self::In) -> Poll<(), io::Error> {
        // TODO: polymorphism over serializers
        let buf = try!(
            serde_json::to_string(&req)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
        self.0.write(buf.as_bytes().into())
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        self.0.flush()
    }
}
