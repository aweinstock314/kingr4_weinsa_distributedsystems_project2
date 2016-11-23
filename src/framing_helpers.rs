use super::*;
//use serde::{Serialize, Deserialize};
use std::marker::PhantomData;
use std::cmp::Ordering;
use tokio_core::io::{Codec, EasyBuf};

fn str_to_ioerror(s: &'static str) -> io::Error {
    let e: Box<Error+Send+Sync> = s.into();
    io::Error::new(io::ErrorKind::Other, e)
}

// https://en.wikipedia.org/wiki/Consistent_Overhead_Byte_Stuffing
pub struct COBSCodec {
    delimiter: u8,
}

impl COBSCodec {
    pub fn new(delimiter: u8) -> COBSCodec {
        COBSCodec { delimiter: delimiter }
    }
}

// adapted from https://bitbucket.org/cmcqueen1975/cobs-python/src/e8d6ed7e7f4754f6a79a5fecc69ca29528a9ee39/python2/cobs/cobs/_cobs_py.py
impl Codec for COBSCodec {
    type In = Vec<u8>;
    type Out = Vec<u8>;

    fn encode(&mut self, in_bytes: Self::Out, out_bytes: &mut Vec<u8>) -> io::Result<()> {
        // TODO: reserve out_bytes for efficiency?
        let mut final_zero = true;
        let mut idx = 0;
        let mut search_start_idx = 0;
        for &in_char in in_bytes.iter() {
            if in_char == self.delimiter {
                final_zero = true;
                out_bytes.push(idx - search_start_idx + 1);
                out_bytes.extend_from_slice(&in_bytes[search_start_idx as usize..idx as usize]);
                search_start_idx = idx + 1;
            } else {
                if idx - search_start_idx == 0xfd {
                    final_zero = false;
                    out_bytes.push(0xff);
                    out_bytes.extend_from_slice(&in_bytes[search_start_idx as usize..idx as usize+1]);
                    search_start_idx = idx + 1;
                }
            }
            idx += 1;
        }
        if idx != search_start_idx || final_zero {
            out_bytes.push(idx - search_start_idx + 1);
            out_bytes.extend_from_slice(&in_bytes[search_start_idx as usize..idx as usize]);
        }
        Ok(())
    }

    fn decode(&mut self, in_bytes: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        let mut out_bytes = vec![]; // TODO: reserve for efficiency?
        let mut idx = 0;
        if in_bytes.len() > 0 {
            loop {
                let length = in_bytes.as_slice()[idx];
                if length == 0 {
                    return Err(str_to_ioerror("COBSCodec::decode: encountered zero length"));
                }
                idx += 1;
                let end = idx + (length as usize) - 1;
                let copy_bytes = &in_bytes.as_slice()[idx..end];
                if copy_bytes.iter().any(|&c| c == self.delimiter) {
                    return Err(str_to_ioerror("COBSCodec::decode: encountered unexpected delimiter"));
                }
                out_bytes.extend_from_slice(copy_bytes);
                idx = end;
                match idx.cmp(&in_bytes.len()) {
                    Ordering::Greater => return Ok(None),
                    Ordering::Less => if length < 0xff {
                        out_bytes.push(self.delimiter);
                    },
                    Ordering::Equal => break,
                }
            }
        }
        Ok(Some(out_bytes))
    }
}

pub struct SerdeJSONCodec<S, D>(PhantomData<(S, D)>);
impl<S, D> SerdeJSONCodec<S, D> {
    pub fn new() -> SerdeJSONCodec<S, D> {
        SerdeJSONCodec(PhantomData)
    }
}

impl<S: Serialize, D: Deserialize> Codec for SerdeJSONCodec<S, D> {
    type In = D;
    type Out = S;
    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        buf.extend(serde_json::to_string(&msg).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?.into_bytes());
        Ok(())
    }
    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        use serde_json::error::Error::*;
        use serde_json::error::ErrorCode::*;
        match serde_json::from_str(&String::from_utf8_lossy(buf.as_slice())) {
            Ok(val) => Ok(Some(val)),
            Err(Syntax(ref code, _, _)) if match *code {
                EOFWhileParsingList | EOFWhileParsingObject | EOFWhileParsingString | EOFWhileParsingValue => true,
                _ => false
            } => Ok(None),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }
}

pub struct PostcomposeCodec<F, G>(F, G);
pub fn postcompose_codec<F, G>(f: F, g: G) -> PostcomposeCodec<F, G> {
    PostcomposeCodec(f, g)
}
impl<A, B, F, G> Codec for PostcomposeCodec<F, G> where
    F: Codec<In=A, Out=B>,
    G: Codec<In=Vec<u8>, Out=Vec<u8>> {
    type In = A;
    type Out = B;
    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let mut tmp = vec![];
        self.0.encode(msg, &mut tmp)?;
        self.1.encode(tmp, buf)
    }
    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        match self.1.decode(buf)? {
            Some(x) => {
                let mut tmp = EasyBuf::new();
                tmp.get_mut().extend(x.into_iter());
                self.0.decode(&mut tmp)
            },
            None => Ok(None),
        }
    }
}

/*pub struct LengthPrefixedFramerState {
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
}*/
