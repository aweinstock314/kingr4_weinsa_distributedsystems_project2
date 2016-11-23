/*
encode :: Out -> bytes
decode :: bytes -> In
encodeF :: B -> bytes
decodeF :: bytes -> A
encodeG :: C -> bytes
decodeG :: bytes -> B
decode . encode :: Out -> In
decodeF . encodeF :: B -> A
decodeG . encodeG :: C -> B
decodeG . encodeG . decodeF . encodeF :: C -> A
doesn't look possible to get (A -> C) without "misusing" the bytes
*/
/*pub struct ComposedCodec<F, G>(F, G);
impl<A, B, C, F: Codec<In=A, Out=B>, G: Codec<In=B, Out=C>> Codec for ComposedCodec<F, G> {
    type In = A;
    type Out = C;
    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let mut tmp = EasyBuf::new();
        self.1.encode(msg, &mut *tmp.get_mut())?;
        let b = match self.1.decode(&mut tmp)? {
            Some(val) => val,
            None => panic!("ComposedCodec: self.1.encode should have encoded a whole frame"),
        };
        self.0.encode(b, buf)
    }
    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        let b = match self.0.decode(buf)? {
            Some(val) => val,
            None => return Ok(None),
        };
        let mut tmp = EasyBuf::new();
        self.1.encode(b, &mut *tmp.get_mut())?;
        unimplemented!()
    }
}*/
