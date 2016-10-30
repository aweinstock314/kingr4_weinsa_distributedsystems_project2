/// This file handles all the parsing
/// 1. Parsing the incoming nodes file of the nodes and their respective ips/ports

use super::*;

named!(parse_usize<&[u8], usize>, map_res!(map_res!(nom::digit, str::from_utf8), str::FromStr::from_str));

// Parses the nodes.txt 
// input in form of (pid, ip, port, cliport)
// ex: (1,127.0.0.1,9001,10001)
// returns a map of of nodes: pid -> (socketaddr, cliport)
pub fn parse_nodes(input: &[u8]) -> IResult<&[u8], Nodes> {
    named!(quoted_string<&[u8], &[u8]>, chain!(tag!("\"") ~ s: is_not!("\"\r\n") ~ tag!("\""), || s));
    named!(quoted_ip<&[u8], IpAddr>, map_res!(map_res!(quoted_string, str::from_utf8), IpAddr::from_str));
    named!(node<&[u8], (Pid, (SocketAddr, u16))>, chain!(tag!("(") ~
        pid: parse_usize ~ tag!(",") ~
        ip: quoted_ip ~ tag!(",") ~
        port: parse_usize ~ tag!(",") ~
        cliport: parse_usize ~ tag!(")"),
        || { (pid, (SocketAddr::new(ip, port as u16), cliport as u16)) }));
    named!(nodes<&[u8], Vec<(Pid, (SocketAddr, u16))> >, separated_list!(is_a!("\r\n"), node));

    nodes(input).map(|nodes: Vec<(Pid, (SocketAddr, u16))>| { nodes.into_iter().collect::<Nodes>() })
}

// Can't get the types to line up right, too general for now
/*fn run_parser<R: Read+Send, A: Send, E: Send, F: FnMut(&[u8]) -> IResult<&[u8], A, E>>(reader: R, parser: F) -> Box<Future<Item=A, Error=Box<Error+Send>>> {
    let buf_future: Box<Future<Item=A, Error=Box<Error+Send>>> = read_to_end(reader, Vec::new()).map_err(Box::new).boxed();
    buf_future.and_then(|(_, buf)| {
        match parser(&buf) {
            IResult::Done(_, o) => futures::finished(o).boxed(),
            IResult::Error(_) => futures::failed("Parse error".into()).boxed(),
            IResult::Incomplete(_) => futures::failed("Incomplete parse".into()).boxed(),
        }
    }).boxed()
}*/

// simply runs a passed in parser on the passed in filename
// returns a Result, which is either a generic (i.e. whatever you want) or an Error
pub fn run_parser_on_file<A, F: Fn(&[u8]) -> IResult<&[u8], A>>(filename: &str, parser: F) -> Result<A, Box<Error>> {
    let mut file = BufReader::new(try!(File::open(filename)));
    let mut buf = vec![];
    try!(file.read_to_end(&mut buf));
    match parser(&buf) {
        IResult::Done(_, o) => Ok(o),
        IResult::Error(_) => Err("Parse error".into()),
        IResult::Incomplete(_) => Err("Incomplete parse".into()),
    }
}


