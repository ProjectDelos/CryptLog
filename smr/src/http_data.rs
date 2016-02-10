extern crate rustc_serialize;
use self::rustc_serialize::{Encodable, Decodable};

use std::collections::HashSet;

use indexed_queue::{LogData, Entry, LogIndex, ObjId};

#[derive(RustcEncodable, RustcDecodable)]
pub enum HttpResponse {
    Stream(Vec<LogData>),
    Append(LogIndex),
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub enum HttpRequest {
    Stream(HashSet<ObjId>, LogIndex, Option<LogIndex>),
    Append(Entry),
}
