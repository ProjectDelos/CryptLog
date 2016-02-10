extern crate rustc_serialize;
use self::rustc_serialize::{Encodable, Decodable};

use std::collections::HashSet;

use indexed_queue::{Entry, LogIndex, ObjId};

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub enum HttpResponse {
    Stream(Vec<Entry>),
    Append(LogIndex),
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub enum HttpRequest {
    Stream(HashSet<ObjId>, LogIndex, Option<LogIndex>),
    Append(Entry),
}
