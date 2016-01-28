extern crate rustc_serialize;

use std::collections::VecDeque;

//use self::rustc_serialize::json;
use self::rustc_serialize::{Encodable, Decodable};

use std::sync::mpsc;

pub type LogIndex = usize;
pub type ObjId = i32;

#[derive(RustcDecodable, RustcEncodable, Debug, Clone)]
pub enum State {
    Encrypted(Vec<u8>),
    Encoded(String),
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone)]
pub struct Operation {
    obj_id: ObjId, // hard coded
    operator: State,
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone)]
pub struct Entry {
    pub idx: Option<LogIndex>,
    read_set: Vec<State>,
    write_set: Vec<State>,
    pub operations: Vec<State>,
}

impl Entry {
    pub fn new(read_set: Vec<State>, write_set: Vec<State>,
               operations: Vec<State>) -> Entry {
        return Entry {
            idx: None,
            read_set: read_set,
            write_set: write_set,
            operations: operations,
        }
    }
}

pub trait IndexedQueue {
    fn append(&mut self, e: Entry);
    fn stream_from(&self, idx: LogIndex) -> mpsc::Receiver<Entry>;
}

pub struct InMemoryQueue {
    q: VecDeque<Entry>,
}

impl InMemoryQueue {
    pub fn new() -> InMemoryQueue {
        return InMemoryQueue {
            q: VecDeque::new(),
        }
    }
}

impl IndexedQueue for InMemoryQueue {
    fn append (&mut self, mut e: Entry) {
        e.idx = Some(self.q.len());
        println!("InMemoryQueue::append {:?}", e);
        self.q.push_back(e);
    }

    fn stream_from(&self, idx: LogIndex) -> mpsc::Receiver<Entry> {
        println!("InMemoryQueue::stream_from idx {}", idx);
        let (tx, rx) = mpsc::channel();
        // TODO: thread
        for i in idx..self.q.len() as LogIndex {
            tx.send(self.q[i as usize].clone()).unwrap();
        }
        return rx;
    }
}

