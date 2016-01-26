extern crate rustc_serialize;

use std::collections::VecDeque;

//use self::rustc_serialize::json;
use self::rustc_serialize::{Encodable, Decodable};

use std::sync::mpsc;

pub type LogIndex = usize;

#[derive(RustcDecodable, RustcEncodable, Debug, Copy, Clone)]
pub struct Entry {
    data: i32,
    pub idx: Option<LogIndex>,
}

impl Entry {
    pub fn new(data: i32) -> Entry {
        return Entry {
            data: data,
            idx: None,
        }
    }

    pub fn data(&self) -> i32 {
        return self.data;
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
            tx.send(self.q[i as usize]).unwrap();
        }
        return rx;
    }
}

