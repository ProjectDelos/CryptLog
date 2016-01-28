extern crate rustc_serialize;

use std::collections::VecDeque;
use std::sync::{Mutex, Arc};

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
    pub obj_id: ObjId, // hard coded
    pub operator: State,
}

impl Operation {
    pub fn new(obj_id : ObjId, operator: State) -> Operation {
        Operation{obj_id: obj_id, operator: operator}
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone)]
pub enum TxType {
    Begin,
    End,
    None,
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone)]
pub enum TxState {
    Accepted,
    Aborted,
    None,
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone)]
pub struct Entry {
    pub idx: Option<LogIndex>,
    read_set: Vec<ObjId>,
    write_set: Vec<ObjId>,
    pub operations: Vec<Operation>,

    tx_type: TxType,
    tx_state: TxState,
}

impl Entry {
    pub fn new(read_set: Vec<ObjId>, write_set: Vec<ObjId>,
               operations: Vec<Operation>,
               tx_type: TxType, tx_state: TxState) -> Entry {
        return Entry {
            idx: None,
            read_set: read_set,
            write_set: write_set,
            operations: operations,
            tx_type: tx_type,
            tx_state: tx_state,
        }
    }
}

pub trait IndexedQueue {
    fn append(&mut self, e: Entry) -> LogIndex;
    fn stream_from_to(&self, from: LogIndex,
                      to: Option<LogIndex>) -> mpsc::Receiver<Entry>;
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
    fn append (&mut self, mut e: Entry) -> LogIndex {
        e.idx = Some(self.q.len());
        println!("InMemoryQueue::append {:?}", e);
        self.q.push_back(e);
        return self.q.len();
    }

    fn stream_from_to(&self, from: LogIndex,
                      to: Option<LogIndex>) -> mpsc::Receiver<Entry> {
        // TODO: thread
        println!("InMemoryQueue::stream_from_to from idx {}", from);
        let (tx, rx) = mpsc::channel();
        let to = match to {
            Some(idx) => idx,
            None => self.q.len()
        };

        for i in from..to as LogIndex {
            tx.send(self.q[i as usize].clone()).unwrap();
        }
        return rx;
    }
}

#[derive(Clone)]
pub struct SharedQueue {
    q: Arc<Mutex<InMemoryQueue>>,
}

impl SharedQueue {
    pub fn new() -> SharedQueue {
        SharedQueue {
            q: Arc::new(Mutex::new(InMemoryQueue::new())),
        }
    }
}

impl IndexedQueue for SharedQueue {
    fn append(&mut self, e: Entry) -> LogIndex {
        self.q.lock().unwrap().append(e)
    }
    fn stream_from_to(&self, from: LogIndex,
                      to: Option<LogIndex>) -> mpsc::Receiver<Entry> {
        self.q.lock().unwrap().stream_from_to(from, to)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::State::{Encoded};
    fn entry() -> Entry {
        Entry::new(
            vec![0,1,2],
            vec![1,2],
            vec![
                Operation::new(0, Encoded("get(k0)".to_string())),
                Operation::new(1, Encoded("get(k1)".to_string())),
                Operation::new(2, Encoded("get(k2)".to_string())),
                Operation::new(1, Encoded("put(k1, 0)".to_string())),
                Operation::new(2, Encoded("put(k2, 1)".to_string())),
            ],
            TxType::None,
            TxState::None)
    }

    #[test]
    fn in_memory() {
        let mut q = InMemoryQueue::new();
        let e = entry();
        q.append(e);
    }
}
