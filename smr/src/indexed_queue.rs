extern crate rustc_serialize;

use std::collections::{VecDeque, HashSet, HashMap};
use std::sync::{Mutex, Arc};

// use self::rustc_serialize::json;
use self::rustc_serialize::Encodable;

use std::sync::mpsc;

pub type LogIndex = i64;
pub type ObjId = i32;

#[derive(RustcDecodable, RustcEncodable, Debug, Clone, PartialEq, Eq)]
pub enum State {
    Encrypted(Vec<u8>),
    Encoded(String),
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    pub obj_id: ObjId, // hard coded
    pub operator: State,
}

impl Operation {
    pub fn new(obj_id: ObjId, operator: State) -> Operation {
        Operation {
            obj_id: obj_id,
            operator: operator,
        }
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone, PartialEq, Eq)]
pub enum TxType {
    Begin,
    End,
    None,
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone, PartialEq, Eq)]
pub enum TxState {
    Accepted,
    Aborted,
    None,
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone)]
pub struct Entry {
    pub idx: Option<LogIndex>,

    pub reads: HashMap<ObjId, LogIndex>,
    pub writes: HashSet<ObjId>,
    pub operations: Vec<Operation>,

    pub tx_type: TxType,
    pub tx_state: TxState,
}

impl Entry {
    pub fn new(reads: HashMap<ObjId, LogIndex>,
               writes: HashSet<ObjId>,
               operations: Vec<Operation>,
               tx_type: TxType,
               tx_state: TxState)
               -> Entry {
        return Entry {
            idx: None,
            reads: reads,
            writes: writes,
            operations: operations,
            tx_type: tx_type,
            tx_state: tx_state,
        };
    }
}

pub trait IndexedQueue {
    fn append(&mut self, e: Entry) -> LogIndex;
    fn stream(&self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<Entry>;
}

#[derive(Clone)]
pub struct InMemoryQueue {
    q: VecDeque<Entry>,
    version: HashMap<ObjId, LogIndex>,
}

impl InMemoryQueue {
    pub fn new() -> InMemoryQueue {
        return InMemoryQueue {
            q: VecDeque::new(),
            version: HashMap::new(),
        };
    }
}

impl IndexedQueue for InMemoryQueue {
    fn append(&mut self, mut e: Entry) -> LogIndex {
        e.idx = Some(self.q.len() as LogIndex);
        // println!("InMemoryQueue::append {:?}", e);
        self.q.push_back(e);
        return self.q.len() as LogIndex;
    }

    fn stream(&self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<Entry> {
        let (tx, rx) = mpsc::channel();
        let to = match to {
            Some(idx) => idx,
            None => self.q.len() as LogIndex,
        };

        for i in from..to as LogIndex {
            if !self.q[i as usize].writes.is_disjoint(&obj_ids) {
                // entry relevant to some obj_ids
                tx.send(self.q[i as usize].clone()).unwrap();
            }
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
        SharedQueue { q: Arc::new(Mutex::new(InMemoryQueue::new())) }
    }
}

impl IndexedQueue for SharedQueue {
    fn append(&mut self, e: Entry) -> LogIndex {
        self.q.lock().unwrap().append(e)
    }
    fn stream(&self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<Entry> {
        self.q.lock().unwrap().stream(obj_ids, from, to)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::State::Encoded;
    use std::thread;
    fn entry() -> Entry {
        Entry::new(vec![(0, 0), (1, 0), (2, 0)].into_iter().collect(),
                   vec![1, 2].into_iter().collect(),
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
        let n = 5;
        for _ in 0..n {
            let e = entry();
            q.append(e);
        }
        let rx = q.stream(&vec![0, 1, 2].into_iter().collect(), 0, None);
        let mut read = 0;
        let ent = entry();
        for e in rx {
            assert_eq!(e.idx.unwrap(), read);
            assert_eq!(e.operations[0], ent.operations[0]);
            read += 1;
            // assert_eq!(e, entry);
        }
        assert_eq!(read, n);
    }

    #[test]
    fn shared_queue() {
        let mut q1 = SharedQueue::new();
        let mut q2 = q1.clone();
        let q3 = q1.clone();
        let n = 5;

        let child1 = thread::spawn(move || {
            // some work here
            for _ in 0..n {
                q1.append(entry());
            }
        });
        let child2 = thread::spawn(move || {
            for _ in 0..n {
                q2.append(entry());
            }
        });
        child1.join().unwrap();
        child2.join().unwrap();

        let rx = q3.stream(&vec![0, 1, 2].into_iter().collect(), 0, None);
        let mut read = 0;
        for e in rx {
            assert_eq!(e.idx.unwrap(), read);
            read += 1;
        }
        assert_eq!(read, n * 2);
    }
}
