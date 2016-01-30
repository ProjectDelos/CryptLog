extern crate rustc_serialize;

// use self::rustc_serialize::json;

use std::collections::{HashMap, HashSet};
use indexed_queue::{IndexedQueue, Entry, ObjId, State, Operation, TxType, TxState, LogIndex};

pub type Callback = FnMut(Operation);
// use indexed_queue::LogIndex;

pub struct Runtime<T>
    where T: IndexedQueue
{
    iq: T,

    callbacks: HashMap<ObjId, Vec<Box<Callback>>>,
    // pub index: HashMap<ObjId, LogIndex>,
    pub global_idx: LogIndex,
    obj_ids: HashSet<ObjId>,

    // Transaction semantics
    read_set: HashSet<ObjId>,
    write_set: HashSet<ObjId>,
    operations: Vec<Operation>,
    tx_mode: bool, // begin_tx_idx: Option<LogIndex>,
}

impl<T> Runtime<T> where T: IndexedQueue
{
    pub fn append(&mut self, obj_id: ObjId, data: State) {
        if self.tx_mode {
            self.write_set.insert(obj_id);
            self.operations.push(Operation::new(obj_id, data));
        } else {
            self.iq.append(Entry::new(HashSet::new(),
                                      vec![obj_id].into_iter().collect(),
                                      vec![Operation::new(obj_id, data)],
                                      TxType::None,
                                      TxState::None));
        }
    }

    pub fn begin_tx(&mut self) {
        self.tx_mode = true;
        // Sync all objects
        self.sync(None);
    }

    pub fn end_tx(&mut self) {
        self.iq.append(Entry::new(self.read_set.drain().collect(),
                                  self.write_set.drain().collect(),
                                  self.operations.drain(..).collect(),
                                  TxType::End,
                                  TxState::None));
        self.tx_mode = false;
    }

    pub fn sync(&mut self, obj_id: Option<ObjId>) {
        // for tx, only record read
        if obj_id.is_some() {
            if self.tx_mode {
                self.read_set.insert(obj_id.unwrap());
                return;
            }

        }

        // sync all objects runtime tracks
        let rx = self.iq.stream(&self.obj_ids, self.global_idx + 1, None);

        // send updates to relevant callbacks
        loop {
            match rx.recv() {
                Ok(e) => {
                    for op in &e.operations {
                        if !self.obj_ids.contains(&op.obj_id) {
                            // entry also has operation on object not tracked
                            continue;
                        }

                        // operation on tracked object sent to interested ds
                        let mut callbacks = self.callbacks.get_mut(&op.obj_id).unwrap();
                        for c in callbacks.iter_mut() {
                            c(op.clone());
                        }
                    }

                    let idx = e.idx.unwrap();
                    assert_eq!(idx, self.global_idx + 1);
                    self.global_idx = idx as LogIndex;
                }
                Err(_) => break,
            };
        }
    }

    pub fn catch_up(&mut self, obj_id: ObjId, mut c: &mut Box<Callback>) {
        let rx = self.iq.stream(&vec![obj_id].into_iter().collect(),
                                0,
                                Some(self.global_idx + 1));

        loop {
            match rx.recv() {
                Ok(e) => {
                    for op in &e.operations {
                        if obj_id != op.obj_id {
                            // entry also has operation on different object
                            continue;
                        }

                        (*c)(op.clone());
                    }
                }
                Err(_) => break,
            };
        }
    }

    pub fn register_object(&mut self, obj_id: ObjId, mut c: Box<Callback>) {
        self.obj_ids.insert(obj_id);

        self.catch_up(obj_id, &mut c);

        if !self.callbacks.contains_key(&obj_id) {
            self.callbacks.insert(obj_id, Vec::new());
        }
        self.callbacks.get_mut(&obj_id).unwrap().push(c);
    }
}

impl<T> Runtime<T> where T: IndexedQueue
{
    pub fn new(iq: T) -> Runtime<T> {
        return Runtime {
            iq: iq,
            obj_ids: HashSet::new(),
            callbacks: HashMap::new(),
            // index: HashMap::new(),
            global_idx: -1 as LogIndex,
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            operations: Vec::new(),
            tx_mode: false,
        };
    }
}

#[cfg(test)]
mod test {
    use super::Runtime;
    use indexed_queue::{InMemoryQueue, State};

    #[test]
    fn create_runtime() {
        let mut r: Runtime<InMemoryQueue> = Runtime::new(InMemoryQueue::new());
        r.append(0, State::Encoded(String::from("Hello")));
    }
}
