extern crate rustc_serialize;

// use self::rustc_serialize::json;
use std::marker::PhantomData;

use std::collections::{HashMap, HashSet};
use indexed_queue::{IndexedQueue, Entry, ObjId, State, Operation, TxType, TxState, LogIndex, LogOp};

pub type Callback = FnMut(LogIndex, Operation) + Send;
pub type EntryCallback = FnMut(Entry) + Send;

pub trait Encryptor {
    fn encrypt(s: State) -> State;
    fn decrypt(s: State) -> State;
}

#[derive(Clone)]
pub struct Identity;
impl Identity {
    pub fn new() -> Identity {
        return Identity;
    }
}
impl Encryptor for Identity {
    fn encrypt(state: State) -> State {
        return match state {
            State::Encoded(s) => State::Encrypted(s.into_bytes()),
            State::Encrypted(_) => panic!("already encrypted"), 
        };
    }

    fn decrypt(state: State) -> State {
        return match state {
            State::Encoded(_) => panic!("already decrypted"),
            State::Encrypted(v) => State::Encoded(String::from_utf8(v).unwrap()),
        };
    }
}

pub struct Runtime<T, Secure> {
    iq: T,
    e: PhantomData<Secure>,

    callbacks: HashMap<ObjId, Vec<Box<Callback>>>,
    entry_callbacks: Vec<Box<EntryCallback>>,
    version: HashMap<ObjId, LogIndex>,
    pub global_idx: LogIndex,
    obj_ids: HashSet<ObjId>,

    // Transaction semantics
    reads: HashMap<ObjId, LogIndex>,
    writes: HashSet<ObjId>,
    operations: Vec<Operation>,
    tx_mode: bool, // begin_tx_idx: Option<LogIndex>,
}

impl<Q, Secure> Runtime<Q, Secure>
    where Q: IndexedQueue + Send,
          Secure: Encryptor
{
    pub fn new(iq: Q) -> Runtime<Q, Secure> {
        return Runtime {
            iq: iq,
            e: PhantomData,

            obj_ids: HashSet::new(),
            callbacks: HashMap::new(),
            entry_callbacks: Vec::new(),
            version: HashMap::new(),
            global_idx: -1 as LogIndex,

            reads: HashMap::new(),
            writes: HashSet::new(),
            operations: Vec::new(),
            tx_mode: false,
        };
    }
    pub fn append(&mut self, obj_id: ObjId, data: State) {
        if self.tx_mode {
            self.writes.insert(obj_id);
            self.operations.push(Operation::new(obj_id, Secure::encrypt(data)));
        } else {
            self.iq.append(Entry::new(HashMap::new(),
                                      vec![obj_id].into_iter().collect(),
                                      vec![Operation::new(obj_id, Secure::encrypt(data))],
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
        self.iq.append(Entry::new(self.reads.drain().collect(),
                                  self.writes.drain().collect(),
                                  self.operations.drain(..).collect(),
                                  TxType::End,
                                  TxState::None));
        self.tx_mode = false;
        // TODO: wait for tx validation and return ok or err
    }

    pub fn validate_tx(&mut self, e: &mut Entry) {
        if e.tx_type == TxType::End && e.tx_state == TxState::None {
            // validate based on versions
            for (obj_id, version) in &e.reads {
                if *version < self.version[obj_id] {
                    e.tx_state = TxState::Aborted;
                    return;
                }
            }
            e.tx_state = TxState::Accepted;
            // update versions
            for obj_id in &e.writes {
                let idx: &mut i64 = self.version.get_mut(obj_id).unwrap();
                *idx = e.idx.unwrap();
            }
        }
    }


    pub fn sync(&mut self, obj_id: Option<ObjId>) {
        // for tx, only record read
        if obj_id.is_some() {
            if self.tx_mode {
                let obj_id = obj_id.unwrap();
                self.reads.insert(obj_id, self.version[&obj_id]);
                return;
            }

        }

        // sync all objects runtime tracks
        let rx = self.iq.stream(&self.obj_ids, self.global_idx + 1, None);
        // send updates to relevant callbacks
        loop {
            match rx.recv() {
                Ok(mut e) => {
                    // replicate entries to entry_callbacks
                    for cb in self.entry_callbacks.iter_mut() {
                        let e = e.clone();
                        cb(e);
                    }

                    self.validate_tx(&mut e);
                    if e.tx_state == TxState::Aborted {
                        continue;
                    }
                    let idx = e.idx.clone().unwrap();

                    for op in &e.operations {
                        if !self.obj_ids.contains(&op.obj_id) {
                            // entry also has operation on object not tracked
                            continue;
                        }
                        let obj_id = op.obj_id;

                        match op.operator {
                            LogOp::Op(ref operator) => {
                                // operation on tracked object sent to interested ds
                                let mut callbacks = self.callbacks.get_mut(&obj_id).unwrap();
                                let dec_operator = Secure::decrypt(operator.clone());
                                for c in callbacks.iter_mut() {
                                    c(idx, Operation::new(obj_id, dec_operator.clone()));
                                }

                            }
                            _ => {
                                unimplemented!();
                            }
                        }
                    }

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

                        match op.operator {
                            LogOp::Op(ref operator) => {
                                let dec_operator = Secure::decrypt(operator.clone());
                                (*c)(e.idx.unwrap(), Operation::new(obj_id, dec_operator));
                            }
                            _ => {
                                unimplemented!();
                            }
                        }
                    }
                }
                Err(_) => break,
            };
        }
    }

    pub fn register_object(&mut self, obj_id: ObjId, mut c: Box<Callback>) {
        self.obj_ids.insert(obj_id);
        if !self.version.contains_key(&obj_id) {
            self.version.insert(obj_id, -1);
        }

        self.catch_up(obj_id, &mut c);

        if !self.callbacks.contains_key(&obj_id) {
            self.callbacks.insert(obj_id, Vec::new());
        }
        self.callbacks.get_mut(&obj_id).unwrap().push(c);
    }
    pub fn register_log_reader(&mut self, c: Box<EntryCallback>) {
        self.entry_callbacks.push(c);
    }
}


#[cfg(test)]
mod test {
    use super::{Runtime, Identity};
    use indexed_queue::{InMemoryQueue, State};

    #[test]
    fn create_runtime() {
        let q = InMemoryQueue::new();
        let mut r: Runtime<InMemoryQueue, Identity> = Runtime::new(q);
        r.append(0, State::Encoded(String::from("Hello")));
    }
}
