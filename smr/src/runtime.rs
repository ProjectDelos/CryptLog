extern crate rustc_serialize;

use std::collections::{HashMap, HashSet};
use indexed_queue::{IndexedQueue, Entry, ObjId, State, Operation, TxType, TxState, LogIndex, LogOp};
use encryptors::MetaEncryptor;

pub type Callback = FnMut(LogIndex, Operation) + Send;
pub type EntryCallback = FnMut(Entry) + Send;

// State::Encoded(s) => State::Encrypted(s.into_bytes()),
// State::Encrypted(v) => State::Encoded(String::from_utf8(v).unwrap()),

pub struct Runtime<Q> {
    iq: Q,

    callbacks: HashMap<ObjId, Vec<Box<Callback>>>,
    entry_callbacks: Vec<Box<EntryCallback>>,
    version: HashMap<ObjId, LogIndex>,
    pub global_idx: LogIndex,
    obj_ids: HashSet<ObjId>,

    // Transaction semantics
    reads: HashMap<ObjId, LogIndex>,
    writes: HashSet<ObjId>,
    operations: Vec<Operation>,
    pub tx_mode: bool, // begin_tx_idx: Option<LogIndex>,

    pub secure: Option<MetaEncryptor>,
}

impl<Q> Runtime<Q> where Q: IndexedQueue + Send
{
    pub fn new(iq: Q, me: Option<MetaEncryptor>) -> Runtime<Q> {
        return Runtime {
            iq: iq,

            obj_ids: HashSet::new(),
            callbacks: HashMap::new(),
            entry_callbacks: Vec::new(),
            version: HashMap::new(),
            global_idx: -1 as LogIndex,

            reads: HashMap::new(),
            writes: HashSet::new(),
            operations: Vec::new(),
            tx_mode: false,

            secure: me,
        };
    }

    pub fn append(&mut self, obj_id: ObjId, data: State) {
        if self.tx_mode {
            self.writes.insert(obj_id);
            self.operations.push(Operation::new(obj_id, data));
        } else {
            self.iq.append(Entry::new(HashMap::new(),
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

    pub fn end_tx(&mut self) -> TxState {
        let tx_idx = self.iq.append(Entry::new(self.reads.drain().collect(),
                                               self.writes.drain().collect(),
                                               self.operations.drain(..).collect(),
                                               TxType::End,
                                               TxState::None));
        self.tx_mode = false;
        self.reads.clear();
        self.writes.clear();
        self.operations.clear();
        return self.internal_sync(None, Some(tx_idx));
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
        self.internal_sync(obj_id, None);
    }

    pub fn internal_sync(&mut self, obj_id: Option<ObjId>, tx_idx: Option<LogIndex>) -> TxState {
        use indexed_queue::LogData::{LogEntry, LogSnapshot};
        // for tx, only record read
        if obj_id.is_some() {
            if self.tx_mode {
                let obj_id = obj_id.unwrap();
                self.reads.insert(obj_id, self.version[&obj_id]);
                return TxState::None;
            }

        }

        // sync all objects runtime tracks
        let rx = self.iq.stream(&self.obj_ids, self.global_idx + 1, None);
        // send updates to relevant callbacks
        loop {
            match rx.recv() {
                Ok(LogEntry(mut e)) => {
                    let e_idx = e.idx.clone().unwrap();
                    self.global_idx = e_idx.clone() as LogIndex;

                    self.validate_tx(&mut e);

                    // see if entry has idx user is waiting on
                    let same_idx = match tx_idx {
                        Some(tx_idx) => tx_idx == e_idx,
                        None => false,
                    };

                    // no callback updates needed if tx was aborted
                    if e.tx_state == TxState::Aborted {
                        if same_idx {
                            return TxState::Aborted;
                        }
                        continue;
                    }
                    // replicate entries to entry_callbacks
                    for cb in self.entry_callbacks.iter_mut() {
                        let e = e.clone();
                        cb(e);
                    }
                    
                    for op in &e.operations {
                        // every operation is a write of sorts
                        *(self.version.get_mut(&op.obj_id).unwrap()) += 1;

                        if !self.obj_ids.contains(&op.obj_id) {
                            // entry also has operation on object not tracked
                            continue;
                        }
                        let obj_id = op.obj_id;
                        match op.operator {
                            LogOp::Op(ref operator) => {
                                // operation on tracked object sent to interested ds
                                let mut callbacks = self.callbacks.get_mut(&obj_id).unwrap();
                                let dec_operator = operator.clone();
                                for c in callbacks.iter_mut() {
                                    c(e_idx, Operation::new(obj_id, dec_operator.clone()));
                                }

                            }
                            _ => {
                                unimplemented!();
                            }
                        }
                    }



                    if same_idx {
                        return e.tx_state.clone();
                    }
                }
                Ok(LogSnapshot(s)) => {
                    self.global_idx = s.idx as LogIndex;

                    if !self.obj_ids.contains(&s.obj_id) {
                        continue;
                    }
                    let obj_id = s.obj_id;
                    let idx = s.idx;
                    let callbacks = self.callbacks.get_mut(&obj_id).unwrap();
                    let snapshot = s.payload;
                    for c in callbacks.iter_mut() {
                        c(idx, Operation::from_snapshot(obj_id, snapshot.clone()));
                    }
                }
                Err(_) => break,
            };
        }
        return TxState::None;
    }

    pub fn catch_up(&mut self, obj_id: ObjId, mut c: &mut Box<Callback>) {
        use indexed_queue::LogData::{LogEntry, LogSnapshot};
        let rx = self.iq.stream(&vec![obj_id].into_iter().collect(),
                                0,
                                Some(self.global_idx));

        loop {
            match rx.recv() {
                Ok(LogEntry(e)) => {
                    for op in &e.operations {
                        if obj_id != op.obj_id {
                            // entry also has operation on different object
                            continue;
                        }

                        match op.operator {
                            LogOp::Op(ref operator) => {
                                let dec_operator = operator.clone();
                                (*c)(e.idx.unwrap(), Operation::new(obj_id, dec_operator));
                            }
                            _ => {
                                unimplemented!();
                            }
                        }
                    }
                }
                Ok(LogSnapshot(s)) => {
                    let snapshot = s.payload;
                    (*c)(s.idx, Operation::from_snapshot(obj_id, snapshot));
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
    use super::Runtime;
    use indexed_queue::{InMemoryQueue, State};
    use encryptors::MetaEncryptor;

    #[test]
    fn create_runtime() {
        let q = InMemoryQueue::new();
        let mut r: Runtime<InMemoryQueue> = Runtime::new(q, Some(MetaEncryptor::new()));
        r.append(0, State::Encoded(String::from("Hello")));
    }
}
