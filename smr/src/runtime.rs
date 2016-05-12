extern crate rustc_serialize;

use std::collections::{HashMap, HashSet};
use indexed_queue::{IndexedQueue, Entry, ObjId, State, Operation, TxType, TxState, LogIndex, LogOp};
use encryptors::MetaEncryptor;

pub type Callback = FnMut(LogIndex, Operation) + Send;
pub type EntryCallback = FnMut(Entry) + Send;

// Class: Runtime
// Paramatrized By:
// * Q: structure allowing seamless communicating with Shared Log
pub struct Runtime<Q> {
    iq: Q, // structure allowing seamless communicating with Shared Log

    callbacks: HashMap<ObjId, Vec<Box<Callback>>>,
    pre_callbacks: Vec<Box<EntryCallback>>,
    post_callbacks: Vec<Box<EntryCallback>>,
    pub global_idx: LogIndex, // index of last SharedLog entry synced
    obj_ids: HashSet<ObjId>, // ids of objectes registered with runtime

    // Transaction semantics
    reads: HashMap<ObjId, LogIndex>, // map of <object id, version read during transaction>
    writes: HashSet<ObjId>, // set of obj_ids written in current transaction
    version: HashMap<ObjId, LogIndex>, // map of last version read for each object id
    operations: Vec<Operation>, // operations to be included in current open transaction, if any
    pub tx_mode: bool, // true during transaction

    pub secure: Option<MetaEncryptor>, // structure to allow use of exising Encryptors/ Decryptors
}

impl<Q> Drop for Runtime<Q> {
    fn drop(&mut self) {
        self.stop();
        // println!("DROPPING RUNTIME");
    }
}

impl<Q> Runtime<Q> {
    pub fn stop(&mut self) {
        // println!("Clearing Runtime");

        self.callbacks.clear();
        self.pre_callbacks.clear();
        self.post_callbacks.clear();
        self.version.clear();
        self.obj_ids.clear();

        self.reads.clear();
        self.writes.clear();
        self.operations.clear();

        self.secure.take();
        // println!("Runtime Cleared");
    }
}

impl<Q> Runtime<Q> where Q: IndexedQueue + Send
{
    pub fn new(iq: Q, me: Option<MetaEncryptor>) -> Runtime<Q> {
        return Runtime {
            iq: iq,

            obj_ids: HashSet::new(),
            callbacks: HashMap::new(),
            pre_callbacks: Vec::new(),
            post_callbacks: Vec::new(),
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
            // accumulate transaction writes
            self.writes.insert(obj_id);
            self.operations.push(Operation::new(obj_id, data));
        } else {
            // append (send) entry to SharedLog
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
        // signal end of transaction by sending TxEnd logentry to SharedLog
        let tx_idx = self.iq.append(Entry::new(self.reads.drain().collect(),
                                               self.writes.drain().collect(),
                                               self.operations.drain(..).collect(),
                                               TxType::End,
                                               TxState::None));
        // clean up transaction state
        self.tx_mode = false;
        self.reads.clear();
        self.writes.clear();
        self.operations.clear();
        // sync up to transaction before returning to client
        return self.internal_sync(None, Some(tx_idx));
    }

    pub fn validate_tx(&mut self, e: &mut Entry) {
        if e.tx_type == TxType::End && e.tx_state == TxState::None {
            // validate based on versions
            for (obj_id, version) in &e.reads {
                if *version < self.version[obj_id] {
                    // there exist more recent changes to obj_id in tx reads set
                    // so transaction must be aborted
                    e.tx_state = TxState::Aborted;
                    return;
                }
            }

            e.tx_state = TxState::Accepted;
            // update versions of objects in writes set
            for obj_id in &e.writes {
                let idx: &mut i64 = self.version.get_mut(obj_id).unwrap();
                *idx = e.idx.unwrap();
            }
        }
    }

    // Method: sync, Blocking
    // Syncs all objects registered with runtime
    // Arguments:
    //  * obj_id : obj_id of object that led to need of sync, or None
    pub fn sync(&mut self, obj_id: Option<ObjId>) {
        self.internal_sync(obj_id, None);
    }

    // Method: internal_sync, Blocking
    // Syncs all objects registered with runtime fully if tx_idx is None, or up to tx_idx
    // Arguments:
    // * obj_id : obj_id of object that led to need of sync, or None
    // * tx_idx: sync up to transaction idx if some
    // Returns:
    // * returns TxState::None if tx_idx is None, or the state of transaction tx_idx if tx_idx is some
    pub fn internal_sync(&mut self, obj_id: Option<ObjId>, tx_idx: Option<LogIndex>) -> TxState {
        use indexed_queue::LogData::{LogEntry, LogSnapshot};
        // during transaction, record read, return
        if obj_id.is_some() {
            if self.tx_mode {
                let obj_id = obj_id.unwrap();
                self.reads.insert(obj_id, self.version[&obj_id]);
                return TxState::None;
            }

        }

        // sync all objects runtime tracks
        let rx = self.iq.stream(&self.obj_ids, self.global_idx + 1, None);
        // process and send updates to relevant callbacks
        loop {
            match rx.recv() {
                Ok(LogEntry(mut e)) => {
                    // update global index to entry index
                    let e_idx = e.idx.clone().expect("index does not exist");
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

                    // report to pre update callbakcs
                    for cb in self.pre_callbacks.iter_mut() {
                        let e = e.clone();
                        cb(e);
                    }

                    // report updates to callbacks
                    for op in &e.operations {
                        // every operation is a write, so we update object version
                        *(self.version
                              .get_mut(&op.obj_id)
                              .expect("version for object must exist")) += 1;

                        if !self.obj_ids.contains(&op.obj_id) {
                            // entry also has operation on object not tracked
                            continue;
                        }

                        let obj_id = op.obj_id;
                        match op.operator {
                            LogOp::Op(ref operator) => {
                                // operation on tracked object sent to interested data structure
                                let mut callbacks = self.callbacks
                                                        .get_mut(&obj_id)
                                                        .expect("callbacks for object must exist");
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

                    // report to post update callbacks
                    for cb in self.post_callbacks.iter_mut() {
                        let e = e.clone();
                        cb(e);
                    }

                    // return to client waiting on current log entry
                    if same_idx {
                        return e.tx_state.clone();
                    }
                }
                Ok(LogSnapshot(s)) => {
                    self.global_idx = s.idx as LogIndex;

                    if !self.obj_ids.contains(&s.obj_id) {
                        // not interested in received snapshot
                        continue;
                    }

                    let obj_id = s.obj_id;
                    let idx = s.idx;
                    let callbacks = self.callbacks
                                        .get_mut(&obj_id)
                                        .expect("snapshot callback must exist");
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

    // Method: catch_up, Blocking
    // Syncs state of obj_id and reports updates via callback c
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
                    println!("got snap");
                    let snapshot = s.payload;
                    (*c)(s.idx, Operation::from_snapshot(obj_id, snapshot));
                }
                Err(_) => break,
            };
        }
    }

    // Method: register_object
    // Registers obj_id in runtime and sync sobject to most recent state
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
    pub fn register_pre_callback(&mut self, c: Box<EntryCallback>) {
        self.pre_callbacks.push(c);
    }
    pub fn register_post_callback(&mut self, c: Box<EntryCallback>) {
        self.post_callbacks.push(c);
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
