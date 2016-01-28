extern crate rustc_serialize;

//use self::rustc_serialize::json;

use std::collections::{HashMap};
use indexed_queue::{IndexedQueue, Entry, ObjId, State,
                    Operation, TxType, TxState, LogIndex};

pub type Callback = FnMut(Entry);
//use indexed_queue::LogIndex;

pub struct Runtime<T>
    where T: IndexedQueue {
    iq: T,

    callbacks: HashMap<ObjId, Vec<Box<Callback>>>,  
    pub index: HashMap<ObjId, LogIndex>,

    // Transaction semantics
    read_set: Vec<ObjId>,
    write_set: Vec<ObjId>,
    operations: Vec<Operation>,
    tx_mode: bool,
    begin_tx_idx: Option<LogIndex>,
}

impl<T> Runtime<T>
    where T: IndexedQueue {

    pub fn append(&mut self, obj_id: ObjId, data: State) {
        if self.tx_mode {
            self.write_set.push(obj_id);
            self.operations.push(Operation::new(obj_id, data));
        } else {
            self.iq.append(Entry::new(
                Vec::new(),
                vec![obj_id],
                vec![Operation::new(obj_id, data)],
                TxType::None,
                TxState::None));
        }
    }

    pub fn begin_tx(&mut self) {
        self.tx_mode = true;
        // dummy begintx entry
        self.begin_tx_idx = Some(self.iq.append(Entry::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            TxType::Begin,
            TxState::None)));

    }

    pub fn end_tx(&mut self) {
        self.iq.append(Entry::new(
            // TODO: move here?
            self.read_set.clone(),
            self.write_set.clone(),
            self.operations.clone(),
            TxType::End,
            TxState::None));
        self.tx_mode = false;
    }

    pub fn sync(&mut self, obj_id: ObjId) {
        let mut rx;
        if self.tx_mode {
            self.read_set.push(obj_id);
            rx = self.iq.stream_from_to(self.index[&obj_id],
                                        self.begin_tx_idx);
        } else {
            rx = self.iq.stream_from_to(self.index[&obj_id], None);
        }

        // send updates to relevant callbacks
        let mut callbacks = self.callbacks.get_mut(&obj_id).unwrap();
        loop {
            match rx.recv() {
                Ok(e) => {
                    let mut idx = self.index.get_mut(&obj_id).unwrap();
                    *idx = e.idx.unwrap();
                    for c in callbacks.iter_mut() {
                        c(e.clone());
                    }
                }
                Err(_) => break
             };
        }
    }

    pub fn register_object(&mut self, obj_id: ObjId, c: Box<Callback>) {
        if !self.callbacks.contains_key(&obj_id) {
            self.callbacks.insert(obj_id, Vec::new());
        }
        self.callbacks.get_mut(&obj_id).unwrap().push(c);

        if !self.index.contains_key(&obj_id) {
            self.index.insert(obj_id, 0);
        }
    }

}

impl<T> Runtime<T>
    where T: IndexedQueue {

    pub fn new(iq: T) -> Runtime<T> {
        return Runtime {
            iq: iq,
            callbacks: HashMap::new(),
            index: HashMap::new(),
            read_set: Vec::new(),
            write_set: Vec::new(),
            operations: Vec::new(),
            tx_mode: false,
            begin_tx_idx: None,
        }
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
