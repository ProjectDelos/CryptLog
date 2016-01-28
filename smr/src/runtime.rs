extern crate rustc_serialize;

//use self::rustc_serialize::json;

use std::collections::{HashMap};
use indexed_queue::{IndexedQueue, Entry, ObjId, State};

pub type Callback = FnMut(Entry);
use indexed_queue::LogIndex;

pub struct Runtime<T>
    where T: IndexedQueue {
    iq: T,

    callbacks: HashMap<ObjId, Vec<Box<Callback>>>,  
    pub index: HashMap<ObjId, LogIndex>,

    // Transaction semantics
    read_set: Vec<State>,
    write_set: Vec<State>,
    operations: Vec<State>,
    tx_mode: bool,
}

impl<T> Runtime<T>
    where T: IndexedQueue {

    pub fn append(&mut self, obj_id: ObjId, data: State) {
        if self.tx_mode {
            self.write_set.push(State::Encoded(obj_id.to_string()));
            self.operations.push(data);
        } else {
            self.iq.append(Entry::new(
                Vec::new(),
                vec![State::Encoded(obj_id.to_string())],
                vec![data]))
        }
    }

    // TODO: when TX Begins, log entry for TxBegin has an idx
    // Sync all objects relevant to the transaction up to that idx

    pub fn sync(&mut self, obj_id: ObjId) {
        // TODO: when TX added, acknowdledge read attempt

        // send updates to relevant callbacks
        let mut callbacks = self.callbacks.get_mut(&obj_id).unwrap();
        let rx = self.iq.stream_from(self.index[&obj_id]);
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
