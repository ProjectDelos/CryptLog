extern crate rustc_serialize;

//use self::rustc_serialize::json;

use std::collections::{HashMap};
use indexed_queue::{IndexedQueue, Entry};

pub type ObjId = i32;
pub type Callback = FnMut(Entry);
use indexed_queue::LogIndex;

pub struct Runtime<T>
    where T: IndexedQueue {
    iq: T,

    callbacks: HashMap<ObjId, Vec<Box<Callback>>>,  
    pub index: HashMap<ObjId, LogIndex>,
}

impl<T> Runtime<T>
    where T: IndexedQueue {

    pub fn append(&mut self, entry: Entry) {
       self.iq.append(entry);
    }

    pub fn sync(&mut self, obj_id: ObjId) {
        let mut callbacks = self.callbacks.get_mut(&obj_id).unwrap();
        let rx = self.iq.stream_from(self.index[&obj_id]);
        loop {
            match rx.recv() {
                Ok(e) => {
                    let mut idx = self.index.get_mut(&obj_id).unwrap();
                    *idx = e.idx.unwrap();
                    for c in callbacks.iter_mut() {
                        c(e);
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
        }
     }
}

#[cfg(test)]
mod test {
    use super::Runtime;
    use indexed_queue::InMemoryQueue;
    use indexed_queue::Entry;

    #[test]
    fn create_runtime() {
        let mut r: Runtime<InMemoryQueue> = Runtime::new(InMemoryQueue::new());
        r.append(Entry::new(10));
    }
}
