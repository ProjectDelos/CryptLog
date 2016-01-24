extern crate rustc_serialize;

use std::marker::PhantomData;
//use self::rustc_serialize::json;
use self::rustc_serialize::{Encodable, Decodable};

use indexed_queue::IndexedQueue;

// templated with DS type and object types
pub struct Runtime<Q, T>
    where Q: Encodable + Decodable,
          T: IndexedQueue<Q> {
    iq: T,
    pd: PhantomData<Q>,

    // map from obj_id to callbacks

    // map from obj_id to last updated index
}

impl<Q, T> Runtime<Q, T>
    where Q: Encodable + Decodable,
          T: IndexedQueue<Q> {
    pub fn append(&mut self, entry: Q) {
       self.iq.append(entry);
    }

    fn sync(obj_id: i32) {
        // stream from indexed queue

        // send entries through data structures callbacks
    }

}

impl<Q, T> Runtime<Q, T>
    where Q: Encodable + Decodable,
          T: IndexedQueue<Q>{
    pub fn new(iq: T) -> Runtime<Q, T> {
        return Runtime {
            iq: iq,
            pd: PhantomData,
        }
     }
}

#[cfg(test)]
mod test {
    use super::Runtime;
    use indexed_queue::InMemoryQueue;

    #[derive(RustcDecodable, RustcEncodable, Debug, Copy, Clone)]
    pub struct IntEntry {
        data: usize,
    }

    impl IntEntry {
        pub fn new(data: usize) -> IntEntry {
            return IntEntry {
                data: data,
            }
        }

        pub fn data(&self) -> usize {
            return self.data;
        }
    }

    #[test]
    fn create_runtime() {
        let mut r: Runtime<IntEntry, InMemoryQueue<IntEntry>> =
            Runtime::new(
            InMemoryQueue::new());
        r.append(IntEntry::new(10));
    }
}
