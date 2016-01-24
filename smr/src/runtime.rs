extern crate rustc_serialize;

use std::collections::VecDeque;
use std::fmt;
use std::marker::PhantomData;
//use self::rustc_serialize::json;
use self::rustc_serialize::{Encodable, Decodable};

use std::sync::mpsc;

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

pub trait IndexedQueue<T: Encodable + Decodable> {
    fn append(&mut self, e: T);
    fn stream_from(&self, idx: usize) -> mpsc::Receiver<T>;
}

pub struct InMemoryQueue<LogEntry: Encodable + Decodable> {
    q: VecDeque<LogEntry>,
}

impl<LogEntry: Encodable + Decodable> InMemoryQueue<LogEntry> {
    pub fn new() -> InMemoryQueue<LogEntry> {
        return InMemoryQueue {
            q: VecDeque::new(),
        }
    }
}

impl<T: Encodable + Decodable + fmt::Debug + Copy + Clone> IndexedQueue<T> for InMemoryQueue<T> {
    fn append (&mut self, e: T) {
        println!("InMemoryQueue::append {:?}", e);
        self.q.push_back(e);
    }

    fn stream_from(&self, idx: usize) -> mpsc::Receiver<T> {
        println!("InMemoryQueue::stream_from idx {}", idx);
        let (tx, rx) = mpsc::channel();
        for i in idx..self.q.len() {
            tx.send(self.q[i]).unwrap();
        }
        return rx;
    }
}

// templated with DS type and object types
pub struct Runtime<Q: Encodable + Decodable, T: IndexedQueue<Q>> {
    iq: T,
    pd: PhantomData<Q>,

    // map from obj_id to callbacks

    // map from obj_id to last updated index
}

impl<Q: Encodable + Decodable, T: IndexedQueue<Q>> Runtime<Q, T> {
    pub fn append(&mut self, entry: Q) {
       self.iq.append(entry);
    }

    fn sync(obj_id: i32) {
        // stream from indexed queue

        // send entries through data structures callbacks
    }

}

impl<Q: Encodable + Decodable, T: IndexedQueue<Q>> Runtime<Q, T> {
    pub fn new(iq: T) -> Runtime<Q, T> {
        return Runtime {
            iq: iq,
            pd: PhantomData,
        }
     }
}
