extern crate rustc_serialize;

use std::fmt;
use std::collections::VecDeque;

//use self::rustc_serialize::json;
use self::rustc_serialize::{Encodable, Decodable};

use std::sync::mpsc;

pub trait IndexedQueue<T>
    where T: Encodable + Decodable {
    fn append(&mut self, e: T);
    fn stream_from(&self, idx: usize) -> mpsc::Receiver<T>;
}

pub struct InMemoryQueue<T>
    where T: Encodable + Decodable {
    q: VecDeque<T>,
}

impl<T> InMemoryQueue<T>
    where T: Encodable + Decodable {
    pub fn new() -> InMemoryQueue<T> {
        return InMemoryQueue {
            q: VecDeque::new(),
        }
    }
}

impl<T> IndexedQueue<T> for InMemoryQueue<T>
    where T: Encodable + Decodable + fmt::Debug + Copy + Clone {
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
