// smr/ds.rs
// State Machine Replicated Data Structures

extern crate rustc_serialize;
use self::rustc_serialize::json;
use self::rustc_serialize::{Encodable, Decodable, Encoder, Decoder};

use runtime::{Runtime, Encryptor};
use indexed_queue::{Operation, IndexedQueue, State, LogOp};
use std::sync::{Arc, Mutex, MutexGuard};
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct Register<Q, Secure> {
    runtime: Option<Arc<Mutex<Runtime<Q, Secure>>>>,
    obj_id: i32,

    pub data: Arc<Mutex<i32>>,
}

impl<Q, S> Decodable for Register<Q, S> {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let data = try!(Decodable::decode(d));
        let reg: Register<Q, S> = Register::default(data);
        let res: Result<Self, D::Error> = Ok(reg);
        return res;
    }
}

impl<Q, Secure> Encodable for Register<Q, Secure> {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let data = self.data.lock().unwrap();
        data.encode(s)
    }
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub enum RegisterOp {
    Write {
        data: i32,
    },
}

// where Q: IndexedQueue + Send + Clone,
//      Secure: Encryptor + Send + Clone
impl<Q, Secure> Register<Q, Secure> {
    pub fn new(aruntime: &Arc<Mutex<Runtime<Q, Secure>>>,
               obj_id: i32,
               data: i32)
               -> Register<Q, Secure> {
        let reg = Register {
            obj_id: obj_id,
            runtime: Some(aruntime.clone()),
            data: Arc::new(Mutex::new(data)),
        };
        return reg;
    }

    fn default(data: i32) -> Register<Q, Secure> {
        Register {
            obj_id: 0,
            data: Arc::new(Mutex::new(data)),
            runtime: None,
        }
    }
}


impl<Q, Secure> Register<Q, Secure>
    where Q: 'static + IndexedQueue + Send + Clone,
          Secure: 'static + Encryptor + Send + Clone
{
    fn with_runtime<R, T, F>(&self, f: F) -> T
        where F: FnOnce(MutexGuard<Runtime<Q, Secure>>) -> T
    {
        assert!(self.runtime.is_some(), "invalid runtime");
        self.runtime
            .as_ref()
            .map(|runtime| {
                let runtime = runtime.lock().unwrap();
                f(runtime)
            })
            .unwrap()
    }

    pub fn start(&mut self) {
        self.with_runtime::<(), _, _>(|mut runtime| {
            let mut reg = self.clone();
            runtime.register_object(self.obj_id,
                                    Box::new(move |_, op: Operation| reg.callback(op)));
        });
    }

    pub fn read(&mut self) -> i32 {
        self.with_runtime::<i32, _, _>(|mut runtime| {
            runtime.sync(Some(self.obj_id));
            self.data.lock().unwrap().clone()
        })
    }

    pub fn write(&mut self, val: i32) {
        self.with_runtime::<(), _, _>(|mut runtime| {
            let op = RegisterOp::Write { data: val };
            runtime.append(self.obj_id, State::Encoded(json::encode(&op).unwrap()));
        });
    }

    pub fn callback(&mut self, op: Operation) {
        match op.operator {
            LogOp::Op(State::Encoded(ref s)) => {
                // TODO: with multiple-TX
                let op = json::decode(&s).unwrap();
                match op {
                    RegisterOp::Write{data} => {
                        let mut m_data = self.data.lock().unwrap();
                        *m_data = data;
                    }
                }
            }
            LogOp::Snapshot(State::Encoded(ref s)) => {
                let reg = json::decode(&s).unwrap();
                *self = reg;
            }
            _ => {
                unimplemented!();
            }
        }
    }
}

#[derive(Clone)]
pub struct BTMap<K, V, Q, Secure> {
    runtime: Option<Arc<Mutex<Runtime<Q, Secure>>>>,
    obj_id: i32,

    pub data: Arc<Mutex<BTreeMap<K, V>>>,
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub enum BTMapOp<K, V> {
    Insert {
        key: K,
        val: V,
    },
}

impl<K, V, Q, S> Decodable for BTMap<K, V, Q, S>
    where K: Encodable + Decodable + Ord,
          V: Encodable + Decodable
{
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let data = try!(Decodable::decode(d));
        let btmap: BTMap<K, V, Q, S> = BTMap::default(data);
        let res: Result<Self, D::Error> = Ok(btmap);
        return res;
    }
}

impl<K, V, Q, Secure> Encodable for BTMap<K, V, Q, Secure>
    where K: Encodable + Decodable + Ord,
          V: Encodable + Decodable
{
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let data = self.data.lock().unwrap();
        data.encode(s)
    }
}

impl<K, V, Q, Secure> BTMap<K, V, Q, Secure> {
    pub fn new(aruntime: &Arc<Mutex<Runtime<Q, Secure>>>,
               obj_id: i32,
               data: BTreeMap<K, V>)
               -> BTMap<K, V, Q, Secure> {
        let btmap = BTMap {
            obj_id: obj_id,
            runtime: Some(aruntime.clone()),
            data: Arc::new(Mutex::new(data)),
        };
        return btmap;
    }

    fn default(data: BTreeMap<K, V>) -> BTMap<K, V, Q, Secure> {
        BTMap {
            obj_id: 0,
            data: Arc::new(Mutex::new(data)),
            runtime: None,
        }
    }
}

impl<K, V, Q, Secure> BTMap<K, V, Q, Secure>
    where K: 'static + Ord + Send + Clone + Encodable + Decodable,
          V: 'static + Ord + Send + Clone + Encodable + Decodable,
          Q: 'static + IndexedQueue + Send + Clone,
          Secure: 'static + Encryptor + Send + Clone
{
    fn with_runtime<R, T, F>(&self, f: F) -> T
        where F: FnOnce(MutexGuard<Runtime<Q, Secure>>) -> T
    {
        assert!(self.runtime.is_some(), "invalid runtime");
        self.runtime
            .as_ref()
            .map(|runtime| {
                let runtime = runtime.lock().unwrap();
                f(runtime)
            })
            .unwrap()
    }

    pub fn start(&mut self) {
        self.with_runtime::<(), _, _>(|mut runtime| {
            let mut obj = self.clone();
            runtime.register_object(self.obj_id,
                                    Box::new(move |_, op: Operation| obj.callback(op)));

        });
    }

    pub fn get(&self, k: &K) -> Option<V> {
        self.with_runtime::<V, _, _>(|mut runtime| {
            runtime.sync(Some(self.obj_id));
            let data = self.data.lock().unwrap();
            data.get(k).cloned()
        })
    }

    pub fn insert(&mut self, k: K, v: V) {
        self.with_runtime::<(), _, _>(|mut runtime| {
            let op = BTMapOp::Insert { key: k, val: v };
            runtime.append(self.obj_id, State::Encoded(json::encode(&op).unwrap()));
        });
    }

    pub fn callback(&mut self, op: Operation) {
        match op.operator {
            LogOp::Op(State::Encoded(ref s)) => {
                let op = json::decode(&s).unwrap();
                match op {
                    BTMapOp::Insert{key: k, val: v} => {
                        let mut m_data = self.data.lock().unwrap();
                        m_data.insert(k, v);
                    }
                }
            }
            LogOp::Snapshot(State::Encoded(ref s)) => {
                let obj = json::decode(&s).unwrap();
                *self = obj;
            }
            _ => {
                unimplemented!();
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::char;
    use super::{Register, BTMap};
    use runtime::{Runtime, Identity};
    use indexed_queue::{InMemoryQueue, SharedQueue, ObjId};
    use std::sync::{Arc, Mutex};

    #[test]
    fn register_read_write() {
        let q = InMemoryQueue::new();
        let runtime: Runtime<InMemoryQueue, Identity> = Runtime::new(q);
        let aruntime = Arc::new(Mutex::new(runtime));
        let n = 5;
        let obj_id = 1;
        let mut data = 15;
        let mut reg = Register::new(&aruntime, obj_id, data);
        reg.start();
        assert_eq!(data, reg.read());

        for _ in 0..n {
            data += 5;
            reg.write(data);
            assert_eq!(data, reg.read());
        }
        match reg.runtime {
            Some(ref runtime) => {
                assert_eq!(runtime.lock().unwrap().global_idx, n - 1);
            }
            None => panic!("invalid runtime"),
        }
    }

    #[test]
    fn btmap_read_write() {
        let q = InMemoryQueue::new();
        let runtime: Runtime<InMemoryQueue, Identity> = Runtime::new(q);
        let aruntime = Arc::new(Mutex::new(runtime));
        let n = 5;
        let obj_id = 1;
        let mut btmap = BTMap::new(&aruntime, obj_id, BTreeMap::new());
        btmap.start();

        for key in 0..n {
            let mut val = String::from("hello_");
            val.push(char::from_u32(key as u32).unwrap());
            btmap.insert(key, val.clone());
            assert_eq!(val, btmap.get(&key).unwrap());
        }


        assert!(btmap.runtime.is_some(), "invalid runtime");
        btmap.runtime.map(|runtime| {
            assert_eq!(runtime.lock().unwrap().global_idx, n - 1);
        });
    }

    #[test]
    fn multiple_clients() {
        let q = SharedQueue::new();
        let runtime: Runtime<SharedQueue, Identity> = Runtime::new(q);
        let aruntime = Arc::new(Mutex::new(runtime));

        let mut reg1 = Register::new(&aruntime, 1 as ObjId, 1);
        let mut reg2 = Register::new(&aruntime, 2 as ObjId, 2);
        reg1.start();
        reg2.start();

        // reg1: 1 + 2 + 3 + 2 + 3
        // reg2: 2^5
        for _ in 1..3 {
            for i in 2..4 {
                let x = reg1.read();
                reg1.write(x + i)
            }

            for _ in 2..4 {
                let x = reg2.read();
                reg2.write(x * 2);
            }
        }

        let mut reg1b = Register::new(&aruntime, 1 as ObjId, 10);
        reg1b.start();
        let mut reg2b = Register::new(&aruntime, 2 as ObjId, 20);
        reg2b.start();
        assert_eq!(reg1b.read(), 11);
        assert_eq!(reg2b.read(), 32);

        reg1b.write(100);
        assert_eq!(reg1.read(), 100);
    }

    #[test]
    fn transaction() {
        let q = SharedQueue::new();
        let runtime: Runtime<SharedQueue, Identity> = Runtime::new(q);
        let aruntime = Arc::new(Mutex::new(runtime));

        let mut reg1 = Register::new(&aruntime, 1 as ObjId, 10);
        let mut reg2 = Register::new(&aruntime, 2 as ObjId, 20);
        let mut reg3 = Register::new(&aruntime, 3 as ObjId, 0);
        reg1.start();
        reg2.start();
        reg3.start();

        {
            let mut runtime = aruntime.lock().unwrap();
            runtime.begin_tx();
        }
        let x = reg1.read();
        let y = reg2.read();
        reg3.write(x + y + 1);
        {
            let mut runtime = aruntime.lock().unwrap();
            runtime.end_tx();
        }
        assert_eq!(reg3.read(), 31);
    }
}
