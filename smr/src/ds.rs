// smr/ds.rs
// State Machine Replicated Data Structures

extern crate rustc_serialize;
use self::rustc_serialize::json;
use self::rustc_serialize::{Encodable, Decodable, Encoder, Decoder};

use runtime::Runtime;
use indexed_queue::{Operation, IndexedQueue, State, LogOp};
use encryptors::{MetaEncryptor, Int, Addable};

use std::sync::{Arc, Mutex, MutexGuard};
use std::collections::BTreeMap;
use std::ops::Add;

fn convert_from_addable_addable(secure: &Option<MetaEncryptor>, a: Addable) -> Addable {
    return a;
}

fn convert_from_int_addable(secure: &Option<MetaEncryptor>, val: i32) -> Addable {
    secure.as_ref()
          .map(|secure| secure.encrypt_ahe(Int::from(val)))
          .unwrap()
}

fn convert_from_addable_int(secure: &Option<MetaEncryptor>, a: Addable) -> i32 {
    match secure {
        &Some(ref secure) => {
            match secure.decrypt_ahe::<i32>(a) {
                Ok(data) => data,
                Err(e) => panic!("error decrypting {}", e), 
            }
        }
        &None => panic!("no secure given"),
    }
}


pub type IntRegister<Q> = Register<Q, i32>;

impl<Q> IntRegister<Q> where Q: 'static + IndexedQueue + Send + Clone
{
    pub fn new(aruntime: &Arc<Mutex<Runtime<Q>>>, obj_id: i32, data: i32) -> IntRegister<Q> {
        let reg = Register::with_callbacks(aruntime,
                                           obj_id,
                                           data,
                                           Box::new(convert_from_addable_int),
                                           Box::new(convert_from_int_addable));

        reg as IntRegister<Q>
    }
}

pub type AddableRegister<Q> = Register<Q, Addable>;

impl<Q> AddableRegister<Q> where Q: 'static + IndexedQueue + Send + Clone
{
    pub fn new(aruntime: &Arc<Mutex<Runtime<Q>>>,
               obj_id: i32,
               data: Addable)
               -> AddableRegister<Q> {
        let reg = Register::with_callbacks(aruntime,
                                           obj_id,
                                           data,
                                           Box::new(convert_from_addable_addable),
                                           Box::new(convert_from_addable_addable));

        reg as AddableRegister<Q>
    }
}

#[derive(Clone)]
pub struct Register<Q, I> {
    runtime: Option<Arc<Mutex<Runtime<Q>>>>,
    obj_id: i32,
    convert_from: Option<Arc<Box<Fn(&Option<MetaEncryptor>, Addable) -> I + Send + Sync>>>,
    convert_to: Option<Arc<Box<Fn(&Option<MetaEncryptor>, I) -> Addable + Send + Sync>>>,

    pub data: Arc<Mutex<I>>,
    secure: Option<MetaEncryptor>,
}

impl<Q, I: Decodable> Decodable for Register<Q, I> {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let data = try!(Decodable::decode(d));
        let reg: Register<Q, I> = Register::default(data);
        let res: Result<Self, D::Error> = Ok(reg);
        return res;
    }
}

impl<Q, I: Encodable> Encodable for Register<Q, I> {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let data = self.data.lock().unwrap();
        data.encode(s)
    }
}


#[derive(RustcEncodable, RustcDecodable, Debug)]
pub enum RegisterOp<T> {
    Write {
        data: T,
    },
}

impl<Q, I> Register<Q, I> {
    pub fn with_callbacks(aruntime: &Arc<Mutex<Runtime<Q>>>,
                          obj_id: i32,
                          data: I,
                          conv: Box<Fn(&Option<MetaEncryptor>, Addable) -> I + Send + Sync>,
                          conv_back: Box<Fn(&Option<MetaEncryptor>, I) -> Addable + Send + Sync>)
                          -> Register<Q, I> {
        let reg = Register {
            obj_id: obj_id,
            runtime: Some(aruntime.clone()),
            convert_from: Some(Arc::new(conv)),
            convert_to: Some(Arc::new(conv_back)),
            data: Arc::new(Mutex::new(data)),
            secure: aruntime.lock().unwrap().secure.clone(),
        };
        return reg;
    }

    fn default(data: I) -> Register<Q, I> {
        Register {
            obj_id: 0,
            data: Arc::new(Mutex::new(data)),
            convert_from: None,
            convert_to: None,
            runtime: None,
            secure: None,
        }
    }
}

impl<Q, I> Register<Q, I>
    where Q: 'static + IndexedQueue + Send + Clone,
          I: 'static + Encodable + Decodable + Send + Clone
{
    fn with_runtime<R, T, F>(&self, f: F) -> T
        where F: FnOnce(MutexGuard<Runtime<Q>>) -> T
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

    pub fn read(&mut self) -> I {
        self.with_runtime::<I, _, _>(|mut runtime| {
            runtime.sync(Some(self.obj_id));
            self.data.lock().unwrap().clone()
        })
    }

    pub fn write(&mut self, val: I) {
        self.with_runtime::<(), _, _>(|mut runtime| {
            let secure = &self.secure;
            let data: Addable = self.convert_to.as_ref().map(|ct| ct(secure, val)).unwrap();

            let encrypted_op = RegisterOp::Write { data: data };
            let op = json::encode(&encrypted_op).unwrap();
            runtime.append(self.obj_id, State::Encrypted(op.into_bytes()));
        });
    }

    pub fn get_data(&self, data: Addable) -> I {
        let secure = &self.secure;
        self.convert_from.as_ref().map(|cf| cf(secure, data)).unwrap()
    }

    pub fn callback(&mut self, op: Operation) {
        match op.operator {
            LogOp::Op(State::Encrypted(ref bytes)) => {
                let s = String::from_utf8(bytes.clone()).unwrap();
                let encrypted_op = json::decode(&s).unwrap();
                match encrypted_op {
                    RegisterOp::Write{data} => {
                        let data = self.get_data(data);
                        let mut m_data = self.data.lock().unwrap();
                        *m_data = data;
                    }
                }
            }
            LogOp::Op(State::Encoded(ref s)) => {
                let op = json::decode(&s).unwrap();
                match op {
                    RegisterOp::Write{data} => {
                        let mut m_data = self.data.lock().unwrap();
                        *m_data = data;
                    }
                }
            }
            // could get TX succeeded, mark the variable
            LogOp::Snapshot(State::Encoded(ref s)) => {
                let mut reg: Register<Q, I> = json::decode(&s).unwrap();
                reg.convert_from = self.convert_from.clone();
                reg.convert_to = self.convert_to.clone();
                reg.runtime = self.runtime.clone();
                *self = reg;
            }
            _ => {
                unimplemented!();
            }
        }
    }
}

#[derive(Clone)]
pub struct BTMap<K, V, Q> {
    runtime: Option<Arc<Mutex<Runtime<Q>>>>,
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

impl<K, V, Q> Decodable for BTMap<K, V, Q>
    where K: Encodable + Decodable + Ord,
          V: Encodable + Decodable
{
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let data = try!(Decodable::decode(d));
        let btmap: BTMap<K, V, Q> = BTMap::default(data);
        let res: Result<Self, D::Error> = Ok(btmap);
        return res;
    }
}

impl<K, V, Q> Encodable for BTMap<K, V, Q>
    where K: Encodable + Decodable + Ord,
          V: Encodable + Decodable
{
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let data = self.data.lock().unwrap();
        data.encode(s)
    }
}

impl<K, V, Q> BTMap<K, V, Q> {
    pub fn new(aruntime: &Arc<Mutex<Runtime<Q>>>,
               obj_id: i32,
               data: BTreeMap<K, V>)
               -> BTMap<K, V, Q> {
        let btmap = BTMap {
            obj_id: obj_id,
            runtime: Some(aruntime.clone()),
            data: Arc::new(Mutex::new(data)),
        };
        return btmap;
    }

    fn default(data: BTreeMap<K, V>) -> BTMap<K, V, Q> {
        BTMap {
            obj_id: 0,
            data: Arc::new(Mutex::new(data)),
            runtime: None,
        }
    }
}

impl<K, V, Q> BTMap<K, V, Q>
    where K: 'static + Ord + Send + Clone + Encodable + Decodable,
          V: 'static + Ord + Send + Clone + Encodable + Decodable,
          Q: 'static + IndexedQueue + Send + Clone
{
    fn with_runtime<R, T, F>(&self, f: F) -> T
        where F: FnOnce(MutexGuard<Runtime<Q>>) -> T
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
            // let op = BTMapOp::Insert { key: k, val: v };
            let encrypted_op = BTMapOp::Insert {
                key: MetaEncryptor::encrypt_ident(k),
                val: MetaEncryptor::encrypt_ident(v),
            };
            let op = json::encode(&encrypted_op).unwrap();
            runtime.append(self.obj_id, State::Encrypted(op.into_bytes()));
        });
    }

    pub fn callback(&mut self, op: Operation) {
        match op.operator {
            LogOp::Op(State::Encrypted(ref s)) => {
                let encrypted_op = json::decode(&String::from_utf8(s.clone()).unwrap()).unwrap();
                match encrypted_op {
                    BTMapOp::Insert{key: k, val: v} => {
                        let k = MetaEncryptor::decrypt_ident(k);
                        let v = MetaEncryptor::decrypt_ident(v);
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
    use super::{BTMap, IntRegister};
    use runtime::Runtime;
    use indexed_queue::{InMemoryQueue, SharedQueue, ObjId, TxState};
    use std::sync::{Arc, Mutex};
    use encryptors::MetaEncryptor;

    #[test]
    fn register_read_write() {
        let q = InMemoryQueue::new();
        let runtime: Runtime<InMemoryQueue> = Runtime::new(q, Some(MetaEncryptor::new()));
        let aruntime = Arc::new(Mutex::new(runtime));
        let n = 5;
        let obj_id = 1;
        let mut data = 15;
        let mut reg = IntRegister::new(&aruntime, obj_id, data);
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
        let runtime: Runtime<InMemoryQueue> = Runtime::new(q, Some(MetaEncryptor::new()));
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
    fn multiple_objects() {
        let q = SharedQueue::new();
        let runtime: Runtime<SharedQueue> = Runtime::new(q, Some(MetaEncryptor::new()));
        let aruntime = Arc::new(Mutex::new(runtime));

        let mut reg1 = IntRegister::new(&aruntime, 1 as ObjId, 1);
        let mut reg2 = IntRegister::new(&aruntime, 2 as ObjId, 2);
        let mut btmap = BTMap::new(&aruntime, 3 as ObjId, BTreeMap::new());
        reg1.start();
        reg2.start();
        btmap.start();

        // reg1: 1 + 2 + 3 + 2 + 3
        // reg2: 2^5
        // btmap[1] = 1 * 10 + 2 + 3 = 15
        // btmap[2] = 2 * 10 + 2 + 3 = 25
        for turn in 1..3 {
            for i in 2..4 {
                let x = reg1.read();
                reg1.write(x + i)
            }

            for i in 2..4 {
                let val = btmap.get(&turn);
                btmap.insert(turn, val.unwrap_or(turn * 10) + i);
            }

            for _ in 2..4 {
                let x = reg2.read();
                reg2.write(x * 2);
            }
        }

        // check register values correctly read in new views
        let mut reg1b = IntRegister::new(&aruntime, 1 as ObjId, 10);
        reg1b.start();
        let mut reg2b = IntRegister::new(&aruntime, 2 as ObjId, 20);
        reg2b.start();
        assert_eq!(reg1b.read(), 11);
        assert_eq!(reg2b.read(), 32);
        // check btmap values correctly read in new view
        let mut btmapb: BTMap<i32, i32, _> = BTMap::new(&aruntime, 3 as ObjId, BTreeMap::new());
        btmapb.start();
        assert_eq!(btmapb.get(&1).unwrap(), 15);
        assert_eq!(btmapb.get(&2).unwrap(), 25);

        // check writing to same object via different register view
        reg1b.write(100);
        assert_eq!(reg1.read(), 100);
    }

    #[test]
    fn transaction_accepted() {
        let q = SharedQueue::new();
        let runtime: Runtime<SharedQueue> = Runtime::new(q, Some(MetaEncryptor::new()));
        let aruntime = Arc::new(Mutex::new(runtime));

        let mut reg1 = IntRegister::new(&aruntime, 1 as ObjId, 10);
        let mut reg2 = IntRegister::new(&aruntime, 2 as ObjId, 20);
        let mut reg3 = IntRegister::new(&aruntime, 3 as ObjId, 0);
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
            let tx_state = runtime.end_tx();
            assert_eq!(tx_state, TxState::Accepted);
        }
        assert_eq!(reg3.read(), 31);
    }

    #[test]
    fn transaction_aborted() {
        let q = SharedQueue::new();
        // 2 runtimes sharing the sameq
        let me = Some(MetaEncryptor::new());
        let runtime: Runtime<SharedQueue> = Runtime::new(q.clone(), me.clone());
        let aruntime = Arc::new(Mutex::new(runtime));
        let runtime_2: Runtime<SharedQueue> = Runtime::new(q, me);
        let aruntime_2 = Arc::new(Mutex::new(runtime_2));

        // start user1
        let mut user1_reg1 = IntRegister::new(&aruntime, 1 as ObjId, 10);
        let mut user1_reg2 = IntRegister::new(&aruntime, 2 as ObjId, 20);
        user1_reg1.start();
        user1_reg2.start();
        // start user 2
        let mut user2_reg1 = IntRegister::new(&aruntime_2, 1 as ObjId, 10);
        let mut user2_reg2 = IntRegister::new(&aruntime_2, 2 as ObjId, 20);
        user2_reg1.start();
        user2_reg2.start();

        // user 1 starts transaction
        {
            {
                let mut runtime = aruntime.lock().unwrap();
                runtime.begin_tx();
            }
            let x = user1_reg1.read();
            user1_reg2.write(x + 1);
        }

        // user 2 invalidates user 1's transaction
        {
            user2_reg1.write(1000);
        }

        // user1 checks transaction
        {
            let mut runtime = aruntime.lock().unwrap();
            let tx_state = runtime.end_tx();
            assert_eq!(tx_state, TxState::Aborted);
        }

        assert_eq!(user1_reg1.read(), 1000);
        assert_eq!(user2_reg1.read(), 1000);
        assert_eq!(user1_reg2.read(), 20);
        assert_eq!(user2_reg2.read(), 20);
    }

}
