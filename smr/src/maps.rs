extern crate rustc_serialize;
use self::rustc_serialize::json;
use self::rustc_serialize::{Encodable, Decodable, Encoder, Decoder};

use runtime::Runtime;
use indexed_queue::{Operation, IndexedQueue, State, LogOp};
use encryptors::{MetaEncryptor, Encrypted, Eqable, Ordable};
use converters::{Converter, EqableConverter, OrdableConverter, ConvertersLib};

use std::sync::{Arc, Mutex, MutexGuard};
use std::collections::{HashMap, BTreeMap};
use std::hash::Hash;
use std::cmp::Eq;

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub enum MapOp<K, V> {
    Insert {
        key: K,
        val: V,
    },
}

#[derive(Clone)]
pub struct HMap<K, V, Q> {
    runtime: Option<Arc<Mutex<Runtime<Q>>>>,
    obj_id: i32,

    convert_eq: Option<EqableConverter<K>>,
    convert: Option<Converter<V>>,

    secure: Option<MetaEncryptor>,
    pub data: Arc<Mutex<HashMap<K, V>>>,
}

impl<K, V, Q> Decodable for HMap<K, V, Q>
    where K: Encodable + Decodable + Hash + Eq,
          V: Encodable + Decodable
{
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let data = try!(Decodable::decode(d));
        let hmap: HMap<K, V, Q> = HMap::default(data);
        let res: Result<Self, D::Error> = Ok(hmap);
        return res;
    }
}

impl<K, V, Q> Encodable for HMap<K, V, Q>
    where K: Encodable + Decodable + Hash + Eq,
          V: Encodable + Decodable
{
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let data = self.data.lock().unwrap();
        data.encode(s)
    }
}

// TODO: see if worth it to 'inherit' from more generic Map
impl<K, V, Q> HMap<K, V, Q> {
    pub fn new(aruntime: &Arc<Mutex<Runtime<Q>>>,
               obj_id: i32,
               data: HashMap<K, V>,
               convert: Converter<V>,
               convert_eq: EqableConverter<K>)
               -> HMap<K, V, Q> {
        let hmap = HMap {
            obj_id: obj_id,
            runtime: Some(aruntime.clone()),
            data: Arc::new(Mutex::new(data)),
            convert: Some(convert),
            convert_eq: Some(convert_eq),
            secure: aruntime.lock().unwrap().secure.clone(),
        };
        return hmap;
    }

    fn default(data: HashMap<K, V>) -> HMap<K, V, Q> {
        HMap {
            runtime: None,
            obj_id: 0,
            data: Arc::new(Mutex::new(data)),
            convert: None,
            convert_eq: None,
            secure: None,
        }
    }
}


impl<K, V, Q> HMap<K, V, Q>
    where K: 'static + Send + Clone + Encodable + Decodable + Hash + Eq,
          V: 'static + Send + Clone + Encodable + Decodable,
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
            let key = self.convert_eq
                          .as_ref()
                          .map(|convert_eq| {
                              let to = &convert_eq.to;
                              to(&self.secure, k)
                          })
                          .unwrap();
            let val = self.convert
                          .as_ref()
                          .map(|convert| {
                              let to = &convert.to;
                              to(&self.secure, v)
                          })
                          .unwrap();
            let encrypted_op = MapOp::Insert {
                key: key,
                val: val,
            };
            let op = json::encode(&encrypted_op).unwrap();
            runtime.append(self.obj_id, State::Encrypted(op.into_bytes()));
        });
    }

    pub fn get_val(&self, val: Encrypted) -> V {
        self.convert
            .as_ref()
            .map(|convert| {
                let from = &convert.from;
                from(&self.secure, val)
            })
            .unwrap()
    }

    pub fn get_key(&self, key: Eqable) -> K {
        self.convert_eq
            .as_ref()
            .map(|convert_eq| {
                let from = &convert_eq.from;
                from(&self.secure, key)
            })
            .unwrap()
    }

    pub fn callback(&mut self, op: Operation) {
        match op.operator {
            LogOp::Op(State::Encrypted(ref s)) => {
                let encrypted_op = json::decode(&String::from_utf8(s.clone()).unwrap()).unwrap();
                match encrypted_op {
                    MapOp::Insert{key: k, val: v} => {
                        let k = self.get_key(k);
                        let v = self.get_val(v);
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

pub type StringBTMap<Q> = BTMap<String, String, Q>;
impl<Q> StringBTMap<Q> {
    pub fn new(aruntime: &Arc<Mutex<Runtime<Q>>>,
               obj_id: i32,
               data: BTreeMap<String, String>)
               -> StringBTMap<Q> {
        BTMap::from(aruntime,
                    obj_id,
                    data,
                    Converter::new(ConvertersLib::encodable_from_encrypted(),
                                   ConvertersLib::encrypted_from_encodable()),
                    OrdableConverter::new(ConvertersLib::encodable_from_ordable(),
                                          ConvertersLib::ordable_from_encodable()))
    }
}

pub type EncBTMap<Q> = BTMap<Ordable, Encrypted, Q>;
impl<Q> EncBTMap<Q> {
    pub fn new(aruntime: &Arc<Mutex<Runtime<Q>>>,
               obj_id: i32,
               data: BTreeMap<Ordable, Encrypted>)
               -> EncBTMap<Q> {
        BTMap::from(aruntime,
                    obj_id,
                    data,
                    Converter::new(ConvertersLib::encrypted_from_encrypted(),
                                   ConvertersLib::encrypted_from_encrypted()),
                    OrdableConverter::new(ConvertersLib::ordable_from_ordable(),
                                          ConvertersLib::ordable_from_ordable()))
    }
}

#[derive(Clone)]
pub struct BTMap<K, V, Q> {
    runtime: Option<Arc<Mutex<Runtime<Q>>>>,
    obj_id: i32,

    convert_ord: Option<OrdableConverter<K>>,
    convert: Option<Converter<V>>,

    secure: Option<MetaEncryptor>,
    pub data: Arc<Mutex<BTreeMap<K, V>>>,
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
    pub fn from(aruntime: &Arc<Mutex<Runtime<Q>>>,
                obj_id: i32,
                data: BTreeMap<K, V>,
                convert: Converter<V>,
                convert_ord: OrdableConverter<K>)
                -> BTMap<K, V, Q> {
        let btmap = BTMap {
            obj_id: obj_id,
            runtime: Some(aruntime.clone()),
            secure: aruntime.lock().unwrap().secure.clone(),
            data: Arc::new(Mutex::new(data)),
            convert: Some(convert),
            convert_ord: Some(convert_ord),
        };
        return btmap;
    }

    fn default(data: BTreeMap<K, V>) -> BTMap<K, V, Q> {
        BTMap {
            obj_id: 0,
            runtime: None,
            secure: None,
            data: Arc::new(Mutex::new(data)),
            convert: None,
            convert_ord: None,
        }
    }
}

impl<K, V, Q> BTMap<K, V, Q>
    where K: 'static + Ord + Send + Clone + Encodable + Decodable,
          V: 'static + Send + Clone + Encodable + Decodable,
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

    // modfies local map without syncing to log
    // use only for testing map is in order
    pub fn pop_first(&mut self) -> Option<(K, V)> {
        self.with_runtime::<K, _, _>(|mut runtime| {
            runtime.sync(Some(self.obj_id));
            // println!("synced!");
            {
                let data = self.data.lock().unwrap();
                if data.is_empty() {
                    return None;
                }
            }

            let res = {
                let data = self.data.lock().unwrap();
                let (first_key, first_value) = data.iter().next().unwrap();
                (first_key.clone(), first_value.clone())
            };

            self.data.lock().unwrap().remove(&res.0);
            Some(res)
        })
    }

    pub fn get_val(&self, val: Encrypted) -> V {
        self.convert
            .as_ref()
            .map(|convert| {
                let from = &convert.from;
                from(&self.secure, val)
            })
            .unwrap()
    }

    pub fn get_key(&self, key: Ordable) -> K {
        self.convert_ord
            .as_ref()
            .map(|convert_ord| {
                let from = &convert_ord.from;
                from(&self.secure, key)
            })
            .unwrap()
    }

    pub fn insert(&mut self, k: K, v: V) {
        self.with_runtime::<(), _, _>(|mut runtime| {
            let key = self.convert_ord
                          .as_ref()
                          .map(|convert_ord| {
                              let to = &convert_ord.to;
                              to(&self.secure, k)
                          })
                          .unwrap();
            let val = self.convert
                          .as_ref()
                          .map(|convert| {
                              let to = &convert.to;
                              to(&self.secure, v)
                          })
                          .unwrap();
            let encrypted_op = MapOp::Insert {
                key: key,
                val: val,
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
                    MapOp::Insert{key: k, val: v} => {
                        let k = self.get_key(k);
                        let v = self.get_val(v);
                        let mut m_data = self.data.lock().unwrap();
                        m_data.insert(k, v);
                    }
                }
            }
            LogOp::Snapshot(State::Encoded(ref s)) => {
                println!("client gets snapshot");
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
    use super::{HMap, StringBTMap};
    use std::collections::{HashMap, BTreeMap};
    use std::char;
    use std::sync::{Arc, Mutex};
    use runtime::Runtime;
    use indexed_queue::InMemoryQueue;
    use encryptors::MetaEncryptor;
    use converters::{Converter, ConvertersLib, EqableConverter};

    #[test]
    fn hmap_read_write() {
        let q = InMemoryQueue::new();
        let runtime: Runtime<InMemoryQueue> = Runtime::new(q, Some(MetaEncryptor::new()));
        let aruntime = Arc::new(Mutex::new(runtime));
        let n = 5;
        let obj_id = 1;
        let converter: Converter<String> =
            Converter::new(ConvertersLib::encodable_from_encrypted(),
                           ConvertersLib::encrypted_from_encodable());
        let converter_eq: EqableConverter<i32> =
            EqableConverter::new(ConvertersLib::encodable_from_eqable(),
                                 ConvertersLib::eqable_from_encodable());
        let mut hmap = HMap::new(&aruntime, obj_id, HashMap::new(), converter, converter_eq);
        hmap.start();

        for key in 0..n {
            let mut val = String::from("hello_");
            val.push(char::from_u32(key as u32).unwrap());
            hmap.insert(key, val.clone());
            assert_eq!(val, hmap.get(&key).unwrap());
        }

        assert!(hmap.runtime.is_some(), "invalid runtime");
        hmap.runtime.map(|runtime| {
            assert_eq!(runtime.lock().unwrap().global_idx, (n - 1) as i64);
        });

    }

    #[test]
    fn btmap_read_write() {
        let q = InMemoryQueue::new();
        let runtime: Runtime<InMemoryQueue> = Runtime::new(q, Some(MetaEncryptor::new()));
        let aruntime = Arc::new(Mutex::new(runtime));
        let n = 5;
        let obj_id = 1;
        let mut btmap = StringBTMap::new(&aruntime, obj_id, BTreeMap::new());
        btmap.start();

        let keys = vec!["h0", "h1", "h2", "alphabet", "h0rry"];
        let vals = vec!["h0", "h1", "h2", "alphabet", "h0rry"];
        let should_be_at = vec![3, 0, 4, 1, 2];
        for i in 0..keys.len() {
            btmap.insert(String::from(keys[i].clone()), String::from(vals[i].clone()));
            assert_eq!(vals[i], btmap.get(&String::from(keys[i])).unwrap());
        }

        assert!(btmap.runtime.is_some(), "invalid runtime");
        btmap.runtime = btmap.runtime
                             .map(|runtime| {
                                 assert_eq!(runtime.lock().unwrap().global_idx, (n - 1) as i64);
                                 runtime
                             });

        for i in 0..keys.len() {
            let (key, val) = btmap.pop_first().unwrap();
            // println!("key {:?} val {:?}", key, val);
            assert_eq!(val, vals[should_be_at[i]]);
        }

    }
}
