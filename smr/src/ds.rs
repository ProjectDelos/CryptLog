// smr/ds.rs
// State Machine Replicated Data Structures

extern crate rustc_serialize;
use self::rustc_serialize::json;

use runtime::Runtime;
use indexed_queue::{Operation, IndexedQueue, State};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Register<T: IndexedQueue + Clone + 'static> {
    runtime: Arc<Mutex<Runtime<T>>>,
    obj_id: i32,

    data: Arc<Mutex<i32>>,
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub enum RegisterOp {
    Write {
        data: i32,
    },
}

impl<T: IndexedQueue + Clone> Register<T> {
    fn new(aruntime: &Arc<Mutex<Runtime<T>>>, obj_id: i32, data: i32) -> Register<T> {
        let reg = Register {
            obj_id: obj_id,
            runtime: aruntime.clone(),
            data: Arc::new(Mutex::new(data)),
        };
        {
            let mut runtime = reg.runtime.lock().unwrap();
            let mut reg = reg.clone();
            runtime.register_object(obj_id, Box::new(move |op: Operation| reg.callback(op)));
        }
        return reg;
    }

    fn read(&mut self) -> i32 {
        self.runtime.lock().unwrap().sync(Some(self.obj_id));
        return self.data.lock().unwrap().clone();
    }

    fn write(&mut self, val: i32) {
        let mut runtime = self.runtime.lock().unwrap();
        let op = RegisterOp::Write { data: val };
        runtime.append(self.obj_id, State::Encoded(json::encode(&op).unwrap()));
    }

    fn callback(&mut self, op: Operation) {
        match op.operator {
            // TODO: with multiple-TX
            State::Encoded(ref s) => {
                let op = json::decode(&s).unwrap();
                match op {
                    RegisterOp::Write{data} => {
                        let mut m_data = self.data.lock().unwrap();
                        *m_data = data;
                    }
                }
            }
            _ => {
                // nothing
            }
        }
    }
}

#[cfg(test)]
mod test {
    use runtime::Runtime;
    use indexed_queue::{InMemoryQueue, SharedQueue, ObjId};
    use super::Register;
    use std::sync::{Arc, Mutex};

    #[test]
    fn read_write_register() {
        let runtime = Arc::new(Mutex::new(Runtime::new(InMemoryQueue::new())));
        let n = 5;
        let obj_id = 1;
        let mut data = 15;
        let mut reg = Register::new(&runtime, obj_id, data);
        assert_eq!(data, reg.read());

        for _ in 0..n {
            data += 5;
            reg.write(data);
            assert_eq!(data, reg.read());
        }
        assert_eq!(reg.runtime.lock().unwrap().global_idx, n - 1);
    }

    #[test]
    fn multiple_clients() {
        let runtime = Arc::new(Mutex::new(Runtime::new(SharedQueue::new())));

        let mut reg1 = Register::new(&runtime, 1 as ObjId, 1);
        let mut reg2 = Register::new(&runtime, 2 as ObjId, 2);

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
        println!("reg1 {} reg2 {}", reg1.read(), reg2.read());

        let mut reg1b = Register::new(&runtime, 1 as ObjId, 10);
        let mut reg2b = Register::new(&runtime, 2 as ObjId, 20);
        assert_eq!(reg1b.read(), 11);
        assert_eq!(reg2b.read(), 32);

        reg1b.write(100);
        assert_eq!(reg1.read(), 100);
    }
}
