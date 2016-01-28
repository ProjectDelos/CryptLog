// smr/ds.rs
// State Machine Replicated Data Structures

extern crate rustc_serialize;
use self::rustc_serialize::json;

use runtime::{Runtime};
use indexed_queue::{Entry, InMemoryQueue,  IndexedQueue, State};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Register {
    runtime: Arc<Mutex<Runtime<InMemoryQueue>>>,
    obj_id: i32,

    data: Arc<Mutex<i32>>,
}

#[derive(RustcEncodable, RustcDecodable)]
pub enum RegisterOp {
    Write{data: i32},
}

impl Register {
    fn new(aruntime: Arc<Mutex<Runtime<InMemoryQueue>>>,
           obj_id: i32, data: i32) -> Register {
        let reg = Register {
            obj_id: obj_id,
            runtime: aruntime.clone(),
            data: Arc::new(Mutex::new(data)),
        };
        {
            let mut runtime = reg.runtime.lock().unwrap();
            let mut reg = reg.clone();
            runtime.register_object(obj_id, Box::new(move |e: Entry| {
                reg.callback(e)
            }));
        }
        return reg;
    }

    fn read(&mut self) -> i32 {
        self.runtime.lock().unwrap().sync(self.obj_id);
        return self.data.lock().unwrap().clone();
    }

    fn write(&mut self, val: i32) {
        let mut runtime = self.runtime.lock().unwrap();
        let op = RegisterOp::Write{data: val};
        runtime.append(self.obj_id, State::Encoded(json::encode(&op).unwrap()));
    }

    // TODO: could just pass the operations back?
    fn callback(&mut self, e: Entry) {
        for op in &e.operations {
            match op {
                &State::Encoded(ref s) => {
                    let op = json::decode(&s).unwrap();
                    match op {
                        RegisterOp::Write{data} => {
                            let mut m_data = self.data.lock().unwrap();
                            *m_data = data;
                        }
                    }
                }
                _ => {/* nothing */}
            }
        }
    }
}

#[cfg(test)]
mod test {
    use runtime::Runtime;
    use indexed_queue::{InMemoryQueue};
    use super::Register;
    use std::sync::{Arc, Mutex};

    #[test]
    fn read_write_register() {
        let runtime = Arc::new(Mutex::new(Runtime::new(InMemoryQueue::new())));
        let n = 5;
        let obj_id = 1;
        let mut data = 15;
        let mut reg = Register::new(runtime, obj_id, data);
        assert_eq!(data, reg.read());

        for _ in 0..n {
            data += 5;
            reg.write(data);
            assert_eq!(data, reg.read());
        }
        assert_eq!(reg.runtime.lock().unwrap().index[&obj_id], n-1);
    }
}
