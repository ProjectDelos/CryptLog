extern crate smr;
extern crate rand;
extern crate chan;
extern crate crossbeam;
extern crate time;

use rand::Rng;
use smr::maps::{StringBTMap, EncBTMap};
use smr::runtime::Runtime;
use smr::indexed_queue::{IndexedQueue, HttpClient, DynamoQueue, SharedQueue, ObjId};
use std::sync::{Arc, Mutex};
use smr::vm::{VM, MapSkiplist, Snapshotter, AsyncSnapshotter};
use smr::encryptors::{MetaEncryptor};
use std::collections::{BTreeMap};
use std::thread;
use std::time::Duration;
use smr::http_server::HttpServer;
use std::io::Write;

// Need: to be able to create an indexed queue that is also clonable
trait IndexedClonable: 'static+IndexedQueue+Clone+Send+Sync {}

impl IndexedClonable for DynamoQueue {}
impl IndexedClonable for SharedQueue {}
impl IndexedClonable for HttpClient {}

// Simple trait that allows it to create a new IndexedClonable
trait NewQueue<T: IndexedClonable> {
    fn new_queue() -> T;
}

impl NewQueue<DynamoQueue> for DynamoQueue {
    fn new_queue() -> DynamoQueue {
        return DynamoQueue::new();
    }
}

impl NewQueue<SharedQueue> for SharedQueue {
    fn new_queue() -> SharedQueue {
        return SharedQueue::new();
    }
}

/*
Benchmarks the given map with a synthetic work load of w% writes per node and n nodes
*/

enum Op<K, V> {
    Write(K, V),
    Read(K),
}

fn start_vm<Q: 'static+IndexedClonable>(q: Q) -> VM<Q, MapSkiplist, AsyncSnapshotter> {
    let mut vm = VM::new(q, MapSkiplist::new(), AsyncSnapshotter::new());
    let id = 1 as ObjId;
    let map = EncBTMap::new(&vm.runtime, id, BTreeMap::new());
    let mut map_copy = map.clone();
    vm.register_object(id as ObjId,
                       Box::new(move |_, e| map_copy.callback(e)),
                       map.clone());
    vm.start();
    return vm;
}

fn gen_ops<K: Clone, V: Clone>(keys: &[K], values: &[V], n: i64, w: i64) -> Vec<Op<K,V>> {
    let mut ops : Vec<Op<K, V>> = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let r = rand::random::<i64>() % 100;
        let k = keys[rand::random::<usize>() % keys.len()].clone();
        let v = values[rand::random::<usize>() % values.len()].clone();
        if r <= w {
            ops.push(Op::Write(k, v));
        } else {
            ops.push(Op::Read(k));
        }
    }
    return ops;
}


fn bench<Q: IndexedClonable>(mode: i64, w: i64, n: i64, nops: i64, q: Q, encryptor: Option<MetaEncryptor>) {
    let keys : Vec<String> = [1..1000].into_iter().map(|_| {
        rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>()
    }).collect();
    let values : Vec<String> = [1..1000].into_iter().map(|_| {
        rand::thread_rng()
        .gen_ascii_chars()
        .take(10)
        .collect::<String>()
    }).collect();
    let mut maps : Vec<_> = [0..n].into_iter().map(|_| {
        let ops = gen_ops(&keys, &values, nops, w);
        let runtime: Runtime<Q> = Runtime::new(q.clone(), encryptor.clone());
        let mut btmap = StringBTMap::new(&Arc::new(Mutex::new(runtime)), 1, BTreeMap::new());
        btmap.start();
        (btmap, ops)
    }).collect();
    let (send, recv) = chan::async();
    let handles = maps.drain(..).map(|(mut map, mut ops)| {
        let recv = recv.clone();
        thread::spawn(move || {
            let _ = recv.recv().unwrap();
            let _ : Vec<_> = ops.drain(..).map(|op| {
                match op {
                    Op::Write(k, v) => {
                        map.insert(k, v);
                    }
                    Op::Read(k) => {
                        map.get(&k);
                    }
                }
            }).collect();
        })
    });
    let start = time::precise_time_ns();
    for _ in 0..n {
        send.send(());
    }
    let _ : Vec<_> = handles.map(|h| {
        h.join().unwrap();
    }).collect();
    let end = time::precise_time_ns();
    let mut stderr = std::io::stderr();
    writeln!(&mut stderr, "{}, {}, {}, {}, {}, {}", mode, n, w, nops, end-start, (end-start)/(nops as u64)).unwrap();
}

fn main() {
    let nops = 10000;
    /*
    Output:
    Mode, N, W, Nops, Time, Time/Nops
    */
    for n in (1..3).map(|i| { 10.pow(i) }) {
        for w in (1..3).map(|i| { i*30 }) {
            // first do an in memory shared queue
            {
                println!("Benching: n={} w={}", n, w);
                let mut q = SharedQueue::new_queue();
                // no encryption
                println!("No Encryption");
                //bench::<SharedQueue>(0, w, n, q, None);
                // homomorphic encryption
                println!("Encryption: No VM");
                q = SharedQueue::new_queue();
                let encryptor = MetaEncryptor::new();
                bench::<SharedQueue>(1, w, n, nops, q, Some(encryptor));
                // homomorphic encryption using the VM as the queue

                println!("Encryption: With VM");
                crossbeam::scope(|scope| {
                    let encryptor = MetaEncryptor::new();
                    let server_addr = String::from("127.0.0.1:67")+&n.to_string()+&(w/10).to_string();
                    let to_server_addr = String::from("http://127.0.0.1:67")+&n.to_string()+&(w/10).to_string();
                    let (send, recv) = chan::sync(0);
                    let (dsend, drecv) = chan::sync(0);
                    scope.spawn(move || {
                        let q = SharedQueue::new_queue();
                        let vm = start_vm(q);
                        let mut s = HttpServer::new(vm, &server_addr);
                        recv.recv().unwrap();
                        s.close();
                        dsend.send(());
                    });
                    thread::sleep(Duration::from_millis(100));
                    let q = HttpClient::new(&to_server_addr);
                    bench::<HttpClient>(2, w, n, nops, q, Some(encryptor));
                    send.send(());
                    drecv.recv().unwrap();
                });

            }
            // Later transition to DynamoQueue
            // bench::<DynamoQueue, DynamoQueue>(w, n);
        }
    }
    println!("Hello, world!");
}
