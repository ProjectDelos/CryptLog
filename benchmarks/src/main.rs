#![feature(const_fn)]
extern crate smr;
extern crate rand;
extern crate chan;
extern crate crossbeam;
extern crate time;

use rand::Rng;
use smr::maps::{StringBTMap, EncBTMap, UnencBTMap};
use smr::runtime::Runtime;
use smr::indexed_queue::{IndexedQueue, ContendedQueue, HttpClient, DynamoQueue, SharedQueue, ObjId};
use std::sync::{Arc, Mutex};
use smr::vm::{VM, MapSkiplist, Snapshotter, AsyncSnapshotter};
use smr::encryptors::{MetaEncryptor};
use std::collections::{BTreeMap};
use std::thread;
use std::time::Duration;
use smr::http_server::HttpServer;
use std::io::Write;use std::sync::atomic::{AtomicUsize, Ordering};



// Need: to be able to create an indexed queue that is also clonable
trait IndexedClonable: 'static+IndexedQueue+Clone+Send+Sync {}

impl IndexedClonable for DynamoQueue {}
impl IndexedClonable for SharedQueue {}
impl IndexedClonable for HttpClient {}
impl IndexedClonable for ContendedQueue {}
impl IndexedClonable for VM<SharedQueue, MapSkiplist, AsyncSnapshotter> {}
impl IndexedClonable for VM<ContendedQueue, MapSkiplist, AsyncSnapshotter> {}

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
        let r = rand::random::<i64>() % 1000;
        let k = keys[rand::random::<usize>() % keys.len()].clone();
        let v = values[rand::random::<usize>() % values.len()].clone();
        if r < w {
            ops.push(Op::Write(k, v));
        } else {
            ops.push(Op::Read(k));
        }
    }
    return ops;
}

struct BenchOpts {
    mode: i64,
    w: i64,
    n: i64,
    nops: i64,
}

impl BenchOpts {
    fn output_csv(&self, t: u64) {
        let mut stderr = std::io::stderr();
        writeln!(&mut stderr, "{}, {}, {}, {}, {}, {}", self.mode, self.n, self.w, self.nops, t, t/(self.nops as u64)).unwrap();
    }
}


enum Map<Q> {
    Unenc(UnencBTMap<Q>),
    Enc(StringBTMap<Q>),
}
// bench_integration: benchmarks overall performance on a random read write load
// mode: is 1 with shared Queue, 2 with VM as the queue (this is only for outputting the csv file)
// w: is the number of writes per 100 operations
// n: is the number of clients to create
// nops: is the total number of operations
// q: is the implementor of the shared queue
// encryptor is the encryptor that is being used
fn bench_integration<Q: IndexedClonable>(opts: BenchOpts, q: Q, encryptor: Option<MetaEncryptor>) {
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
    let mut maps : Vec<_> = [0..opts.n].into_iter().map(|_| {
        let ops = gen_ops(&keys, &values, opts.nops, opts.w);
        let runtime: Runtime<Q> = Runtime::new(q.clone(), encryptor.clone());

        let mut btmap = if (opts.mode == 0 && opts.mode == 2) {
            let mut map = UnencBTMap::new(&Arc::new(Mutex::new(runtime)), 1, BTreeMap::new());
            map.start();
            Map::Unenc(map)
        } else {
            let mut map = StringBTMap::new(&Arc::new(Mutex::new(runtime)), 1, BTreeMap::new());
            map.start();
            Map::Enc(map)
        };
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
                        match map {
                            Map::Enc(ref mut map) => {
                                map.insert(k, v);
                            }
                            Map::Unenc(ref mut map) => {
                                map.insert(k, v);
                            }
                        }

                    }
                    Op::Read(k) => {
                        match map {
                            Map::Enc(ref mut map) => {
                                map.get(&k);
                            }
                            Map::Unenc(ref mut map) => {
                                map.get(&k);
                            }
                        }
                    }
                }
            }).collect();
        })
    });
    let start = time::precise_time_ns();
    for _ in 0..opts.n {
        send.send(());
    }
    let _ : Vec<_> = handles.map(|h| {
        h.join().unwrap();
    }).collect();
    let end = time::precise_time_ns();
    opts.output_csv(end-start)
}


// PORT_NUM has to increment by 1 every time it is used so that every client and server run on a new server.
static PORT_NUM: AtomicUsize = AtomicUsize::new(7000);
// http_run runs the given queue as an http server with all the clients connecting to it
fn http_run<Q: IndexedClonable>(opts: BenchOpts, q: Q, encryptor: Option<MetaEncryptor>) {
    let port = PORT_NUM.fetch_add(1, Ordering::SeqCst);
    crossbeam::scope(|scope| {
        println!("in crossbeam scope");
        // create a unique server address
        let server_addr = String::from("127.0.0.1:") + &port.to_string(); // &opts.n.to_string()+&(opts.w/10).to_string();
        let to_server_addr = String::from("http://127.0.0.1:")+ &port.to_string(); // &opts.n.to_string()+&(opts.w/10).to_string();
        println!("server addr: {}", server_addr);
        let (send, recv) = chan::sync(0);
        let (dsend, drecv) = chan::sync(0);
        scope.spawn(move || {
            let mut s = HttpServer::new(q, &server_addr);
            println!("created http server");
            recv.recv().unwrap();
            s.close();
            dsend.send(());
        });
        // wait for the server to start up
        thread::sleep(Duration::from_millis(100));
        // the new queue is the http client to the queue
        println!("created http client");
        let client = HttpClient::new(&to_server_addr);
        println!("benching integration");
        bench_integration::<HttpClient>(opts, client, encryptor);
        println!("done with bench: shutting down");
        send.send(());
        println!("signal sent");
        drecv.recv().unwrap();
        println!("signal received");
    });
    println!("done running http_run");
}

fn main() {
    let nops = 200;
    /*
    Output:
    Mode, N, W, Nops, Time, Time/Nops
    */
    for n in vec![1, 2, 4, 16, 32] {
        for w in vec![1, 5, 10, 50, 100] {
            // first do an in memory shared queue
            {
                println!("Benching: n={} w={}", n, w);
                // no encryption
                {
                    println!("No Encryption");
                    let encryptor = MetaEncryptor::new();
                    let q = ContendedQueue::new(10);
                    bench_integration(BenchOpts{mode: 0, w: w, n: n, nops: nops}, q, Some(encryptor));
                }
                {
                    println!("Encryption: No VM");
                    let encryptor = MetaEncryptor::new();
                    let q = ContendedQueue::new(10);
                    bench_integration(BenchOpts{mode: 1, w: w, n: n, nops: nops}, q, Some(encryptor));
                }
                {
                    let encryptor = MetaEncryptor::new();
                    let q = ContendedQueue::new(10);
                    let vm = start_vm(q);
                    http_run(BenchOpts{mode: 2, w: w, n: n, nops: nops}, vm, Some(encryptor));
                }
                {
                    let encryptor = MetaEncryptor::new();
                    let q = ContendedQueue::new(10);
                    let vm = start_vm(q);
                    http_run(BenchOpts{mode: 3, w: w, n: n, nops: nops}, vm, Some(encryptor));
                }
                // homomorphic encryption using the VM as the queue
            }
            // Later transition to DynamoQueue
            // bench::<DynamoQueue, DynamoQueue>(w, n);
        }
    }
    println!("Hello, world!");
}
