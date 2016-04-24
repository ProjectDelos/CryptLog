#![feature(const_fn)]
extern crate smr;
extern crate rand;
extern crate chan;
extern crate crossbeam;
extern crate time;
extern crate getopts;
extern crate rustc_serialize;

use std::sync::mpsc;
use rand::Rng;
use smr::maps::{StringBTMap, EncBTMap, UnencBTMap, BTMap};
use smr::runtime::Runtime;
use smr::indexed_queue::{IndexedQueue, ContendedQueue, HttpClient, DynamoQueue, SharedQueue, ObjId, Entry, LogData, LogIndex};
use std::sync::{Arc, Mutex};
use smr::vm::{VM, MapSkiplist, Snapshotter, AsyncSnapshotter};
use smr::encryptors::{MetaEncryptor, Ordable, Encrypted};
use std::collections::{BTreeMap, HashSet};
use std::thread;
use std::time::Duration;
use smr::http_server::HttpServer;
use std::io::Write;use std::sync::atomic::{AtomicUsize, Ordering};
use getopts::Options;
use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::marker::PhantomData;
use self::rustc_serialize::{Encodable, Decodable};
use std::fmt::Debug;

// Need: to be able to create an indexed queue that is also clonable
trait IndexedClonable: 'static+IndexedQueue+Clone+Send+Sync {}

#[derive(Clone)]
struct MockHttpQueue<Q> {
    q: Q,
}

impl<Q: IndexedClonable> MockHttpQueue<Q> {
    fn from(q : Q) -> MockHttpQueue<Q> {
        return MockHttpQueue {
            q: q,
        }
    }
}

impl<Q: IndexedClonable> IndexedQueue for MockHttpQueue<Q> {
    fn stream(&mut self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<LogData> {
        thread::sleep(Duration::from_millis(100));
        self.q.stream(obj_ids, from, to)
    }
    fn append(&mut self, e: Entry) -> LogIndex {
        thread::sleep(Duration::from_millis(100));
        self.q.append(e)
    }
}



impl<Q: IndexedClonable> IndexedClonable for MockHttpQueue<Q> {}
impl IndexedClonable for DynamoQueue {}
impl IndexedClonable for SharedQueue {}
impl IndexedClonable for HttpClient {}
impl IndexedClonable for ContendedQueue {}
impl<Q: IndexedClonable> IndexedClonable for VM<Q, MapSkiplist, AsyncSnapshotter> {}

trait QueueFactory<Q> {
    fn new_queue(&mut self) -> Q;
    fn stop(&mut self);
}

struct ContendedQueueFactory;

impl ContendedQueueFactory {
    fn new() -> ContendedQueueFactory {
        return ContendedQueueFactory{};
    }
}

impl QueueFactory<ContendedQueue> for ContendedQueueFactory {
    fn new_queue(&mut self) -> ContendedQueue {
        ContendedQueue::new(50)
    }
    fn stop(&mut self) {
        return;
    }
}

struct DynamoQueueFactory;

impl DynamoQueueFactory {
    fn new() -> DynamoQueueFactory {
        // TODO delete all old tables etc.
        return DynamoQueueFactory{};
    }
}

impl QueueFactory<DynamoQueue> for DynamoQueueFactory {
    fn new_queue(&mut self) -> DynamoQueue {
        DynamoQueue::new()
    }
    fn stop(&mut self) {
        return;
    }
}

struct MockVMClientFactory<Q: IndexedClonable, F: QueueFactory<Q>> {
    factory: F,
    phantom: PhantomData<Q>,
}

impl<Q: IndexedClonable, F: QueueFactory<Q>> MockVMClientFactory<Q, F> {
    fn new(base_factory: F) -> MockVMClientFactory<Q, F> {
        MockVMClientFactory {
            factory: base_factory,
            phantom: PhantomData,
        }
    }
}


impl<Q: IndexedClonable, F: QueueFactory<Q>> QueueFactory<MockHttpQueue<VM<Q, MapSkiplist, AsyncSnapshotter>>> for MockVMClientFactory<Q, F> {
    fn new_queue(&mut self) -> MockHttpQueue<VM<Q, MapSkiplist, AsyncSnapshotter>> {
        // Start up the vm in a separate thread
        let q = start_vm(self.factory.new_queue());
        MockHttpQueue::from(q)
    }
    fn stop(&mut self) {
        return;
    }
}



struct VMClientFactory<Q: IndexedClonable, F: QueueFactory<Q>> {
    handle: Option<thread::JoinHandle<()>>,
    stop_send: Option<chan::Sender<()>>,
    done_recv: Option<chan::Receiver<()>>,
    factory: F,
    phantom: PhantomData<Q>,
    
}

impl<Q: IndexedClonable, F: QueueFactory<Q>> VMClientFactory<Q, F> {
    fn new(base_factory: F) -> VMClientFactory<Q, F> {
        VMClientFactory {
            handle: None,
            stop_send: None,
            done_recv: None,
            factory: base_factory,
            phantom: PhantomData,
        }
    }
}

// PORT_NUM has to increment by 1 every time it is used so that every client and server run on a new server.
static PORT_NUM: AtomicUsize = AtomicUsize::new(7000);

// TODO: Make Slow wrapper for queue: Remove HTTP Server elements
impl<Q: IndexedClonable, F: QueueFactory<Q>> QueueFactory<HttpClient> for VMClientFactory<Q, F> {
    fn new_queue(&mut self) -> HttpClient {
        let port = PORT_NUM.fetch_add(1, Ordering::SeqCst);
        
        let server_addr = String::from("127.0.0.1:") + &port.to_string();
        let to_server_addr = String::from("http://127.0.0.1:")+ &port.to_string();
        
        let (stop_send, stop_recv) = chan::sync(0);
        let (done_send, done_recv) = chan::sync(0);
        
        let q = start_vm(self.factory.new_queue());
        
        let handle = thread::spawn(move || {
            let mut s = HttpServer::new(q, &server_addr);
            stop_recv.recv().expect("stop_recv does not exist");
            s.close();
            //done_send.send(());
        });
        self.handle = Some(handle);
        self.stop_send = Some(stop_send);
        self.done_recv = Some(done_recv);
        
        thread::sleep(Duration::from_millis(1000));
        let client = HttpClient::new(&to_server_addr);
        return client;
    }
    fn stop(&mut self) {
        if self.stop_send.is_none() || self.handle.is_none() {
            return;
        }
        let stop = self.stop_send.take().expect("stop_send does not exist");
        stop.send(());
        let handle = self.handle.take().expect("handle does not exist");
        handle.join().unwrap();
        // DO NOT WAIT TILL DONE
        // HYPER (the http library) currently is unable to close servers
        // It leaks the underlying threads as well as the underlying listener
        //let done = self.done_recv.take().expect("done send does not exist");
        //done.recv().expect("failed to receive from done channel");
        return;
    }
}
/*
impl <Q: IndexedClonable, F: QueueFactory<Q>> Drop for VMClientFactory<Q, F> {
    fn drop(&mut self) {
        self.stop();
    }
}*/

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

fn gen_kvs(n: i64) -> (Vec<String>, Vec<String>) {
    let mut rng = rand::StdRng::new().unwrap();
    let keys : Vec<String> = (0..n).into_iter().map(|_| {
        rng
        .gen_ascii_chars()
        .take(10)
        .collect::<String>()
    }).collect();
    let values : Vec<String> = (0..n).into_iter().map(|_| {
        rng
        .gen_ascii_chars()
        .take(10)
        .collect::<String>()
    }).collect();
    (keys, values)
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
    out: String,
    mode: i64,
    w: i64,
    n: i64,
    nops: i64,
}

impl BenchOpts {
    fn header(mut out: &mut File) {
        writeln!(&mut out, "mode, n, w, nops, delay, t_per_op").unwrap();
    }
    fn output_csv(&self, t: u64) {
        let mut out = OpenOptions::new()
                .write(true)
                .append(true)
                .open(&self.out).unwrap();
        writeln!(&mut out, "{}, {}, {}, {}, {}, {}", self.mode, self.n, self.w, self.nops, t, t/(self.nops as u64)).unwrap();
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
fn bench_integration<Q: IndexedClonable, F: QueueFactory<Q>>(mut factory: F, opts: BenchOpts) {
    let q = factory.new_queue();
    let encryptor = Some(MetaEncryptor::new());
    let (keys, values) = gen_kvs(1000);
    let mut maps : Vec<_> = (0..opts.n).into_iter().map(|_| {
        let ops = gen_ops(&keys, &values, opts.nops, opts.w);
        let runtime: Runtime<Q> = Runtime::new(q.clone(), encryptor.clone());

        let btmap = if opts.mode == 0 || opts.mode == 2 {
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

struct RecOpts {
    out: String,
    mode: i64,
    n: i64,
}

impl RecOpts {
    fn new(out: String, mode: i64, n: i64) -> RecOpts {
        RecOpts {
            out: out,
            mode: mode,
            n: n,
        }
    }
    fn header(mut out: &mut File) {
        writeln!(&mut out, "mode, n, t_per_op").unwrap();
    }
    
    fn output_csv(&self, t: u64) {
        let mut out = OpenOptions::new()
                .write(true)
                .append(true)
                .open(&self.out).unwrap();
        writeln!(&mut out, "{}, {}, {}", self.mode, self.n, t).unwrap();
    }
}

type MapFactory<Q, K, V> = Fn(&Arc<Mutex<Runtime<Q>>>, i32, BTreeMap<String, String>) -> BTMap<String, String, Q, K, V> + Send + Sync;

fn bench_recovery<Q: IndexedClonable,
                  F: QueueFactory<Q>,
                  K: 'static+Ord+Clone+Send+Encodable+Decodable+Debug+Sync,
                  V: 'static+Debug+Clone+Send+Encodable+Decodable+Sync>(map_factory: Box<MapFactory<Q, K, V>>, mut factory: F, opts: RecOpts) {
    let (keys, values) = gen_kvs(1000);

    // bench without the vm
    let mut t_total = 0;
    let mut samples = 0;
    {
        let total_samples = 20;
        let encryptor = Some(MetaEncryptor::new());
        let mut ops = gen_ops(&keys, &values, opts.n * total_samples, 1000);
        let q = factory.new_queue();
        let mut writer = map_factory(&Arc::new(Mutex::new(Runtime::new(q.clone(), encryptor.clone()))), 1, BTreeMap::new());
        writer.start();
        let mut reader = map_factory(&Arc::new(Mutex::new(Runtime::new(q.clone(), encryptor.clone()))), 1, BTreeMap::new());
        reader.start();
        let mut last_k;
        writer.insert("".to_string(), "".to_string());
        let mut i = 0;
        for op in ops.drain(..) {
            match op {
                    Op::Write(k, v) => {
                        writer.insert(k.clone(), v);
                        last_k = k;
                        // one more write has happened
                        i += 1;
                        // every opts.n writes perform a read and record the time it takes
                        if i % opts.n == 0 {
                            let start = time::precise_time_ns();
                            reader.get(&last_k);
                            let end = time::precise_time_ns();
                            t_total += end - start;
                            samples += 1;
                        }
                    }
                    Op::Read(_) => panic!("should only be writes"),
            }
        }
        factory.stop();
    }
    opts.output_csv(t_total/samples);
}

#[derive(Clone)]
struct LatencyOpts {
    out: String,
    mode: i64,
    n: i64, // number of clients writing
    t: i64, // amount of ms between reader's reads
}

impl LatencyOpts {
    fn header(mut out: &mut File) {
        writeln!(&mut out, "mode, n, delay, t_per_op").unwrap();
    }
    fn output_csv(&self, t: u64) {
        let mut out = OpenOptions::new()
                .write(true)
                .append(true)
                .open(&self.out).unwrap();

        writeln!(&mut out, "{}, {}, {}, {}", self.mode, self.n, self.t, t).unwrap();
    }
}



// Increase the number of running clients
// TODO: Test read latency: with n operations between reads (reader that continuously reads)
fn bench_read_latency<Q: IndexedClonable,
                      F: QueueFactory<Q>,
                      K: 'static+Ord+Clone+Send+Encodable+Decodable+Debug+Sync,
                      V: 'static+Debug+Clone+Send+Encodable+Decodable+Sync>(map_factory: Box<MapFactory<Q, K, V>>, mut factory: F, opts: LatencyOpts) {
    let q = factory.new_queue();
    // BUG: When nops is set too high and we are running the VM we get a connection reset by peer
    let nops = 100;
    
    let encryptor = Some(MetaEncryptor::new());
    // Generate 1000 unique key and value possibilities
    let (keys, values) = gen_kvs(1000);
    let stop = Arc::new(Mutex::new(false));
    
    // Generate n different writers
    let mut writers : Vec<_> = (0..opts.n).into_iter().map(|_| {
        // Generate a list of 1000 operations all of which are writes
        let ops = gen_ops(&keys, &values, nops, 1000);
        let runtime: Runtime<Q> = Runtime::new(q.clone(), encryptor.clone());
        let mut writer = map_factory(&Arc::new(Mutex::new(runtime)), 1, BTreeMap::new());
        writer.start();
        (writer, ops)
    }).collect();
    
    
    let (send, recv) = chan::async();
    let total_t = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(AtomicUsize::new(0));
    let read_recv = recv.clone();
    
    let r_total_t = total_t.clone();
    let r_samples = samples.clone();
    let r_stop = stop.clone();
    let t = opts.t as u64;
    let reader_handle = thread::spawn(move || {
        let runtime: Runtime<Q> = Runtime::new(q.clone(), encryptor.clone());
        let mut reader = map_factory(&Arc::new(Mutex::new(runtime)), 1, BTreeMap::new());
        reader.start();
        read_recv.recv().unwrap();
        loop {
            {
                 let s = r_stop.lock().unwrap();
                 if *s {
                     return;
                 } 
            }
            let start = time::precise_time_ns();
            reader.get(&keys[0]);
            let end = time::precise_time_ns();
            r_total_t.fetch_add((end-start) as usize, Ordering::SeqCst);
            r_samples.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(t));  
        }
    });
    
    
    let handles = writers.drain(..).map(|(mut writer, mut ops)| {
        let recv = recv.clone();
        thread::spawn(move || {
            let _ = recv.recv().unwrap();
            let _ : Vec<_> = ops.drain(..).map(|op| {
                match op {
                    Op::Write(k, v) => {
                            writer.insert(k, v);
                    }
                    Op::Read(_) => panic!("should be all writes"),
                }
            }).collect();
        })
    });
    
    for _ in 0..(opts.n+1) {
        send.send(());
    }
    let _ : Vec<_> = handles.map(|h| {
        h.join().unwrap();
    }).collect();
    {
        let mut s = stop.lock().unwrap();
        *s = true;
    } 
    let t = total_t.load(Ordering::SeqCst) / samples.load(Ordering::SeqCst);
    reader_handle.join().unwrap();
    thread::sleep(Duration::from_millis(1000));
    factory.stop();
    thread::sleep(Duration::from_millis(100));
    opts.output_csv(t as u64)
}

fn main() {
    println!("creating options");
    let mut opts = Options::new();
    let args: Vec<String> = env::args().collect();
    println!("adding optional options");
    opts.optopt("l", "latency", "set read latency output file name", "NAME");
    opts.optopt("r", "recovery", "set recovery latency output file name", "NAME");
    opts.optopt("i", "integration", "set the integration benchmark output file name", "NAME");
    println!("parsing args");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { println!("error parsing args"); panic!(f.to_string()) }
    };
    println!("done parsing args");

    if matches.opt_present("l") {
        let output = matches.opt_str("l").unwrap();
        {
            let mut out = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(output.clone()).unwrap();
            LatencyOpts::header(&mut out);
        }
        // test read latency
        for n in vec![2, 4, 8, 16, 32] {
            println!("Benching Latency: {}", n);
            {
                println!("Latency: -VM-ENC");
                let opts = LatencyOpts{out: output.clone(), mode: 0, n: n, t: 1000};
                bench_read_latency(Box::new(UnencBTMap::new), ContendedQueueFactory::new(), opts);
            }
            {
                println!("Latency: -VM+ENC");
                let opts = LatencyOpts{out: output.clone(), mode: 1, n: n, t: 1000};
                bench_read_latency(Box::new(StringBTMap::new), ContendedQueueFactory::new(), opts);
            }
            {
                println!("Latency: +VM-ENC");
                let opts = LatencyOpts{out: output.clone(), mode: 2, n: n, t: 1000};
                bench_read_latency(Box::new(UnencBTMap::new), MockVMClientFactory::new(ContendedQueueFactory::new()), opts);
            }
            {
                println!("Latency: +VM+ENC");
                let opts = LatencyOpts{out: output.clone(), mode: 3, n: n, t: 1000};
                bench_read_latency(Box::new(StringBTMap::new), MockVMClientFactory::new(ContendedQueueFactory::new()), opts);
            }
        }
    }
    
    if matches.opt_present("r") {
        let output = matches.opt_str("r").unwrap();
        {
            let mut out = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(output.clone()).unwrap();
            RecOpts::header(&mut out);
        }
        // test recovery latency
        for n in (0..7).map(|i| (2 as i64).pow(i)) {
            println!("Benching Recovery: n={}", n);
            {
                // bench without the vm and without encryption
                println!("Recovery: -VM-ENC");
                let opts = RecOpts::new(output.clone(), 0, n);
                bench_recovery(Box::new(UnencBTMap::new), ContendedQueueFactory::new(), opts);
            }
            {
                // bench without the vm and with encryption
                println!("Recovery: -VM+ENC");
                let opts = RecOpts::new(output.clone(), 1, n);
                bench_recovery(Box::new(StringBTMap::new), ContendedQueueFactory::new(), opts);
            }
            {
                // bench with the vm and without encryption
                let opts = RecOpts::new(output.clone(), 2, n);
                println!("Recovery: +VM-ENC");
                bench_recovery(Box::new(UnencBTMap::new), MockVMClientFactory::new(ContendedQueueFactory::new()), opts);
            }
            {
                // bench with the vm and without encryption
                let opts = RecOpts::new(output.clone(), 3, n);
                println!("Recovery: +VM+ENC");
                bench_recovery(Box::new(StringBTMap::new), MockVMClientFactory::new(ContendedQueueFactory::new()), opts);
            }
        }
    }
    return;
    if matches.opt_present("i") {
        let output = matches.opt_str("i").unwrap();
        {
            let mut out = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(output.clone()).unwrap();
            BenchOpts::header(&mut out);
        }
        // test integration
        for n in vec![1, 2, 4, 16, 32] {
            let nops = 200;
            for w in vec![1, 5, 10, 50, 100] {
                // first do an in memory shared queue
                {
                    println!("Benching Integration: n={} w={}", n, w);
                    // no encryption
                    {
                        println!("No Encryption");
                        bench_integration(
                            ContendedQueueFactory::new(),
                            BenchOpts{out: output.clone(), mode: 0, w: w, n: n, nops: nops});
                    }
                    {
                        println!("Encryption: No VM");
                        bench_integration(
                            ContendedQueueFactory::new(),
                            BenchOpts{out: output.clone(), mode: 1, w: w, n: n, nops: nops});
                    }
                    {
                        println!("No Encryption:: VM");
                        bench_integration(
                            MockVMClientFactory::new(ContendedQueueFactory::new()),
                            BenchOpts{out: output.clone(), mode: 2, w: w, n: n, nops: nops});
                    }
                    {
                        println!("Encryption: VM");
                        bench_integration(
                            MockVMClientFactory::new(ContendedQueueFactory::new()),
                            BenchOpts{out: output.clone(), mode: 3, w: w, n: n, nops: nops});
                    }
                    // homomorphic encryption using the VM as the queue
                }
                // Later transition to DynamoQueue
                // bench::<DynamoQueue, DynamoQueue>(w, n);
            }
        }
    }
    println!("Hello, world!");
}
