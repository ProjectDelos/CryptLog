/*
client: starts up a client to the shared log

- nops:   the number of operations to run
- writes: the number of writes out of this load
- delay:  the amount of delay between two operations
- keys: the number of unique keys to generate
- out: the file to write out to

file output format:

op, time

where op signals what operation it is
read: 0
write: 1
time: the time that this operation took
*/

#[macro_use]
extern crate clap;
extern crate rand;
extern crate time;
extern crate smr;

use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};
use std::fs::OpenOptions;
use std::io::Write;

use std::collections::{BTreeMap};

use smr::runtime::Runtime;
use smr::encryptors::{MetaEncryptor};
use smr::maps::{StringBTMap, UnencBTMap};
use smr::indexed_queue::{IndexedQueue, HttpClient, DynamoQueue};

use clap::App;

// Trait wrapper for IndexedQueue to contain other necessary traits for usage
trait IndexedClonable: 'static+IndexedQueue+Clone+Send+Sync {}
impl IndexedClonable for DynamoQueue {}
impl IndexedClonable for HttpClient {}

trait QueueFactory<Q> {
    fn new_queue(&mut self) -> Q;
}

struct DynamoQueueFactory;

impl DynamoQueueFactory {
    fn new() -> DynamoQueueFactory {
        return DynamoQueueFactory{};
    }
}

impl QueueFactory<DynamoQueue> for DynamoQueueFactory {
    fn new_queue(&mut self) -> DynamoQueue {
        // connects to local dynamo db server to send requests
        DynamoQueue::new()
    }
}

struct HttpClientFactory {
    server_addr: String,
}

impl HttpClientFactory {
    fn new(h: &str, p: &str) -> HttpClientFactory {
        HttpClientFactory{
            server_addr: String::from(h)+ p,
        }
    }
}

impl QueueFactory<HttpClient> for HttpClientFactory {
    fn new_queue(&mut self) -> HttpClient {
        let client = HttpClient::new(&self.server_addr);
        return client;
    }
}

enum Map<Q> {
    Unenc(UnencBTMap<Q>),
    Enc(StringBTMap<Q>),
}

enum Op<K, V> {
    Write(K, V),
    Read(K),
}

// use the first n numbers as the keys and values
fn gen_kvs(k: usize) -> (Vec<String>, Vec<String>) {
    let keys : Vec<String> = (0..k).into_iter().map(|i| {
        i.to_string()
    }).collect();
    (keys.clone(), keys)
}

fn gen_ops(keys: &[String], values: &[String], n: usize, w: usize) -> Vec<Op<String, String>> {
    let ops : Vec<Op<String, String>> = (0..n).map(|i| {
        if i < w {
            Op::Write(keys[rand::random::<usize>()].clone(), values[rand::random::<usize>()].clone())
        } else {
            Op::Read(keys[rand::random::<usize>()].clone())
        }
    }).collect();
    return ops;
}

/*
CSV: format
enc, vm, type, t
*/
fn print_csv(f: &str, enc: bool, vm: bool, times: &[(i32, u64)]) {
    let mut out = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&f).unwrap();
    writeln!(&mut out, "encryption, vm, type, t").unwrap();
    for &(ty, t) in times {
        // encryption, using vm, type, t
        writeln!(&mut out, "{}, {}, {}, {}", enc, vm, ty, t).unwrap();
    }
    return;
}

fn run_client<Q: IndexedClonable, F: QueueFactory<Q>>(mut f: F, mut ops: Vec<Op<String, String>>, opts: Opts) {
    let q = f.new_queue();
    // create an underlying encryptor for this runtime
    let encryptor = Some(MetaEncryptor::new());
    // create a new runtime with the encryptor
    let runtime = Runtime::new(q, encryptor.clone());


    // create a map
    let mut map = if !opts.enc {
        let mut map = UnencBTMap::new(&Arc::new(Mutex::new(runtime)), 1, BTreeMap::new());
        map.start();
        Map::Unenc(map)
    } else {
        let mut map = StringBTMap::new(&Arc::new(Mutex::new(runtime)), 1, BTreeMap::new());
        map.start();
        Map::Enc(map)
    };
    let mut times : Vec<(i32, u64)>= Vec::with_capacity(ops.len());
    // run the operations timing each one
    let _ : Vec<_> = ops.drain(..).map(|op| {
        match op {
            Op::Write(k, v) => {
                match map {
                    Map::Enc(ref mut map) => {
                        let start = time::precise_time_ns();
                        map.insert(k, v);
                        let end = time::precise_time_ns();
                        times.push((0, end-start));
                    }
                    Map::Unenc(ref mut map) => {
                        let start = time::precise_time_ns();
                        map.insert(k, v);
                        let end = time::precise_time_ns();
                        times.push((0, end-start));
                    }
                }

            }
            Op::Read(k) => {
                match map {
                    Map::Enc(ref mut map) => {
                        let start = time::precise_time_ns();
                        map.get(&k);
                        let end = time::precise_time_ns();
                        times.push((1, end-start));
                    }
                    Map::Unenc(ref mut map) => {
                        let start = time::precise_time_ns();
                        map.get(&k);
                        let end = time::precise_time_ns();
                        times.push((1, end-start));
                    }
                }
            }
        }
        if opts.delay > 0 {
            thread::sleep(Duration::from_millis(opts.delay));
        }
    }).collect();
    print_csv(&opts.out, opts.enc, opts.vm, &times);
    println!("Hello, world!");
}

struct Opts {
    enc: bool,
    out: String,
    vm: bool,
    delay: u64,
}

fn main() {
    let yml = load_yaml!("app.yml");
    let app = App::from_yaml(yml);
    let matches = app.get_matches();
    let nops : usize = matches.value_of("nops").unwrap().parse().ok().expect("nops: expected eumber");
    let writes : usize = matches.value_of("writes").unwrap().parse().ok().expect("writes: expected number");
    let keys : usize = matches.value_of("keys").unwrap().parse().ok().expect("keys: expected number");
    let delay : u64 = matches.value_of("delay").unwrap().parse().ok().expect("delay: expected number");
    let out = matches.value_of("out").unwrap();
    let vm = matches.is_present("host");
    let host = matches.value_of("host");
    let port = matches.value_of("port");
    let enc = matches.is_present("enc");

    let (k, v) = gen_kvs(keys);
    let ops = gen_ops(&k, &v, nops, writes);
    let opts = Opts{enc: enc, out: out.to_string(), vm: vm, delay: delay};
    if vm {
        let factory = HttpClientFactory::new(host.unwrap(), port.unwrap());
        run_client(factory, ops, opts);
    } else {
        let factory = DynamoQueueFactory::new();
        run_client(factory, ops, opts);
    };
}
