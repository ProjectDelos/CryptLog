#[macro_use]
extern crate clap;
extern crate smr;
use smr::maps::{EncBTMap, UnencBTMap};
use smr::indexed_queue::{DynamoQueue, ObjId};
use smr::vm::{VM, MapSkiplist, AsyncSnapshotter};
use std::collections::{BTreeMap};
use smr::http_server::HttpServer;
use clap::App;


fn main() {
    let yml = load_yaml!("app.yml");
    let app = App::from_yaml(yml);
    let matches = app.get_matches();
    let enc = matches.is_present("enc");
    let server = matches.value_of("server").unwrap();
    let port = matches.value_of("port").unwrap();
    let server_addr = server.to_string() + &port;
    println!("Hello, world!");

    // start up a vm given an underlying dynamodb queue
    let q = DynamoQueue::new();
    let mut vm = VM::new(q, MapSkiplist::new(), AsyncSnapshotter::new());
    let id = 1 as ObjId;
    if enc {
        let map = EncBTMap::new(&vm.runtime, id, BTreeMap::new());
        let mut map_copy = map.clone();
        vm.register_object(id as ObjId,
                           Box::new(move |_, e| map_copy.callback(e)),
                           map.clone());
        vm.start();
    } else {
        let map = UnencBTMap::new(&vm.runtime, id, BTreeMap::new());
        let mut map_copy = map.clone();
        vm.register_object(id as ObjId,
                           Box::new(move |_, e| map_copy.callback(e)),
                           map.clone());
        vm.start();
    }
    // start up the server at the given address
    HttpServer::new(vm, &server_addr);
}
