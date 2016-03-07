extern crate smr;
extern crate rustc_serialize;

use self::rustc_serialize::json;

use smr::ds::{IntRegister, AddableRegister};
use smr::maps::{StringBTMap, EncBTMap};
use smr::runtime::Runtime;
use smr::indexed_queue::{SharedQueue, ObjId, LogData};
use std::sync::{Arc, Mutex};
use smr::vm::{VM, MapSkiplist, Snapshotter, AsyncSnapshotter};
use smr::encryptors::{MetaEncryptor, Encryptor, AddEncryptor, EqEncryptor, OrdEncryptor, Addable,
                      Ordable, Encrypted};
use smr::indexed_queue::IndexedQueue;
use std::collections::BTreeMap;
use std::time::Duration;
use std::thread;


#[test]
fn vm_streaming_test() {
    let q = SharedQueue::new();
    let mut vm: VM<SharedQueue, MapSkiplist, AsyncSnapshotter> = VM::new(q.clone(),
                                                                         MapSkiplist::new(),
                                                                         AsyncSnapshotter::new());
    let add_encryptor = AddEncryptor::new();
    let obj_id: ObjId = 0;
    let reg = AddableRegister::new(&vm.runtime,
                                   obj_id,
                                   Addable::default(add_encryptor.public_key()));
    let vm_reg = reg.clone();
    let mut snapshot_reg = reg.clone();
    // register the Register with the VM
    vm.register_object(0, Box::new(move |_, e| snapshot_reg.callback(e)), vm_reg);
    vm.start();

    let me = MetaEncryptor::from(EqEncryptor::new(Encryptor::new()),
                                 add_encryptor.clone(),
                                 Encryptor::new(),
                                 OrdEncryptor::new(Encryptor::new()));
    let client_runtime = Runtime::new(q, Some(me));
    let client_runtime = Arc::new(Mutex::new(client_runtime));
    let mut client_reg = IntRegister::new(&client_runtime, obj_id, 0);
    for i in 0..10 {
        client_reg.write(i);
    }

    vm.runtime.lock().unwrap().sync(Some(obj_id));
    let mut i = 0;
    let entries = vm.stream(&[obj_id].iter().cloned().collect(), 0, None);
    for e in entries {
        match e {
            LogData::LogEntry(e) => {
                assert_eq!(i, e.idx.unwrap());
                i += 1;
            }
            _ => panic!("should not snapshot: too few entries"),
        }
    }
    assert_eq!(i, 10);
}

#[test]
fn vm_full() {
    let q = SharedQueue::new();
    let mut vm = VM::new(q.clone(), MapSkiplist::new(), AsyncSnapshotter::new());

    let add_encryptor = AddEncryptor::new();
    // VM does snapshotting in reg, decrypting not needed
    let mut reg = AddableRegister::new(&vm.runtime,
                                       -1,
                                       Addable::default(add_encryptor.public_key()));
    let reg1 = reg.clone();
    // register the Register with the VM
    vm.register_object(0, Box::new(move |_, e| reg.callback(e)), reg1);
    vm.start();

    let reg_run = Arc::new(Mutex::new(Runtime::new(q, Some(MetaEncryptor::new()))));
    let mut reg = IntRegister::new(&reg_run, 0, -1);
    reg.start();

    for i in 0..150 {
        reg.write(i);
    }

    assert_eq!(reg.read(), 149);


    vm.runtime.lock().unwrap().sync(Some(0));
    let mut i = 0;
    let entries = vm.stream(&[0].iter().cloned().collect(), 0, None);

    let e = entries.recv().unwrap();
    match e {
        LogData::LogEntry(_) => panic!("first response should be snapshot"),
        _ => {}
    }

    for e in entries {
        match e {
            LogData::LogEntry(e) => {
                assert_eq!(i + 100, e.idx.unwrap());
                i += 1;
            }
            _ => panic!("should only be one snapshot"),
        }
    }
    assert_eq!(i, 50);
    thread::sleep(Duration::new(1, 0));
    assert_eq!(reg.read(), 149);
    // Now try to recover new register from VM: needs snapshots
}


#[test]
fn register_integration_tests() {
    let q = SharedQueue::new();
    let encryptor = MetaEncryptor::new();

    // SETUP VM: Register two registers
    let mut vm = VM::new(q.clone(), MapSkiplist::new(), AsyncSnapshotter::new());
    let vm_reg1 = AddableRegister::new(&vm.runtime,
                                       1 as ObjId,
                                       Addable::default(encryptor.add.public_key()));
    let mut vm_reg1_copy = vm_reg1.clone();
    // OBJ1
    vm.register_object(1 as ObjId,
                       Box::new(move |_, e| vm_reg1_copy.callback(e)),
                       vm_reg1.clone());

    let vm_reg2 = AddableRegister::new(&vm.runtime,
                                       2 as ObjId,
                                       Addable::default(encryptor.add.public_key()));
    let mut vm_reg2_copy = vm_reg2.clone();
    // OBJ2
    vm.register_object(2 as ObjId,
                       Box::new(move |_, e| vm_reg2_copy.callback(e)),
                       vm_reg2.clone());

    // START VM
    vm.start();

    // SETUP CLIENT REGISTERS
    println!("Starting Client Registers");
    let runtime: Runtime<SharedQueue> = Runtime::new(q.clone(), Some(encryptor.clone()));
    let aruntime = Arc::new(Mutex::new(runtime));

    let mut reg1 = IntRegister::new(&aruntime, 1 as ObjId, 0);
    let mut reg2 = IntRegister::new(&aruntime, 2 as ObjId, 0);
    reg1.start();
    reg2.start();


    // EXECUTE TONS OF WRITES
    let rounds = 266;
    // reg1: 0 -> 1000
    // reg2: 1 -> 2000
    println!("Writing to registers");
    for _ in 0..rounds {
        reg1.inc(1);
        reg2.inc(2);
    }
    println!("Done writing to registers");

    // check register values correctly read in new views
    // let mut reg1b = IntRegister::new(&aruntime, 1 as ObjId, 0);
    // reg1b.start();
    // let mut reg2b = IntRegister::new(&aruntime, 2 as ObjId, 1);
    // reg2b.start();
    // assert_eq!(reg1b.read(), rounds);
    // assert_eq!(reg2b.read(), rounds * 2 + 1);
    // println!("Done reading registers");
    //

    // check if registers can recover from the vm
    // this validates the snapshotting of the vm
    println!("Setting up VM as Register Runtime");
    let meta_runtime = Runtime::new(vm, Some(encryptor));
    let a_meta_runtime = Arc::new(Mutex::new(meta_runtime));
    let mut meta_reg1 = IntRegister::new(&a_meta_runtime, 1 as ObjId, 0);
    let mut meta_reg2 = IntRegister::new(&a_meta_runtime, 2 as ObjId, 0);
    println!("Starting VM Registers");
    meta_reg1.start();
    meta_reg2.start();
    println!("Reading VM Registers");
    assert_eq!(meta_reg1.read(), rounds);
    println!("READING SECOND REGISTER");
    println!("Expecting: {}", rounds * 2);
    assert_eq!(meta_reg2.read(), rounds * 2);
    // Ensure that a snapshot was used
    // Changing the initial value of a register should not make a difference since it
    // should use a snapshot and overwrite the state of the register.
    println!("Starting VM Register with different initial value");
    let mut meta_reg12 = IntRegister::new(&a_meta_runtime, 1 as ObjId, 100);
    println!("Starting");
    meta_reg12.start();
    println!("Reading");
    assert_eq!(meta_reg12.read(), rounds);
    println!("Test Success");
}

#[test]
fn map_enc() {
    let mut aux_btmap: BTreeMap<Ordable, Encrypted> = BTreeMap::new();
    aux_btmap.insert(Ordable::default(), Encrypted::default());

    let e = json::encode(&aux_btmap).unwrap();
    let _: EncBTMap<SharedQueue> = json::decode(&e).unwrap();
}

#[test]
fn btmap_integration_tests() {
    let q = SharedQueue::new();
    let encryptor = MetaEncryptor::new();
    // SETUP VM
    let mut vm = VM::new(q.clone(), MapSkiplist::new(), AsyncSnapshotter::new());
    let vm_map1 = EncBTMap::new(&vm.runtime, 1 as ObjId, BTreeMap::new());
    let mut vm_map1_copy = vm_map1.clone();
    vm.register_object(1 as ObjId,
                       Box::new(move |_, e| vm_map1_copy.callback(e)),
                       vm_map1.clone());

    let vm_map2 = EncBTMap::new(&vm.runtime, 2 as ObjId, BTreeMap::new());
    let mut vm_map2_copy = vm_map2.clone();
    vm.register_object(2 as ObjId,
                       Box::new(move |_, e| vm_map2_copy.callback(e)),
                       vm_map2.clone());
    vm.start();

    // SETUP CLIENT REGISTERS
    println!("Starting Client BTMaps");
    let runtime: Runtime<SharedQueue> = Runtime::new(q.clone(), Some(encryptor.clone()));
    let aruntime = Arc::new(Mutex::new(runtime));

    let mut btmap1 = StringBTMap::new(&aruntime, 1 as ObjId, BTreeMap::new());
    let mut btmap2 = StringBTMap::new(&aruntime, 2 as ObjId, BTreeMap::new());
    btmap1.start();
    btmap2.start();

    // Execute many writes
    println!("Execute map writes");
    let keys = vec!["h0", "h1", "h2", "alphabet", "h0rry"];
    let vals = vec!["h0", "h1", "h2", "alphabet", "h0rry"];
    let vals2 = vec!["v2h0", "v2h1", "v2h2", "v2alphabet", "v2h0rry"];
    let nkeys = keys.len();
    let should_be_at = vec![3, 0, 4, 1, 2];
    let rounds = 50;
    for i in 0..rounds {
        let mi = i % nkeys;
        btmap1.insert(String::from(keys[mi].clone()),
                      String::from(vals[mi].clone()));
        btmap2.insert(String::from(keys[mi].clone()),
                      String::from(vals2[mi].clone()));
    }
    for i in 0..nkeys {
        println!("POPPING!");
        let (_, val) = btmap1.pop_first().expect("btmap1-pop");
        let (_, val2) = btmap2.pop_first().expect("btmap2-pop");
        assert_eq!(val, vals[should_be_at[i]]);
        assert_eq!(val2, vals2[should_be_at[i]]);
    }

    // check if maps can recover from the vm
    // this validates the snapshotting of the vm
    println!("Setting up VM as BTMap Runtime");
    let meta_runtime = Runtime::new(vm, Some(encryptor));
    let a_meta_runtime = Arc::new(Mutex::new(meta_runtime));
    let mut meta_btmap1 = StringBTMap::new(&a_meta_runtime, 1 as ObjId, BTreeMap::new());
    let mut meta_btmap2 = StringBTMap::new(&a_meta_runtime, 2 as ObjId, BTreeMap::new());
    println!("Starting VM BTMaps");
    meta_btmap1.start();
    meta_btmap2.start();

    println!("READING VALUES");
    // Read values (should come from snapshots)
    for i in 0..nkeys {
        println!("reading 1: {}", i);
        let (_, val) = meta_btmap1.pop_first().expect("pop_first meta_btmap1");
        println!("found {} =?= {}", val, vals[should_be_at[i]]);
        println!("reading 2: {}", i);
        let (_, val2) = meta_btmap2.pop_first().expect("pop_fist meta_btmap2");
        println!("found {} =?= {}", val2, vals2[should_be_at[i]]);
        // println!("key {:?} val {:?}", key, val);
        assert_eq!(val, vals[should_be_at[i]]);
        assert_eq!(val2, vals2[should_be_at[i]]);
    }
}
