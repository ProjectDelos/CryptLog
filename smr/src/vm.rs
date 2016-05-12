extern crate rustc_serialize;
extern crate chan;

const NENTRIES_PER_SNAP: usize = 100;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use std::sync::mpsc;

use indexed_queue::{IndexedQueue, ObjId, LogIndex, Operation, Entry, LogData, Snapshot};
use runtime::{Runtime, Callback};
use indexed_queue::State::Encoded;

use self::chan::{Sender, Receiver, WaitGroup};

use self::SnapshotOp::*;
use self::rustc_serialize::{json, Encodable};

#[derive(Debug)]
enum SnapshotOp {
    SnapshotRequest(WaitGroup, LogIndex),
    LogOp(LogIndex, Operation),
    Stop,
}

pub trait Skiplist {
    // Insert obj_id in list of objects tracked by Skiplist
    fn insert(&mut self, obj_id: ObjId);
    // Append idx to skiplist as relevant to object obj_id
    fn append(&mut self, obj_id: ObjId, idx: LogIndex);
    // Stream log indicies in (from, to) range
    // for all entries concerning an object in obj_ids
    // if to is None, stream from 'from' till the end of the log
    fn stream(&self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> Vec<LogIndex>;
    // gc skiplist, if snapshotting makes entries redundant
    fn gc(&mut self, idx: LogIndex);
}

// Class: MapSkiplist
// Creates a per object skiplist
// A skiplist is a list of log indicies relevant to a particular object
#[derive(Clone)]
pub struct MapSkiplist {
    skiplist: Arc<Mutex<HashMap<ObjId, HashSet<LogIndex>>>>, // Map from object_id to skiplist
}

impl MapSkiplist {
    pub fn new() -> MapSkiplist {
        MapSkiplist { skiplist: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl Skiplist for MapSkiplist {
    fn insert(&mut self, obj_id: ObjId) {
        let mut skiplist = self.skiplist.lock().unwrap();
        skiplist.insert(obj_id, HashSet::new());
    }
    fn append(&mut self, obj_id: ObjId, idx: LogIndex) {
        let mut skiplist = self.skiplist.lock().unwrap();
        skiplist.get_mut(&obj_id).unwrap().insert(idx);
    }

    // Method: Stream
    // Arguments:
    // * obj_ids: ids of objects we want to stream entries for
    // * from: initial log index to start streaming
    // * to: final log index for streaming, or None if we want to stream till the end
    // Streams the entries concerning any object in object_ids
    // Returns:
    // * sorted vector of indices
    fn stream(&self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> Vec<LogIndex> {
        let mut set: HashSet<LogIndex> = HashSet::new();
        let skiplist = self.skiplist.lock().unwrap();
        for obj in skiplist.keys() {
            if obj_ids.contains(obj) {
                for &idx in &skiplist[obj] {
                    if from <= idx && idx < to.unwrap_or(idx + 1) {
                        set.insert(idx);
                    }
                }
            }
        }
        let mut v: Vec<LogIndex> = set.drain().collect();
        v.sort();
        return v;
    }

    // GC entries in skiplist periodically (as snapshotting makes them redundant)
    fn gc(&mut self, idx: LogIndex) {
        let mut skiplist = self.skiplist.lock().unwrap();
        let mut new_skiplist = HashMap::new();
        for (obj_id, set) in skiplist.drain() {
            let mut keep = HashSet::new();
            for i in set {
                if i >= idx {
                    keep.insert(i);
                }
            }
            new_skiplist.insert(obj_id, keep);
        }
        *skiplist = new_skiplist;
    }
}

pub trait Snapshotter {
    // Register object obj_id with Snapshotter, to be kept track of
    // Provide an empty object T, to construct a snapshotted object in
    // and a callback that handles applying log entry operations to the object
    fn register_object<T: 'static + Encodable + Send>(&mut self,
                                                      obj_id: ObjId,
                                                      mut callback: Box<Callback>,
                                                      obj: T);
    // Snapshot all objects, as of and including the idx
    fn snapshot(&mut self, idx: LogIndex);
    // Get most recent snapshots for objects in obj_ids
    fn get_snapshots(&self, obj_ids: &HashSet<ObjId>) -> HashMap<ObjId, Snapshot>;
    // Sends log operation and index to obj_id object to be applied
    fn exec(&mut self, obj_id: ObjId, idx: LogIndex, op: Operation);
    // Starts main thread that listens for snapshotting requests
    fn start(&mut self);
}

// Class: AsyncSnapshotter
// Creates and maintains most recent snapshots of all objected that registered with the Snapshotter
// When a snapshotting request is received, all objects get new snapshots
// Launches one thread per object, and a main thread that listens for snapshot requests
// Creates one channel per object, to communicate snapshot requests and log ops to objects
#[derive(Clone)]
pub struct AsyncSnapshotter {
    snapshots: Arc<Mutex<HashMap<ObjId, Snapshot>>>, // per object most recent snapshot
    obj_chan: HashMap<ObjId, Sender<SnapshotOp>>, // per object send channel
    snapshots_tx: Sender<Option<(WaitGroup, Snapshot)>>, // for objects send their snapshot to main thread
    snapshots_rx: Receiver<Option<(WaitGroup, Snapshot)>>, /* for main thread to receive and aggregate snapshots */
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>, // one thread per object and a main thread
}

impl AsyncSnapshotter {
    pub fn new() -> AsyncSnapshotter {
        let (snapshots_tx, snapshots_rx) = chan::async();
        AsyncSnapshotter {
            snapshots: Arc::new(Mutex::new(HashMap::new())),
            obj_chan: HashMap::new(),
            snapshots_tx: snapshots_tx,
            snapshots_rx: snapshots_rx,
            threads: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Snapshotter for AsyncSnapshotter {
    fn register_object<T: 'static + Encodable + Send>(&mut self,
                                                      obj_id: ObjId,
                                                      mut callback: Box<Callback>,
                                                      obj: T) {
        // Create channel for object
        let (obj_chan_tx, obj_chan_rx) = chan::async();
        self.obj_chan.insert(obj_id, obj_chan_tx);
        let snapshots_tx = self.snapshots_tx.clone();
        // Start snapshotting thread for this object
        self.threads.lock().unwrap().push(thread::spawn(move || {
            let obj = obj;
            // messages for the object
            while let Some(msg) = obj_chan_rx.recv() {
                match msg {
                    SnapshotRequest(wg, idx) => {
                        let snap = json::encode(&obj).unwrap();
                        // send the snapshot to the snapshot aggregator/sender (main thread)
                        snapshots_tx.send(Some((wg, Snapshot::new(obj_id, idx, Encoded(snap)))));
                    }
                    LogOp(idx, op) => {
                        callback(idx, op);
                    }
                    Stop => {
                        return;
                    }
                }
            }
        }));
    }

    fn start(&mut self) {
        let snapshots_rx = self.snapshots_rx.clone();
        let n_objects = self.obj_chan.len();
        let snapshots = self.snapshots.clone();

        // main Snapshotter thread
        self.threads.lock().unwrap().push(thread::spawn(move || {
            let mut idx_snapshots: HashMap<LogIndex, Vec<Snapshot>> = HashMap::new();
            // Listen to the channel of snapshots.
            while let Some(Some((wg, s))) = snapshots_rx.recv() {
                // Aggregate a vector of snapshots on a per index basis
                let idx = s.idx;
                if !idx_snapshots.contains_key(&idx) {
                    idx_snapshots.insert(idx, Vec::new());
                }
                idx_snapshots.get_mut(&idx).unwrap().push(s);
                // Once we have aggregated all the snapshots for an index.
                // Atomically swap our existing snapshots.
                if idx_snapshots[&idx].len() == n_objects {
                    // Commit all of them at once to the objects snaps vector.
                    let mut snaps = snapshots.lock().unwrap();
                    for s in idx_snapshots.get_mut(&idx).unwrap().drain(..) {
                        let obj_id = s.obj_id;
                        snaps.insert(obj_id, s);
                    }
                    // Remove all snapshots for this index
                    idx_snapshots.remove(&idx);
                }
                wg.done();
            }
        }));
    }

    // Exec: send LogOp to obj_id channel
    fn exec(&mut self, obj_id: ObjId, idx: LogIndex, op: Operation) {
        self.obj_chan[&obj_id].send(LogOp(idx, op));
    }

    fn snapshot(&mut self, idx: LogIndex) {
        let wg = chan::WaitGroup::new();
        for chan in self.obj_chan.values() {
            // get snapshot for each object
            wg.add(1);
            let wg = wg.clone();
            chan.send(SnapshotRequest(wg, idx));
        }
        // wait for all the snapshots to complete
        wg.wait();
    }

    fn get_snapshots(&self, obj_ids: &HashSet<ObjId>) -> HashMap<ObjId, Snapshot> {
        let snapshots = self.snapshots.lock().unwrap();
        let mut target = HashMap::new();
        for (k, v) in snapshots.iter() {
            if obj_ids.contains(&k) {
                target.insert(*k, v.clone());
            }
        }
        return target;
    }
}

impl Drop for AsyncSnapshotter {
    fn drop(&mut self) {
        // Tell all per object threads threads to stop (register_object) -> snapshots_tx will close
        for chan in self.obj_chan.values() {
            chan.send(Stop);
        }
        // close tx, to allow main snapshotting thread to return
        self.snapshots_tx.send(None);
        // wait for threads
        let mut threads = self.threads.lock().unwrap();
        for thread in threads.drain(..) {
            thread.join().unwrap();
        }
    }
}

// Class: VM
// Responsible for snapshotting, streaming, and creating the skip list
// Starts one thread that is constantly polling the queue and using new entries to construct skip list.
// Starts the Snapshotter
// Accepts and handles stream requests
#[derive(Clone)]
pub struct VM<Q: IndexedQueue + Send,
              Skip: Skiplist + Clone + Send,
              Snap: Snapshotter + Clone + Send>
{
    pub runtime: Arc<Mutex<Runtime<Q>>>, // VM runtime, same as Client VM, but works with encrypted data
    obj_id: Vec<ObjId>, // ids of objects registered with VM
    local_queue: Arc<Mutex<HashMap<LogIndex, Entry>>>, // cached SharedLog
    skiplist: Arc<Mutex<Skip>>, // skiplist
    snapshotter: Arc<Mutex<Snap>>, // snapshotter
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>, // threads spawned by VM
    stop: Arc<AtomicBool>, // used to stop polling thread
    queue: Q, // queue interface that allows communication with client
}

impl<Q, Skip, Snap> VM<Q, Skip, Snap>
    where Q: 'static + IndexedQueue + Clone + Send,
          Skip: 'static + Skiplist + Clone + Send,
          Snap: 'static + Snapshotter + Clone + Send
{
    pub fn new(q: Q, skiplist: Skip, snapshotter: Snap) -> VM<Q, Skip, Snap> {
        let queue = q.clone();
        let vm = VM {
            runtime: Arc::new(Mutex::new(Runtime::new(q, None))),
            obj_id: Vec::new(),
            local_queue: Arc::new(Mutex::new(HashMap::new())),
            skiplist: Arc::new(Mutex::new(skiplist)),
            snapshotter: Arc::new(Mutex::new(snapshotter)),
            threads: Arc::new(Mutex::new(Vec::new())),
            stop: Arc::new(AtomicBool::new(false)),
            queue: queue,
        };
        return vm;
    }

    pub fn start(&mut self) {
        // number of log entries seen
        let seen = Box::new(Arc::new(AtomicUsize::new(0)));
        {
            let snapshotter = self.snapshotter.clone();
            let skiplist = self.skiplist.clone();
            let seen = seen.clone();
            let local_queue = self.local_queue.clone();

            // Pre_hook to be called before the main object callbacks
            // Makes sure entry exists in local_queue
            let pre_hook = Box::new(move |entry: Entry| {
                // Ensure log entry exists in local queue
                let idx = entry.idx.unwrap();
                let mut local_queue = local_queue.lock().unwrap();
                local_queue.insert(idx, entry);
            });

            // Post_hook to be called after main object callbacks
            // Sees if we have enough entries to require snapshot
            let post_hook = Box::new(move |entry: Entry| {
                let idx = entry.idx.unwrap();
                let seen = seen.fetch_add(1, SeqCst);
                if (seen + 1) % NENTRIES_PER_SNAP == 0 {
                    // Time for a snapshot
                    snapshotter.lock().unwrap().snapshot(idx);
                    // Remove now redundant entries from skiplist
                    skiplist.lock().unwrap().gc(idx - 50);
                    // Gc local queue if it grows too large (not yet needed)
                    // let mut new_queue = HashMap::new();
                    // for (i, entry) in local_queue.drain() {
                    // if i >= idx {
                    // new_queue.insert(i, entry);
                    // }
                    // }
                    // local_queue = new_queue;

                }
            });

            self.runtime.lock().unwrap().register_pre_callback(pre_hook);
            self.runtime.lock().unwrap().register_post_callback(post_hook);
        }

        self.snapshotter.lock().unwrap().start();
        self.poll_runtime();
    }

    // Poll runtime for updates every x milliseconds
    fn poll_runtime(&mut self) {
        let runtime = self.runtime.clone();
        let stop = self.stop.clone();
        self.threads.lock().unwrap().push(thread::spawn(move || {
            loop {
                if stop.load(Acquire) {
                    // notice to stop
                    return;
                }
                // sync all of the objects
                runtime.lock().unwrap().sync(None);
                Duration::from_millis(1000);
            }
        }));
    }

    // Register object obj_id with VM, to be kept track of
    // Provide an empty object Snapshottable, to construct a snapshotted object in
    // and a callback that handles applying log entry operations to the object
    pub fn register_object<Snapshottable: 'static + Encodable + Send>(&mut self,
                                                                      obj_id: ObjId,
                                                                      callback: Box<Callback>,
                                                                      obj: Snapshottable) {
        // insert/ register object
        self.obj_id.push(obj_id);
        self.skiplist.lock().unwrap().insert(obj_id);
        self.snapshotter.lock().unwrap().register_object(obj_id, callback, obj);

        // cloned arc references callback is closed over
        let skiplist = self.skiplist.clone();
        let snapshotter = self.snapshotter.clone();
        // VM version of object callback
        let cb = Box::new(move |idx, op: Operation| {
            // Add this index to the skiplist
            skiplist.lock().unwrap().append(obj_id, idx);
            // Execute this entry on the snapshotter for this object
            snapshotter.lock().unwrap().exec(obj_id, idx, op.clone());
        });
        // Register object with VM's Runtime
        self.runtime.lock().unwrap().register_object(obj_id, cb);
    }
}

impl<Q, Skip, Snap> IndexedQueue for VM<Q, Skip, Snap>
    where Q: IndexedQueue + Send + Clone,
          Skip: Skiplist + Clone + Send,
          Snap: Snapshotter + Clone + Send
{
    fn append(&mut self, e: Entry) -> LogIndex {
        self.queue.append(e)
    }

    fn stream(&mut self,
              obj_ids: &HashSet<ObjId>,
              mut from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<LogData> {
        use indexed_queue::LogData::{LogEntry, LogSnapshot};
        // channel to communicate with client
        let (tx, rx) = mpsc::channel();

        // acquire and send most recent object snaps
        let snaps = self.snapshotter.lock().unwrap().get_snapshots(obj_ids);
        let mut new_from = from;
        for (_, snapshot) in snaps {
            if from <= snapshot.idx && (to.is_none() || snapshot.idx < to.unwrap()) {
                new_from = snapshot.idx + 1; // all snapshots are guaranteed to have the same index
                tx.send(LogSnapshot(snapshot)).unwrap();
            }
        }

        // send log entries appended after most recent snap
        from = new_from;
        let idxs = self.skiplist.lock().unwrap().stream(obj_ids, from, to);
        for idx in idxs {
            if idx < from {
                continue;
            }
            let local_queue = self.local_queue.lock().unwrap();
            let entry = local_queue.get(&idx)
                                   .expect(&format!("skiplist entries should be in local log: {}",
                                                    idx))
                                   .clone();
            tx.send(LogEntry(entry)).unwrap();
        }
        return rx;
    }
}

impl<Q, Skip, Snap> Drop for VM<Q, Skip, Snap>
    where Q: IndexedQueue + Send,
          Skip: Skiplist + Clone + Send,
          Snap: Snapshotter + Clone + Send
{
    fn drop(&mut self) {
        self.stop.store(true, Release); // stop polling thread
        {
            let mut threads = self.threads.lock().unwrap();
            for t in threads.drain(..) {
                t.join().unwrap();
            }
            self.runtime.lock().unwrap().stop();
        }
        // Snapshotter stops on drop snapshotter
    }
}

#[cfg(test)]
mod test {
    extern crate rustc_serialize;
    use self::rustc_serialize::json;
    use super::{VM, Skiplist, MapSkiplist, Snapshotter, AsyncSnapshotter};

    use indexed_queue::{SharedQueue, IndexedQueue, ObjId, Operation, LogOp, State};
    use indexed_queue::LogData::LogEntry;
    use indexed_queue::State::Encoded;
    use runtime::Runtime;
    use ds::{RegisterOp, IntRegister, AddableRegister};
    use encryptors::{MetaEncryptor, Addable, AddEncryptor, EqEncryptor, Encryptor, OrdEncryptor};

    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn skiplist_test() {
        let mut skiplist = MapSkiplist::new();
        let obj0 = vec![0, 2, 4, 5, 9];
        let obj1 = vec![0, 1, 5, 7, 9, 10];
        let objs = vec![obj0, obj1];
        skiplist.insert(0);
        skiplist.insert(1);
        for obj in 0..objs.len() {
            for &idx in &objs[obj] {
                skiplist.append(obj as ObjId, idx);
            }
        }
        let stream = skiplist.stream(&[0, 1].iter().cloned().collect(), 0, Some(4));
        assert_eq!(stream, [0, 1, 2]);
        let stream = skiplist.stream(&[0, 1].iter().cloned().collect(), 1, Some(5));
        assert_eq!(stream, [1, 2, 4]);
        let stream = skiplist.stream(&[0].iter().cloned().collect(), 0, None);
        assert_eq!(stream, [0, 2, 4, 5, 9]);
        // gc test
        skiplist.gc(4);
        let stream = skiplist.stream(&[0, 1].iter().cloned().collect(), 0, None);
        assert_eq!(stream, [4, 5, 7, 9, 10]);
    }
    #[test]
    fn snapshot_test() {
        let q = SharedQueue::new();
        let mut snapshotter = AsyncSnapshotter::new();
        let runtime: Arc<Mutex<Runtime<SharedQueue>>> = Arc::new(Mutex::new(Runtime::new(q, None)));
        let reg = IntRegister::new(&runtime, 0, 0);
        let mut reg2 = reg.clone();
        let n = 250;

        snapshotter.register_object(0, Box::new(move |_, e| reg2.callback(e)), reg);
        snapshotter.start();
        for _ in 0..n {
            let reg_op = RegisterOp::Inc { add: 2 };
            snapshotter.exec(0,
                             0,
                             Operation {
                                 obj_id: 0,
                                 operator: LogOp::Op(State::Encoded(json::encode(&reg_op).unwrap())),
                             });
        }
        snapshotter.snapshot(n);
        let snaps = snapshotter.get_snapshots(&[0].iter().cloned().collect());
        for (_, s) in snaps {
            assert_eq!(s.idx, n);
            match s.payload {
                Encoded(s) => {
                    let reg: IntRegister<SharedQueue> = json::decode(&s).unwrap();
                    let data = reg.data.lock().unwrap();
                    assert_eq!(*data, (n * 2) as i32);
                }
                _ => panic!("should never be encrypted in this test"),
            }
        }
    }
    #[test]
    fn vm_streaming() {
        let q = SharedQueue::new();
        let mut vm: VM<SharedQueue, MapSkiplist, AsyncSnapshotter> =
            VM::new(q.clone(), MapSkiplist::new(), AsyncSnapshotter::new());
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
                LogEntry(e) => {
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
            LogEntry(_) => panic!("first response should be snapshot"),
            _ => {}
        }

        for e in entries {
            match e {
                LogEntry(e) => {
                    assert_eq!(i + 100, e.idx.unwrap());
                    i += 1;
                }
                _ => panic!("should only be one snapshot"),
            }
        }
        assert_eq!(i, 50);
        sleep(Duration::new(1, 0));
        assert_eq!(reg.read(), 149);
        // Now try to recover new register from VM: needs snapshots
    }
}
