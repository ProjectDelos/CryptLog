extern crate rustc_serialize;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use indexed_queue::{IndexedQueue, ObjId, LogIndex, Operation};
use runtime::{Runtime, Encryptor, Callback};

use self::SnapshotOp::*;
use self::rustc_serialize::{json, Encodable};

enum SnapshotOp {
    SnapshotRequest(LogIndex),
    LogOp(LogIndex, Operation),
    Stop,
}

#[derive(Clone)]
pub struct Snapshot {
    obj_id: ObjId,
    idx: LogIndex,
    payload: String,
}

pub trait Skiplist {
    fn insert(&mut self, obj_id: ObjId);
    fn append(&mut self, obj_id: ObjId, idx: LogIndex);
}

#[derive(Clone)]
pub struct MapSkiplist {
    skiplist: Arc<Mutex<HashMap<ObjId, Vec<LogIndex>>>>,
}

impl MapSkiplist {
    pub fn new() -> MapSkiplist {
        MapSkiplist { skiplist: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl Skiplist for MapSkiplist {
    fn insert(&mut self, obj_id: ObjId) {
        let mut skiplist = self.skiplist.lock().unwrap();
        skiplist.insert(obj_id, Vec::new());
    }
    fn append(&mut self, obj_id: ObjId, idx: LogIndex) {
        let mut skiplist = self.skiplist.lock().unwrap();
        skiplist.get_mut(&obj_id).unwrap().push(idx);
    }
}

pub trait Snapshotter {
    fn register_object<T: 'static + Encodable + Send>(&mut self,
                                                      obj_id: ObjId,
                                                      mut callback: Box<Callback>,
                                                      obj: T);
    fn snapshot(&mut self, idx: LogIndex);
    fn get_snapshots(&self, obj_ids: HashSet<ObjId>) -> HashMap<ObjId, Snapshot>;
    fn exec(&mut self, obj_id: ObjId, idx: LogIndex, op: Operation);
    fn start(&mut self);
    fn stop(&mut self);
}

// Per object threads and channels to send to each object
#[derive(Clone)]
pub struct AsyncSnapshotter {
    snapshots: Arc<Mutex<HashMap<ObjId, Snapshot>>>,
    obj_chan: HashMap<ObjId, mpsc::Sender<SnapshotOp>>,
    snapshots_tx: Option<mpsc::Sender<Snapshot>>,
    snapshots_rx: Arc<Mutex<Option<mpsc::Receiver<Snapshot>>>>,
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl AsyncSnapshotter {
    pub fn new() -> AsyncSnapshotter {
        let (snapshots_tx, snapshots_rx) = mpsc::channel();
        AsyncSnapshotter {
            snapshots: Arc::new(Mutex::new(HashMap::new())),
            obj_chan: HashMap::new(),
            snapshots_tx: Some(snapshots_tx),
            snapshots_rx: Arc::new(Mutex::new(Some(snapshots_rx))),
            threads: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Snapshotter for AsyncSnapshotter {
    fn register_object<T: 'static + Encodable + Send>(&mut self,
                                                      obj_id: ObjId,
                                                      mut callback: Box<Callback>,
                                                      obj: T) {

        let (obj_chan_tx, obj_chan_rx) = mpsc::channel();
        self.obj_chan.insert(obj_id, obj_chan_tx);
        let snapshots_tx = self.snapshots_tx.clone().unwrap();
        // Start snapshotting thread for this object
        self.threads.lock().unwrap().push(thread::spawn(move || {
            let obj = obj;
            // entries for the object
            while let Ok(msg) = obj_chan_rx.recv() {
                match msg {
                    SnapshotRequest(idx) => {
                        let snap = json::encode(&obj).unwrap();
                        // send the snapshot to the snapshot aggregator/sender
                        snapshots_tx.send(Snapshot {
                                        obj_id: obj_id,
                                        idx: idx,
                                        payload: snap,
                                    })
                                    .unwrap();
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
        let snapshots_rx = self.snapshots_rx.lock().unwrap().take().unwrap();
        let n_objects = self.obj_chan.len();
        let snapshots = self.snapshots.clone();

        self.threads.lock().unwrap().push(thread::spawn(move || {
            let mut idx_snapshots: HashMap<LogIndex, Vec<Snapshot>> = HashMap::new();
            // Listen to the channel of snapshots.
            while let Ok(s) = snapshots_rx.recv() {
                // Aggregate a vector of snapshots on a per index basis.
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
            }
        }));
    }

    fn exec(&mut self, obj_id: ObjId, idx: LogIndex, op: Operation) {
        self.obj_chan[&obj_id].send(LogOp(idx, op)).unwrap();
    }

    fn snapshot(&mut self, idx: LogIndex) {
        for chan in self.obj_chan.values() {
            chan.send(SnapshotRequest(idx)).unwrap();
        }
    }

    fn get_snapshots(&self, obj_ids: HashSet<ObjId>) -> HashMap<ObjId, Snapshot> {
        let snapshots = self.snapshots.lock().unwrap();
        let mut target = HashMap::new();
        for (k, v) in snapshots.iter() {
            if obj_ids.contains(&k) {
                target.insert(*k, v.clone());
            }
        }
        return target;
    }

    fn stop(&mut self) {
        // Tell all per object threads  threads to stop (register_object) -> snapshots_tx will close
        for chan in self.obj_chan.values() {
            chan.send(Stop).unwrap();
        }
        // remove the last reference to snapshot_tx
        self.snapshots_tx.take();

        // wait for all threads to stop
        let mut threads = self.threads.lock().unwrap();
        for thread in threads.drain(..) {
            thread.join().unwrap();
        }
    }
}

// Design: Two threads
// One thread constantly polling the queue and using new entries to construct skip list.
// One thread driving http server for clients to connect to and make rpc requests.
// Responsibilities:
//   Snapshotting: keep track of all objects and object ids
//   Streaming: Keep track of skip list of entries for all objects
pub struct VM<Q: IndexedQueue + Send,
              Secure: Encryptor + Send,
              Skip: Skiplist + Clone + Send,
              Snap: Snapshotter + Clone + Send>
{
    runtime: Arc<Mutex<Runtime<Q, Secure>>>,
    obj_id: Vec<ObjId>,
    skiplist: Skip,
    snapshots: Snap,
    threads: Vec<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl<Q, Secure, Skip, Snap> VM<Q, Secure, Skip, Snap>
    where Q: 'static + IndexedQueue + Send,
          Secure: 'static + Encryptor + Send,
          Skip: 'static + Skiplist + Clone + Send,
          Snap: 'static + Snapshotter + Clone + Send
{
    pub fn new(q: Q, skiplist: Skip, snapshotter: Snap) -> VM<Q, Secure, Skip, Snap> {
        let vm = VM {
            runtime: Arc::new(Mutex::new(Runtime::new(q))),
            obj_id: Vec::new(),
            skiplist: skiplist,
            snapshots: snapshotter,
            threads: Vec::new(),
            stop: Arc::new(AtomicBool::new(false)),
        };
        return vm;
    }

    // start starts the vm streaming of the log
    pub fn start(&mut self) {
        // Start constructing skip list.
        // Skip list stores all indices in the log for each entry.
        // TODO: Should be garbage collected occasionally after snapshotting.
        let obj_id = self.obj_id.clone();
        for id in obj_id {
            let mut snapshotter = self.snapshots.clone();
            let mut skiplist = self.skiplist.clone();
            let vm_callback = Box::new(move |idx: LogIndex, op: Operation| {
                // Add index to the skiplist
                skiplist.append(id, idx);
                // Send the entry async to the object
                snapshotter.exec(id, idx, op);
            });
            self.runtime.lock().unwrap().register_object(id, vm_callback);
        }

        self.snapshots.start();
        self.stream();
    }

    // stream registers the internal callbacks responsible for creating the skiplists etc.
    fn stream(&mut self) {
        // sync all of the objects
        let runtime = self.runtime.clone();
        let stop = self.stop.clone();
        self.threads.push(thread::spawn(move || {
            loop {
                if stop.load(Acquire) {
                    return;
                }
                runtime.lock().unwrap().sync(None);
                Duration::from_millis(4000);
            }
        }));
    }

    // register_snapshottable takes in an object_id and a callback for
    // constructing the object (just like runtime.register_callback).
    // It also takes obj which is a reference to the object that is being created.
    // It is encodable such that the vm can encode it (snapshot) to send to clients.
    // Invarient: the callback is closed over obj
    pub fn register_object<Snapshottable: 'static + Encodable + Send>(&mut self,
                                                                      obj_id: ObjId,
                                                                      callback: Box<Callback>,
                                                                      obj: Snapshottable) {
        self.skiplist.insert(obj_id);
        self.obj_id.push(obj_id);
        self.snapshots.register_object(obj_id, callback, obj);
    }
}

impl<Q, Secure, Skip, Snap> Drop for VM<Q, Secure, Skip, Snap>
    where Q: IndexedQueue + Send,
          Secure: Encryptor + Send,
          Skip: Skiplist + Clone + Send,
          Snap: Snapshotter + Clone + Send
{
    fn drop(&mut self) {
        // Tell thread poller to stop and collect threads
        self.stop.store(true, Release);
        for t in self.threads.drain(..) {
            t.join().unwrap();
        }
        // Stop snapshotter
        println!("STOPPING SNAPSHOTTER");
        self.snapshots.stop();
        println!("VM STOPPED");
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use indexed_queue::SharedQueue;
    use runtime::Identity;

    #[test]
    fn vm() {
        let q = SharedQueue::new();
        let mut vm: VM<SharedQueue, Identity, MapSkiplist, AsyncSnapshotter> =
            VM::new(q, MapSkiplist::new(), AsyncSnapshotter::new());
    }
}
