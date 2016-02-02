extern crate rustc_serialize;
extern crate chan;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use std::sync::mpsc;

use indexed_queue::{IndexedQueue, ObjId, LogIndex, Operation, Entry};
use runtime::{Runtime, Callback};

use self::chan::{Sender, Receiver, WaitGroup};

use self::SnapshotOp::*;
use self::rustc_serialize::{json, Encodable};

enum SnapshotOp {
    SnapshotRequest(WaitGroup, LogIndex),
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
    fn stream(&self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> HashSet<LogIndex>;
    fn gc(&mut self, idx: LogIndex);
}

#[derive(Clone)]
pub struct MapSkiplist {
    skiplist: Arc<Mutex<HashMap<ObjId, HashSet<LogIndex>>>>,
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
    // streams the entries in order
    fn stream(&self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> HashSet<LogIndex> {
        let mut set = HashSet::new();
        let skiplist = self.skiplist.lock().unwrap();
        for obj in skiplist.keys() {
            if obj_ids.contains(obj) {
                for &idx in &skiplist[obj] {
                    if from <= idx && idx <= to.unwrap_or(idx) {
                        set.insert(idx);
                    }
                }
            }
        }
        return set;
    }

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
    fn register_object<T: 'static + Encodable + Send>(&mut self,
                                                      obj_id: ObjId,
                                                      mut callback: Box<Callback>,
                                                      obj: T);
    fn snapshot(&mut self, idx: LogIndex);
    fn get_snapshots(&self, obj_ids: &HashSet<ObjId>) -> HashMap<ObjId, Snapshot>;
    fn exec(&mut self, obj_id: ObjId, idx: LogIndex, op: Operation);
    fn start(&mut self);
    fn stop(&mut self);
}

// Per object threads and channels to send to each object
#[derive(Clone)]
pub struct AsyncSnapshotter {
    snapshots: Arc<Mutex<HashMap<ObjId, Snapshot>>>,
    obj_chan: HashMap<ObjId, Sender<SnapshotOp>>,
    done_chan: HashMap<ObjId, Receiver<SnapshotOp>>,
    snapshots_tx: Sender<(WaitGroup, Snapshot)>,
    snapshots_rx: Receiver<(WaitGroup, Snapshot)>,
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl AsyncSnapshotter {
    pub fn new() -> AsyncSnapshotter {
        let (snapshots_tx, snapshots_rx) = chan::async();
        AsyncSnapshotter {
            snapshots: Arc::new(Mutex::new(HashMap::new())),
            obj_chan: HashMap::new(),
            done_chan: HashMap::new(),
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

        let (obj_chan_tx, obj_chan_rx) = chan::async();
        self.obj_chan.insert(obj_id, obj_chan_tx);
        let snapshots_tx = self.snapshots_tx.clone();
        // Start snapshotting thread for this object
        self.threads.lock().unwrap().push(thread::spawn(move || {
            let obj = obj;
            // entries for the object
            while let Some(msg) = obj_chan_rx.recv() {
                match msg {
                    SnapshotRequest(wg, idx) => {
                        let snap = json::encode(&obj).unwrap();
                        // send the snapshot to the snapshot aggregator/sender
                        snapshots_tx.send((wg,
                                           Snapshot {
                            obj_id: obj_id,
                            idx: idx,
                            payload: snap,
                        }));
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

        self.threads.lock().unwrap().push(thread::spawn(move || {
            let mut idx_snapshots: HashMap<LogIndex, Vec<Snapshot>> = HashMap::new();
            // Listen to the channel of snapshots.
            while let Some((wg, s)) = snapshots_rx.recv() {
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
                wg.done();
            }
        }));
    }

    fn exec(&mut self, obj_id: ObjId, idx: LogIndex, op: Operation) {
        self.obj_chan[&obj_id].send(LogOp(idx, op));
    }

    fn snapshot(&mut self, idx: LogIndex) {
        let wg = chan::WaitGroup::new();
        for chan in self.obj_chan.values() {
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

    fn stop(&mut self) {
        // Tell all per object threads threads to stop (register_object) -> snapshots_tx will close
        for chan in self.obj_chan.values() {
            chan.send(Stop);
        }

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
              Skip: Skiplist + Clone + Send,
              Snap: Snapshotter + Clone + Send>
{
    runtime: Arc<Mutex<Runtime<Q>>>,
    obj_id: Vec<ObjId>,
    local_queue: Arc<Mutex<HashMap<LogIndex, Entry>>>,
    skiplist: Skip,
    snapshots: Snap,
    threads: Vec<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    queue: Q,
}

impl<Q, Skip, Snap> VM<Q, Skip, Snap>
    where Q: 'static + IndexedQueue + Clone + Send,
          Skip: 'static + Skiplist + Clone + Send,
          Snap: 'static + Snapshotter + Clone + Send
{
    pub fn new(q: Q, skiplist: Skip, snapshotter: Snap) -> VM<Q, Skip, Snap> {
        let queue = q.clone();
        let vm = VM {
            runtime: Arc::new(Mutex::new(Runtime::new(q))),
            obj_id: Vec::new(),
            local_queue: Arc::new(Mutex::new(HashMap::new())),
            skiplist: skiplist,
            snapshots: snapshotter,
            threads: Vec::new(),
            stop: Arc::new(AtomicBool::new(false)),
            queue: queue,
        };
        return vm;
    }

    // start starts the vm streaming of the log
    pub fn start(&mut self) {
        // Start constructing skip list.
        // Skip list stores all indices in the log for each entry.
        // TODO: Should be garbage collected occasionally after snapshotting.
        let obj_id = self.obj_id.clone();
        let seen = Box::new(Arc::new(AtomicUsize::new(0)));
        for id in obj_id {
            let mut snapshotter = self.snapshots.clone();
            let mut skiplist = self.skiplist.clone();
            let seen = seen.clone();
            let local_queue = self.local_queue.clone();
            let vm_callback = Box::new(move |idx: LogIndex, op: Operation| {
                let seen = seen.fetch_add(1, SeqCst);
                // Add index to the skiplist
                skiplist.append(id, idx);
                // Send the entry async to the object
                snapshotter.exec(id, idx, op);
                if seen % 100 == 0 {
                    snapshotter.snapshot(idx);
                    // gc local queue
                    let mut local_queue = local_queue.lock().unwrap();
                    let mut new_queue = HashMap::new();
                    for (i, entry) in local_queue.drain() {
                        if i >= idx {
                            new_queue.insert(i, entry);
                        }
                    }
                    // Leave 100 entries in case clients are reading from them
                    skiplist.gc(idx - 50);
                }
            });
            self.runtime.lock().unwrap().register_object(id, vm_callback);
        }

        self.snapshots.start();
        self.poll_runtime();
    }

    fn poll_runtime(&mut self) {
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

impl<Q, Skip, Snap> IndexedQueue for VM<Q, Skip, Snap>
    where Q: IndexedQueue + Send + Clone,
          Skip: Skiplist + Clone + Send,
          Snap: Snapshotter + Clone + Send
{
    fn append(&mut self, e: Entry) -> LogIndex {
        self.queue.append(e)
    }

    fn stream(&self,
              obj_ids: &HashSet<ObjId>,
              mut from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<Entry> {

        let (tx, rx) = mpsc::channel();
        let snaps = self.snapshots.get_snapshots(obj_ids);
        for (id, snapshot) in snaps {
            if from <= snapshot.idx && (to.is_none() || snapshot.idx <= to.unwrap()) {
                from = snapshot.idx; // all snapshots are guaranteed to have the same index
                //tx.send(Snapshot(snapshot));
            }
        }
        let idxs = self.skiplist.stream(obj_ids, from, to);
        for idx in idxs {
            let local_queue = self.local_queue.lock().unwrap();
            let entry = local_queue[&idx].clone();
            tx.send(entry).unwrap();
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

    #[test]
    fn vm() {
        let q = SharedQueue::new();
        let vm: VM<SharedQueue, MapSkiplist, AsyncSnapshotter> = VM::new(q,
                                                                         MapSkiplist::new(),
                                                                         AsyncSnapshotter::new());
    }
}
