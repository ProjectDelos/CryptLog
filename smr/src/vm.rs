extern crate rustc_serialize;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;

use indexed_queue::{IndexedQueue, ObjId, LogIndex, Entry, Operation};
use runtime::{Runtime, Callback};

use self::ObjMsg::*;
use self::rustc_serialize::{json, Encodable};

enum ObjMsg {
    SnapshotRequest(LogIndex),
    LogOp(LogIndex, Operation),
}

struct Snapshot {
    obj_id: ObjId,
    idx: LogIndex,
    payload: String,
}

// Design: Two threads
// One thread constantly polling the queue and using new entries to construct skip list.
// One thread driving http server for clients to connect to and make rpc requests.
// Responsibilities:
//   Snapshotting: keep track of all objects and object ids
//   Streaming: Keep track of skip list of entries for all objects
pub struct VM<T: IndexedQueue+Send> {
    runtime: Runtime<T>,
    obj_id: Vec<ObjId>,
    skiplist: Arc<Mutex<HashMap<ObjId, Vec<LogIndex>>>>,
    obj: HashMap<ObjId, mpsc::Sender<ObjMsg>>,
    // Last snapshot for this object
    snaps: Arc<Mutex<HashMap<ObjId, Snapshot>>>,
    snapchan: Option<(mpsc::Sender<Snapshot>, mpsc::Receiver<Snapshot>)>,
}

impl<T> VM<T> where T: IndexedQueue+Send
{
    pub fn new(q: T) -> VM<T> {
        let vm = VM {
            runtime: Runtime::new(q),
            obj_id: Vec::new(),
            skiplist: Arc::new(Mutex::new(HashMap::new())),
            obj: HashMap::new(),
            snaps: Arc::new(Mutex::new(HashMap::new())),
            snapchan: Some(mpsc::channel()),
        };
        return vm;
    }

    pub fn start(&mut self) {
        // Start up snapshot thread.
        // The snapshot thread continuously aggregates snapshots from all objects
        // and atomically updates the objects snaps.
        let snaps = self.snaps.clone();
        let (_, snapchan) = self.snapchan.take().unwrap();
        let n_objects = self.obj.len();
        thread::spawn(move || {
            let mut idx_snapshots: HashMap<LogIndex, Vec<Snapshot>> = HashMap::new();
            // Listen to the channel of snapshots.
            while let Ok(s) = snapchan.recv() {
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
                    let mut snaps = snaps.lock().unwrap();
                    for s in idx_snapshots.get_mut(&idx).unwrap().drain(..) {
                        let obj_id = s.obj_id;
                        snaps.insert(obj_id, s);
                    }
                    // Remove all snapshots for this index
                    idx_snapshots.remove(&idx);
                }
            }
        });

        // Start constructing skip list.
        // Skip list stores all indices in the log for each entry.
        // TODO: Should be garbage collected occasionally after snapshotting.
        let obj_id = self.obj_id.clone();
        for id in obj_id {
            let skiplist = self.skiplist.clone();
            let snapshot_chan = self.obj[&id].clone();
            let vm_callback = Box::new(move |idx: LogIndex, op: Operation| {
                // Add index to the skiplist
                let mut skiplist = skiplist.lock().unwrap();
                skiplist.get_mut(&id).unwrap().push(idx);
                // Send the entry async to the object
                snapshot_chan.send(LogOp(idx, op)).unwrap();
            });
            self.runtime.register_object(id, vm_callback);
        };
    }

    // stream registers the internal callbacks responsible for creating the skiplists etc.
    fn stream(&mut self) {
        // sync all of the objects
    }

    // register_snapshottable takes in an object_id and a callback for
    // constructing the object (just like runtime.register_callback).
    // It also takes obj which is a reference to the object that is being created.
    // It is encodable such that the vm can encode it (snapshot) to send to clients.
    pub fn register_object<Snapshottable: 'static+Encodable+Send>(&mut self,
                           obj_id: ObjId,
                           mut callback: Box<Callback>,
                           obj: Snapshottable) {
        let mut sl = self.skiplist.lock().unwrap();
        sl.insert(obj_id, Vec::new());
        self.obj_id.push(obj_id);


        let (tx, obj_msgs) = mpsc::channel();
        self.obj.insert(obj_id, tx);

        let tx_snapchan = match self.snapchan {
            Some((ref tx, _)) => Some(tx.clone()),
            None => None
        };
        let tx_snapchan = tx_snapchan.unwrap();

        thread::spawn(move || {
            let obj = obj;
            // entries for the object
            while let Ok(msg) = obj_msgs.recv() {
                match msg {
                    SnapshotRequest(idx) => {
                        let snap = json::encode(&obj).unwrap();
                        // send the snapshot to the snapshot aggregator/sender
                        tx_snapchan.send(Snapshot {
                            obj_id: obj_id,
                            idx: idx,
                            payload: snap,
                        }).unwrap();
                    }
                    LogOp(idx, op) => {
                        callback(idx, op);
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use indexed_queue::SharedQueue;

    #[test]
    fn vm() {
        let q = SharedQueue::new();
        let mut vm: VM<SharedQueue> = VM::new(q);
    }
}
