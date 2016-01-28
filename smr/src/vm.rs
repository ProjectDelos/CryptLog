use std::collections::HashMap;

use indexed_queue::{ObjId, LogIndex, IndexedQueue};

// Design: Two threads
// One thread constantly polling the queue and using new entries to construct skip list.
// One thread driving http server for clients to connect to and make rpc requests.
//


struct VM<T: IndexedQueue> {
    idx: HashMap<ObjId, LogIndex>,
    log: T,
}

impl<T> VM<T> where T: IndexedQueue
{
    fn new(q: T) -> VM<T> {
        VM {
            idx: HashMap::new(),
            log: q,
        }
    }
}
