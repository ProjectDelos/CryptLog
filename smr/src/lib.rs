extern crate rustc_serialize;

pub mod runtime;
use runtime::Runtime;
use runtime::InMemoryQueue;
use runtime::IntEntry;

#[test]
fn create_runtime() {
    let mut r: Runtime<IntEntry, InMemoryQueue<IntEntry>> = Runtime::new(
        InMemoryQueue::new());
    r.append(IntEntry::new(10));
}


