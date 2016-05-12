extern crate serde;
extern crate serde_json;
extern crate rustc_serialize;
extern crate hyper;
extern crate rand;

use std::collections::{VecDeque, HashSet, HashMap};
use std::sync::{Mutex, Arc};
use std::sync::mpsc;
use std::time::Duration;
use std::thread;

use self::hyper::Client;
use self::hyper::header::Connection;
use std::net::TcpStream;
use std::io::{Read, Bytes};
use self::serde::ser::Serialize;
use self::serde::de::Deserialize;

use self::rustc_serialize::json;
use self::rustc_serialize::Encodable;

use http_data::{HttpRequest, HttpResponse};

pub type LogIndex = i64;
pub type ObjId = i32;

#[derive(RustcDecodable, RustcEncodable, Debug, Clone, PartialEq, Eq)]
pub enum State {
    Encrypted(Vec<u8>),
    Encoded(String),
}

// Enum: LogOp
// Log Operator, a wrapper arund a snapshot or data structure operator
// to be used in communication with Shared Log
#[derive(RustcDecodable, RustcEncodable, Debug, Clone, PartialEq, Eq)]
pub enum LogOp {
    Snapshot(State), // snapshot in an encoded/ encrypted state
    Op(State), // data structure operator in an encoded/ encrypted state
}

// Class: Operation
// Wrapper around an object id and a log operator
// to be used in communication with Shared Log
#[derive(RustcDecodable, RustcEncodable, Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    pub obj_id: ObjId, // id of object responsible for operation
    pub operator: LogOp, // data structure operator
}

impl Operation {
    pub fn new(obj_id: ObjId, operator: State) -> Operation {
        Operation {
            obj_id: obj_id,
            operator: LogOp::Op(operator),
        }
    }
    pub fn from_snapshot(obj_id: ObjId, snap: State) -> Operation {
        Operation {
            obj_id: obj_id,
            operator: LogOp::Snapshot(snap),
        }
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone, PartialEq, Eq)]
pub enum TxType {
    Begin,
    End,
    None,
}

#[derive(RustcDecodable, RustcEncodable, Debug, Clone, PartialEq, Eq)]
pub enum TxState {
    Accepted, // if found valid
    Aborted, // if found invalid
    None, // if not checked yet
}

// Class: Snapshot
// Summary of a snapshot from SharedLog
#[derive(Clone, Debug, RustcEncodable, RustcDecodable)]
pub struct Snapshot {
    pub obj_id: ObjId, // id of object snapshotted
    pub idx: LogIndex, // index of snapshot in log
    pub payload: State, // encoded/ encrypted payload of snapshot
}

impl Snapshot {
    pub fn new(obj_id: ObjId, idx: LogIndex, payload: State) -> Snapshot {
        Snapshot {
            obj_id: obj_id,
            idx: idx,
            payload: payload,
        }
    }
}

// Enum: LogData
// Communication with SharedLog happens via sending LogEntries
// and receiving LogEntries and LogSnapshots
#[derive(Clone, Debug, RustcEncodable, RustcDecodable)]
pub enum LogData {
    LogEntry(Entry),
    LogSnapshot(Snapshot),
}

// Class: Entry
// Contains summary of an operation or transaction
#[derive(RustcDecodable, RustcEncodable, Debug, Clone)]
pub struct Entry {
    pub idx: Option<LogIndex>, // index Entry receives in SharedLog, initially None

    pub reads: HashMap<ObjId, LogIndex>, // map of <object id, version read during transaction>
    pub writes: HashSet<ObjId>, // set of obj_ids written in current transaction
    pub operations: Vec<Operation>, // operations to be included in current open transaction, if any

    pub tx_type: TxType, // transaction type (begin, end, ..)
    pub tx_state: TxState, // transaction state (accepted, aborted, ..)
}

impl Entry {
    pub fn new(reads: HashMap<ObjId, LogIndex>,
               writes: HashSet<ObjId>,
               operations: Vec<Operation>,
               tx_type: TxType,
               tx_state: TxState)
               -> Entry {
        return Entry {
            idx: None,
            reads: reads,
            writes: writes,
            operations: operations,
            tx_type: tx_type,
            tx_state: tx_state,
        };
    }
}

// Trait: IndexedQueue
// To be implemented by structure acting as SharedLog
pub trait IndexedQueue {
    // Sends entry to e to log, and returns index at which it was appended
    fn append(&mut self, e: Entry) -> LogIndex;
    // Stream entries relevant to the obj_ids, between log entry indicies (from, to)
    // Note: to is non inclusive
    // If to is not specified: streams up to the length of the log (as read at the beginning of the function)
    fn stream(&mut self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<LogData>;
}

// Class: InMemoryQueue
// In memory implementation of an IndexedQueue, to be used by one client
#[derive(Clone)]
pub struct InMemoryQueue {
    q: VecDeque<Entry>,
}

impl InMemoryQueue {
    pub fn new() -> InMemoryQueue {
        return InMemoryQueue { q: VecDeque::new() };
    }
}

impl IndexedQueue for InMemoryQueue {
    fn append(&mut self, mut e: Entry) -> LogIndex {
        e.idx = Some(self.q.len() as LogIndex);
        self.q.push_back(e);
        return (self.q.len() - 1) as LogIndex;
    }

    fn stream(&mut self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<LogData> {
        use self::LogData::LogEntry;

        // do not need to check against length here
        // because it is guaranteed to be accessed by only one accessor at a time
        let to = match to {
            Some(idx) => idx,
            None => self.q.len() as LogIndex,
        };

        let (tx, rx) = mpsc::channel();
        for i in from..to as LogIndex {
            if !self.q[i as usize].writes.is_disjoint(&obj_ids) {
                // entry relevant to some obj_ids
                tx.send(LogEntry(self.q[i as usize].clone())).unwrap();
            }
        }
        return rx;
    }
}

// Class: SharedQueue
// In memory implementation of an IndexedQueue, can be used by multiple clients
#[derive(Clone)]
pub struct SharedQueue {
    q: Arc<Mutex<InMemoryQueue>>,
}

impl SharedQueue {
    pub fn new() -> SharedQueue {
        SharedQueue { q: Arc::new(Mutex::new(InMemoryQueue::new())) }
    }
}

impl IndexedQueue for SharedQueue {
    fn append(&mut self, e: Entry) -> LogIndex {
        self.q.lock().unwrap().append(e)
    }
    fn stream(&mut self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<LogData> {
        self.q.lock().unwrap().stream(obj_ids, from, to)
    }
}

fn randomize(x: u64, n: u64, d: u64) -> u64 {
    if x == 0 {
        return x;
    }
    let m: i64 = (x * n / d) as i64;
    if m == 0 {
        return x;
    }
    let r = (rand::random::<i32>() as i64) % (m * 2);
    let diff = r - m;
    // +/- 50%
    let out = ((x as i64) + diff) as u64;
    out
}

// Class: ContendedQueue
// In memory implementation of an IndexedQueue, simulates contention
#[derive(Clone)]
pub struct ContendedQueue {
    h: Arc<Mutex<HashMap<LogIndex, Entry>>>,
    delay: u64, // point-to-point time estimate (not round-trip)
}

impl ContendedQueue {
    pub fn new(delay_ms: u64) -> ContendedQueue {
        ContendedQueue {
            h: Arc::new(Mutex::new(HashMap::new())),
            delay: 50,
        }
    }
    fn sleep(&self) {
        thread::sleep(Duration::from_millis(randomize(self.delay, 1, 4)));
    }
}

impl IndexedQueue for ContendedQueue {
    fn append(&mut self, e: Entry) -> LogIndex {
        self.sleep();
        let len = {
            let h = self.h.lock().unwrap();
            h.len()
        };
        self.sleep();
        loop {
            self.sleep();
            let done = {
                let mut h = self.h.lock().unwrap();
                let done = h.len() == len;
                if done {
                    h.insert(len as LogIndex, e.clone());
                }
                done
            };
            self.sleep();
            if done {
                return len as LogIndex;
            }
        }
    }
    fn stream(&mut self,
              obj_ids: &HashSet<ObjId>,
              mut from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<LogData> {
        self.sleep();
        let len = {
            let h = self.h.lock().unwrap();
            h.len() as i64
        };
        self.sleep();
        use self::LogData::LogEntry;
        let (tx, rx) = mpsc::channel();
        loop {
            if from >= len {
                return rx;
            }
            if to.is_some() && from > to.unwrap() {
                return rx;
            }
            self.sleep();
            {
                let h = self.h.lock().unwrap();
                let mut e = h[&from].clone();
                if e.writes.is_disjoint(&obj_ids) {
                    // entry relevant to some obj_ids
                    e.idx = Some(from);
                    tx.send(LogEntry(e)).unwrap();
                }
            }
            self.sleep();
            from += 1;
        }
    }
}

// Class: HttpClient
// Interface to remote SharedLog, implements IndexedQueue
pub struct HttpClient {
    c: Client, // RustLang http client
    to_server: String, // server address
    delay: Duration, // for testing with delays
}

impl Clone for HttpClient {
    fn clone(&self) -> HttpClient {
        HttpClient::with_delay(&self.to_server, self.delay)
    }
}

impl HttpClient {
    pub fn new(to_server: &str) -> HttpClient {
        return HttpClient {
            c: Client::new(),
            to_server: String::from(to_server),
            delay: Duration::new(0, 0),
        };
    }
    pub fn with_delay(to_server: &str, delay: Duration) -> HttpClient {
        return HttpClient {
            c: Client::new(),
            to_server: String::from(to_server),
            delay: delay,
        };
    }

    fn to_server(&self) -> String {
        return self.to_server.clone();
    }
}

impl IndexedQueue for HttpClient {
    fn stream(&mut self,
              obj_ids: &HashSet<ObjId>,
              from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<LogData> {
        // assemble and send request
        let body = json::encode(&HttpRequest::Stream(obj_ids.clone(), from, to)).unwrap();
        thread::sleep(self.delay);
        let mut http_resp = self.c
                                .post(&self.to_server)
                                .header(Connection::keep_alive())
                                .body(&body)
                                .send()
                                .unwrap();
        thread::sleep(self.delay);

        // receive response from server
        let mut resp = String::new();
        http_resp.read_to_string(&mut resp).unwrap();
        let resp = json::decode(&resp).unwrap();

        // channel to communicate with requester of stream
        let (tx, rx) = mpsc::channel();
        match resp {
            HttpResponse::Stream(ref entries) => {
                // stream one entry at a time
                for e in entries {
                    tx.send(e.clone()).unwrap();
                }
            }
            _ => panic!("http_client::stream::wrong response type"),
        };
        return rx;
    }

    fn append(&mut self, e: Entry) -> LogIndex {
        // assemble request
        let e = HttpRequest::Append(e);
        let body = json::encode(&e).expect("error encoding value");
        thread::sleep(self.delay);
        let mut http_resp = self.c
                                .post(&self.to_server)
                                .header(Connection::keep_alive())
                                .body(&body)
                                .send()
                                .expect("error getting from server");
        thread::sleep(self.delay);

        // receive response
        let mut resp = String::new();
        http_resp.read_to_string(&mut resp).expect("error reading from response");
        let resp = json::decode(&resp).expect("error decoding http response");
        match resp {
            HttpResponse::Append(idx) => {
                // return log index at which entry was appended
                return idx;
            }
            _ => panic!("http_client::append::wrong response type"),
        };
    }
}

// Class: DynamoQueue
// Interface with remote SharedLog, implements IndexedQueue
#[derive(Clone)]
pub struct DynamoQueue {
    pub client: Arc<Mutex<DynamoClient>>, // dynamo client
    index: i64, // our cached end of log index
}

impl DynamoQueue {
    pub fn new() -> DynamoQueue {
        DynamoQueue {
            client: Arc::new(Mutex::new(DynamoClient::new())),
            index: 0,
        }
    }
}

impl IndexedQueue for DynamoQueue {
    fn append(&mut self, e: Entry) -> LogIndex {
        let data = json::encode(&e).unwrap();
        loop {
            match self.client.lock().unwrap().put(self.index, &data, true) {
                Err(DynamoError::ValidationError(_)) => {
                    println!("validation error");
                    self.index += 1;
                }
                Err(_) => unimplemented!(),
                Ok(_) => {
                    println!("appended: {}", self.index);
                    break;
                }
            }
        }
        self.index += 1;
        return (self.index - 1) as LogIndex;
    }

    fn stream(&mut self,
              obj_ids: &HashSet<ObjId>,
              mut from: LogIndex,
              to: Option<LogIndex>)
              -> mpsc::Receiver<LogData> {
        let length = match self.client.lock().unwrap().length() {
            Err(_) => unimplemented!(),
            Ok(l) => l,
        };
        use self::LogData::LogEntry;
        let (tx, rx) = mpsc::channel();
        loop {
            // stop if we have read up to length or up to to 'to'
            if from >= length {
                return rx;
            }
            if to.is_some() && from > to.unwrap() {
                return rx;
            }
            match self.client.lock().unwrap().get(from as i64) {
                Ok(data) => {
                    let mut e: Entry = json::decode(&data).unwrap();
                    e.idx = Some(from);
                    let mut contains = false;
                    for op in e.operations {
                        if obj_ids.contains(&op.obj_id) {
                            contains = true;
                            break;
                        }
                    }
                    if !contains {
                        continue;
                    }
                    let mut entry: Entry = json::decode(&data).unwrap();
                    entry.idx = Some(from);
                    println!("entry: {:?}", entry);
                    tx.send(LogEntry(entry)).unwrap();
                    from += 1;
                }
                Err(err) => {
                    println!("Error streaming: {:#?}", err);
                    return rx;
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RequestType {
    Put = 0,
    Get = 1,
    Delete = 2,
    Length = 3,
}

#[derive(Serialize, Deserialize, Debug)]
struct DynamoRequest {
    request_number: i64,
    request_type: i64,
    conditional: bool,
    index: i64,
    data: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct DynamoResponse {
    request_number: i64,
    index: i64,
    data: String,
    length: i64,
    error: String,
    validation_error: bool,
}

#[derive(Debug)]
pub enum DynamoError {
    ValidationError(String),
    Error(String),
}

// Class: DynamoClient
// Connects to DynamoDB through a locally run go server
pub struct DynamoClient {
    ser: serde_json::Serializer<TcpStream>,
    de: serde_json::Deserializer<Bytes<TcpStream>>,
    request_number: i64,
}

impl DynamoClient {
    pub fn new() -> DynamoClient {
        let write_stream = TcpStream::connect("127.0.0.1:8080").unwrap();
        let read_stream = write_stream.try_clone().unwrap();
        let de = serde_json::Deserializer::new(read_stream.bytes());
        let ser = serde_json::Serializer::new(write_stream);
        DynamoClient {
            ser: ser,
            de: de,
            request_number: 0,
        }
    }
    fn make_request(&mut self, req: DynamoRequest) -> Result<DynamoResponse, DynamoError> {
        req.serialize(&mut self.ser).unwrap();
        let resp = DynamoResponse::deserialize(&mut self.de).unwrap();
        if !resp.error.is_empty() {
            if resp.validation_error {
                return Err(DynamoError::ValidationError(resp.error));
            }
            return Err(DynamoError::Error(resp.error));
        };
        return Ok(resp);
    }
    pub fn put(&mut self, index: i64, data: &str, conditional: bool) -> Result<(), DynamoError> {
        let req = DynamoRequest {
            request_number: self.request_number,
            request_type: RequestType::Put as i64,
            conditional: conditional,
            index: index,
            data: data.to_string(),
        };
        self.request_number += 1;
        try!(self.make_request(req));
        return Ok(());
    }
    pub fn get(&mut self, index: i64) -> Result<String, DynamoError> {
        let req = DynamoRequest {
            request_number: self.request_number,
            request_type: RequestType::Get as i64,
            conditional: false,
            index: index,
            data: "".to_string(),
        };
        self.request_number += 1;
        let resp = try!(self.make_request(req));
        return Ok(resp.data);
    }
    pub fn delete(&mut self, index: i64) -> Result<(), DynamoError> {
        let req = DynamoRequest {
            request_number: self.request_number,
            request_type: RequestType::Delete as i64,
            conditional: false,
            index: index,
            data: "".to_string(),
        };
        self.request_number += 1;
        try!(self.make_request(req));
        return Ok(());
    }
    pub fn length(&mut self) -> Result<i64, DynamoError> {
        let req = DynamoRequest {
            request_number: self.request_number,
            request_type: RequestType::Length as i64,
            conditional: false,
            index: 0,
            data: "".to_string(),
        };
        self.request_number += 1;
        let resp = try!(self.make_request(req));
        return Ok(resp.length);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::State::Encoded;
    use super::LogData::LogEntry;
    use std::thread;
    use std::sync::mpsc;
    use std::time::Duration;

    use http_server::HttpServer;
    enum ThreadMssg {
        Close,
    }

    fn entry() -> Entry {
        Entry::new(vec![(0, 0), (1, 0), (2, 0)].into_iter().collect(),
                   vec![1, 2].into_iter().collect(),
                   vec![
                Operation::new(0, Encoded("get(k0)".to_string())),
                Operation::new(1, Encoded("get(k1)".to_string())),
                Operation::new(2, Encoded("get(k2)".to_string())),
                Operation::new(1, Encoded("put(k1, 0)".to_string())),
                Operation::new(2, Encoded("put(k2, 1)".to_string())),
                   ],
                   TxType::None,
                   TxState::None)
    }

    fn stream_works(rx: mpsc::Receiver<LogData>, n: LogIndex) -> bool {
        let mut read = 0;
        let ent = entry();
        for e in rx {
            match e {
                LogEntry(e) => {
                    assert_eq!(e.idx.unwrap(), read);
                    assert_eq!(e.operations[0], ent.operations[0]);
                    read += 1;
                }
                _ => panic!("should not snapshot: too few entries"),
            }
        }
        assert_eq!(read, n);
        return true;
    }

    #[test]
    fn in_memory() {
        let mut q = InMemoryQueue::new();
        let n = 5;
        let obj_ids = &vec![0, 1, 2].into_iter().collect();
        for i in 0..n {
            let e = entry();
            assert_eq!(q.append(e), i as LogIndex);
        }
        let rx = q.stream(&obj_ids, 0, None);
        assert_eq!(stream_works(rx, n), true);
    }

    #[test]
    fn shared_queue() {
        let mut q1 = SharedQueue::new();
        let mut q2 = q1.clone();
        let mut q3 = q1.clone();
        let n = 5;
        let obj_ids = &vec![0, 1, 2].into_iter().collect();

        let child1 = thread::spawn(move || {
            // some work here
            for _ in 0..n {
                q1.append(entry());
            }
        });
        let child2 = thread::spawn(move || {
            for _ in 0..n {
                q2.append(entry());
            }
        });
        child1.join().unwrap();
        child2.join().unwrap();

        let rx = q3.stream(&obj_ids, 0, None);
        assert_eq!(stream_works(rx, n * 2), true);
    }


    #[test]
    fn http_client_server() {
        // More of an integration test
        let to_server = "http://127.0.0.1:6767";
        let server_addr = "127.0.0.1:6767";
        let (tx, rx) = mpsc::channel();

        let mut q = HttpClient::new(to_server);
        let child = thread::spawn(move || {
            let mut s = HttpServer::new(InMemoryQueue::new(), server_addr);
            match rx.recv().unwrap() {
                ThreadMssg::Close => {
                    s.close();
                }
            }

        });
        // give server the chance to start
        thread::sleep(Duration::from_millis(50));

        // client sends over some work via append
        let n = 5;
        let obj_ids = &vec![0, 1, 2].into_iter().collect();
        for _ in 0..n {
            q.append(entry());
        }

        // stream back the work
        let stream_rx = q.stream(&obj_ids, 0, None);
        assert_eq!(stream_works(stream_rx, n), true);

        tx.send(ThreadMssg::Close).unwrap();
        child.join().unwrap();
    }

    #[test]
    #[ignore]
    fn dynamo_client() {
        let n = 5;
        let mut client = DynamoClient::new();
        for i in 0..n {
            let _ = client.delete(i as i64);
        }
        assert!(client.get(0).is_err());


        // PUT REQUEST
        assert!(client.put(0, "data0", false).is_ok());
        let res = client.get(0);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "data0");

        // CONDITIONAL PUT
        let res = client.put(1, "data2", true);
        match res {
            Err(DynamoError::ValidationError(_)) => panic!("should succeed"),
            _ => {}
        }
        // DELETE
        assert!(client.delete(1).is_ok());
        assert!(client.get(1).is_err());; // getting after delete should fail

        // test incorrect conditional put
        let res = client.put(0, "data1", true);
        match res {
            Err(DynamoError::ValidationError(_)) => {}
            _ => panic!("should be validation error"),
        }

        // check for old value of data0
        let res = client.get(0);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "data0".to_string());

        // test delete
        assert!(client.get(0).is_ok());
        assert!(client.delete(0).is_ok());
        assert!(client.get(0).is_err());

        // Bad conditional put: condition should fail
        match client.put(0, "data1", true) {
            Err(DynamoError::ValidationError(_)) => panic!("should have been deleted"),
            _ => {}
        }

        assert!(client.get(0).is_ok());
        assert!(client.delete(0).is_ok());
        assert!(client.get(0).is_err());


        // Tests have to be done serially (not parallel) due to accessing dynamodb table
        // TEST APPENDING
        println!("Testing Dynamo Queue");
        let mut q = DynamoQueue::new();
        println!("Appending Entries");
        for i in 0..n {
            println!("entry: {}", i);
            let e = entry();
            assert_eq!(q.append(e), i as LogIndex);
        }

        // TEST STREAMING
        println!("Streaming Entries");
        let rx = q.stream(&vec![0, 1, 2].into_iter().collect(), 0, None);
        let mut read = 0;
        // Get default entry data
        let ent = entry();
        println!("Streaming and Comparing");
        for e in rx {
            println!("Matching e: {}", read);
            match e {
                LogEntry(e) => {
                    assert_eq!(e.idx.unwrap(), read);
                    assert_eq!(e.operations[0], ent.operations[0]);
                    read += 1;
                }
                _ => panic!("should not snapshot: too few entries"),
            }
            // assert_eq!(e, entry);
        }
        println!("DONE STREAMING");
        assert_eq!(read, n);
        let client = q.client;
        // CLEAR OUT OLD ENTRIES
        println!("deleting old entries");
        for i in 0..n {
            let _ = client.lock().unwrap().delete(i as i64);
        }
    }

}
