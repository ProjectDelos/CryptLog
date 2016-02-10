extern crate rustc_serialize;
extern crate hyper;

use std::io::Read;
use std::sync::Mutex;

use self::rustc_serialize::json;
use self::hyper::Server;
use self::hyper::net::Fresh;
use self::hyper::server::{Handler, Request, Response, Listening};

use indexed_queue::{IndexedQueue, LogIndex, Entry};
use http_data::{HttpRequest, HttpResponse};

pub struct HttpServer<Q: 'static> {
    iq: Q,
    listener: Listening,
}

struct HttpHandler<Q> {
    iq: Mutex<Q>,
}

impl<Q: IndexedQueue> HttpHandler<Q> {
    pub fn new(iq: Mutex<Q>) -> HttpHandler<Q> {
        return HttpHandler { iq: iq };
    }
}

impl<Q: IndexedQueue + Send> Handler for HttpHandler<Q> {
    fn handle(&self, mut req: Request, resp: Response<Fresh>) {
        match req.method {
            hyper::Post => {
                let mut body = String::new();
                req.read_to_string(&mut body).unwrap();
                let body = json::decode(&body).unwrap();
                match body {
                    HttpRequest::Append(entry) => {
                        let idx = self.iq.lock().unwrap().append(entry);
                        let r = HttpResponse::Append(idx);
                        let r = json::encode(&r).unwrap();
                        resp.send(r.as_bytes()).unwrap();
                    }
                    HttpRequest::Stream(ref obj_ids, from, to) => {
                        let mut entries: Vec<Entry> = Vec::new();
                        let rx = self.iq.lock().unwrap().stream(obj_ids, from, to);
                        for e in &rx {
                            entries.push(e);
                        }
                        let r = HttpResponse::Stream(entries);
                        let r = json::encode(&r).unwrap();
                        resp.send(r.as_bytes()).unwrap();
                    }
                };
            }
            _ => unimplemented!(),
        };
    }
}

impl<Q> HttpServer<Q> where Q: IndexedQueue + Clone + Sync + Send
{
    pub fn new(iq: Q, server_addr: &str) -> HttpServer<Q> {
        let handler = HttpHandler::new(Mutex::new(iq.clone()));
        return HttpServer {
            iq: iq,
            listener: Server::http(&server_addr).unwrap().handle(handler).unwrap(),
        };
    }

    pub fn close(&mut self) {
        self.listener.close().unwrap();
    }
}


#[cfg(test)]
mod test {
    use indexed_queue::InMemoryQueue;
    use super::HttpServer;
    use std::thread;

    #[test]
    fn http_server() {
        let iq = InMemoryQueue::new();
        let child = thread::spawn(move || {
            let mut s = HttpServer::new(iq, "127.0.0.1:6768");
            s.close();
        });
        child.join().unwrap()
    }
}
