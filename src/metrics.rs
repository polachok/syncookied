use std::ops::Deref;
use std::collections::HashMap;
use std::sync::mpsc;
use ::influent;
use influent::client::udp::UdpClient;
use influent::measurement::{Value,Measurement};
use ::util;

#[derive(Debug)]
struct MetricSettings {
    name: String,
    tags: Vec<(String,String)>
}

#[derive(Debug)]
pub struct RegisterMessage {
    thread_id: u32,
    metric_id: u32,
    metric_name: String,
    tags: Vec<(String,String)>,
}

#[derive(Debug)]
pub struct DataMessage {
    thread_id: u32,
    metric_id: u32,
    value: i64,
}

#[derive(Debug)]
pub enum Message {
    Register(RegisterMessage),
    Data(DataMessage),
}

impl Message {
    pub fn register(thread_id: u32, metric_id: u32, name: String, tags: Vec<(String,String)>) -> Self {
        Message::Register(RegisterMessage {
             thread_id: thread_id,
             metric_id: metric_id,
             metric_name: name,
             tags: tags,
        })
    }

    pub fn point(thread_id: u32, metric_id: u32, value: i64) -> Self {
        Message::Data(DataMessage {
            thread_id: thread_id,
            metric_id: metric_id,
            value: value,
        })
    }
}

pub struct Collector<'a> {
    chan: mpsc::Receiver<Message>,
    client: Client<'a>,
    map: HashMap<(u32,u32),MetricSettings>,
}

impl<'a> Collector<'a> {
    pub fn new(chan: mpsc::Receiver<Message>, metrics_server: &'a str) -> Self {
        Collector { 
            chan: chan,
            client: Client::new(metrics_server),
            map: HashMap::new(),
        }
    }

    pub fn run(&mut self) {
        info!("Metrics collector starting");
        util::set_thread_name(&format!("syncookied/met"));
        for msg in self.chan.iter() {
            println!("{:?}", msg);
            match msg {
                Message::Register(reg) => {
                    let settings = MetricSettings {
                        name: reg.metric_name,
                        tags: reg.tags,
                    };
                    self.map.insert((reg.thread_id, reg.metric_id), settings);
                },
                Message::Data(data) => {
                    match self.map.get(&(data.thread_id, data.metric_id)) {
                        Some(settings) => {
                            println!("Received value {} for {:?}", data.value, settings);
                        },
                        None => {
                            error!("Unregistered metric: ({}, {})", data.thread_id, data.metric_id);
                        },
                    }
                },
            }
        }
    }
}

pub struct Client<'a> {
    inner: UdpClient<'a>,
}

impl<'a> Deref for Client<'a> {
    type Target = UdpClient<'a>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> Client<'a> {
    pub fn new(metrics_server: &'a str) -> Client<'a> {
        Client { inner: influent::create_udp_client(vec![metrics_server]) }
    }

    pub fn send(&self, metrics: &[Metric]) {
        use influent::client::Client;
        use std::mem;
        let _ = self.inner.write_many(unsafe { mem::transmute(metrics) }, None);
    }
}

pub struct Metric<'a> {
    inner: Measurement<'a>,
}

impl<'a> Metric<'a> {
    pub fn new_with_tags(name: &'a str, tags: &'a [(&'a str, &'a str)]) -> Metric<'a> {
        let mut m = Measurement::new(name);

        for &(ref key, ref val) in tags {
            m.add_tag(key, val);
        }
        Metric { inner: m }
    }

    pub fn add_tag(&mut self, tag: (&'a str, &'a str)) {
        self.inner.add_tag(tag.0, tag.1);
    }

    pub fn set_value(&mut self, val: i64) {
        self.inner.add_field("value", Value::Integer(val));
    }
}
