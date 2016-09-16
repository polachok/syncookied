use std::ops::Deref;
use std::collections::HashMap;
use std::sync::mpsc;
use ::influent;
use influent::client::udp::UdpClient;
use influent::client::Client;
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
    client: UdpClient<'a>,
    map: HashMap<(u32,u32),MetricSettings>,
}

impl<'a> Collector<'a> {
    pub fn new(chan: mpsc::Receiver<Message>, metrics_server: &'a str) -> Self {
        Collector { 
            chan: chan,
            client: influent::create_udp_client(vec![metrics_server]),
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
                            let mut m = Measurement::new(&settings.name);

                            for &(ref key, ref val) in settings.tags.iter() {
                                m.add_tag(&key, &val);
                            }
                            m.add_field("value", Value::Integer(data.value));
                            self.client.write_many(&[m], None);
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
