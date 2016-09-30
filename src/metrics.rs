use std::ops::Deref;
use std::sync::mpsc;
use std::collections::HashMap;
use ::influent;
use influent::client::udp::UdpClient;
use influent::measurement::{Value,Measurement};
use ::util;
use ::uuid::Uuid;
use std::hash::{Hash, SipHasher, Hasher};

pub struct Client {
    chan: mpsc::Sender<Message>,
    map: HashMap<u64, Uuid>
}

impl Client {
    pub fn new(chan: mpsc::Sender<Message>) -> Self {
        Client {
            chan: chan,
            map: HashMap::new(),
        }
    }

    fn hash_metric(metric: &Metric) -> u64 {
        let mut s = SipHasher::new();
        let name = metric.inner.key;
        name.hash(&mut s);
        for tag in metric.inner.tags.iter() {
            tag.hash(&mut s);
        }
        s.finish()
    }

    pub fn send(&mut self, metric: Metric) {
        let chan = &self.chan;
        let h = Self::hash_metric(&metric);
        let val = match metric.inner.fields.get("value").unwrap() {
            &Value::Integer(i) => i,
            _ => panic!("shouldn't be possible"),
        };
        let val2 = match metric.inner.fields.get("value2") {
            Some(&Value::Integer(i)) => Some(i),
            _ => None,
        };

        let id = self.map.entry(h).or_insert_with(|| {
            let mut tags = Vec::new();
            let name = metric.inner.key;
            for (name, val) in metric.inner.tags.iter() {
                tags.push((*name, val.to_owned()));
            }
            let m = Message::register(name, tags);
            let id = m.id();
            chan.send(m);
            id
        });
        chan.send(Message::point(id.to_owned(), val, val2));
    }
}

#[derive(Debug)]
pub enum Message {
    Register(Uuid, &'static str, Vec<(&'static str, String)>),
    Point(Uuid, i64, Option<i64>),
}

impl Message {
    fn register(name: &'static str, tags: Vec<(&'static str, String)>) -> Message {
        let uuid = Uuid::new_v4();
        Message::Register(uuid, name, tags)
    }

    fn point(id: Uuid, val: i64, val2: Option<i64>) -> Message {
        Message::Point(id, val, val2)
    }

    pub fn id(&self) -> Uuid {
        match self {
            &Message::Register(uuid, _, _) => uuid,
            &Message::Point(uuid, _, _) => uuid,
        }
    }
}

pub struct Collector<'a> {
    chan: mpsc::Receiver<Message>,
    client: InfluxClient<'a>,
    map: HashMap<Uuid, (&'static str, Vec<(&'static str, String)>)>,
}

impl<'a> Collector<'a> {
    pub fn new(chan: mpsc::Receiver<Message>, metrics_server: &'a str) -> Self {
        Collector { 
            chan: chan,
            client: InfluxClient::new(metrics_server),
            map: HashMap::new(),
        }
    }

    pub fn run(&mut self) {
        info!("Metrics collector starting");
        util::set_thread_name(&format!("syncookied/met"));

        for msg in self.chan.iter() {
            match msg {
                Message::Register(id, name, tags) => {
                    self.map.insert(id, (name, tags));
                },
                Message::Point(id, val, val2) => {
                    match self.map.get(&id) {
                        Some(&(name, ref tags)) => {
                            let mut m = Metric::new_with_tags(name, &tags);
                            m.set_value(val);
                            if let Some(val2) = val2 {
                                m.set_field("value2", val2);
                            }
                            self.client.send(m);
                        },
                        None => error!("unregistered metric"),
                    }
                },
            }
        }
    }
}

pub struct InfluxClient<'a> {
    inner: UdpClient<'a,String>,
}

impl<'a> Deref for InfluxClient<'a> {
    type Target = UdpClient<'a,String>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> InfluxClient<'a> {
    pub fn new(metrics_server: &'a str) -> InfluxClient<'a> {
        InfluxClient { inner: influent::create_udp_client(vec![metrics_server]) }
    }

    pub fn send(&self, metric: Metric) {
        use influent::client::Client;
        let _ = self.inner.write_one(metric.inner, None);
    }
}

#[derive(Debug)]
pub struct Metric {
    inner: Measurement<'static,String>,
}

impl Metric {
    pub fn new_with_tags(name: &'static str, tags: &[(&'static str,String)]) -> Metric {
        let mut m = Measurement::new(name);

        for &(ref key, ref val) in tags {
            m.add_tag(key, val.clone());
        }
        Metric { inner: m }
    }

    pub fn add_tag(&mut self, tag: (&'static str, String)) {
        self.inner.add_tag(tag.0, tag.1);
    }

    pub fn set_field(&mut self, name: &'static str, val: i64) {
        self.inner.add_field(name, Value::Integer(val));
    }

    pub fn set_value(&mut self, val: i64) {
        self.set_field("value", val);
    }
}
