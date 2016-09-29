use std::ops::Deref;
use std::sync::mpsc;
use ::influent;
use influent::client::udp::UdpClient;
use influent::measurement::{Value,Measurement};
use ::util;

pub struct Collector<'a> {
    chan: mpsc::Receiver<Metric>,
    client: Client<'a>,
}

impl<'a> Collector<'a> {
    pub fn new(chan: mpsc::Receiver<Metric>, metrics_server: &'a str) -> Self {
        Collector { 
            chan: chan,
            client: Client::new(metrics_server),
        }
    }

    pub fn run(&mut self) {
        info!("Metrics collector starting");
        util::set_thread_name(&format!("syncookied/met"));
        for msg in self.chan.iter() {
            self.client.send(msg);
        }
    }
}

pub struct Client<'a> {
    inner: UdpClient<'a,String>,
}

impl<'a> Deref for Client<'a> {
    type Target = UdpClient<'a,String>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> Client<'a> {
    pub fn new(metrics_server: &'a str) -> Client<'a> {
        Client { inner: influent::create_udp_client(vec![metrics_server]) }
    }

    pub fn send(&self, metric: Metric) {
        use influent::client::Client;
        use std::mem;
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

    pub fn set_value(&mut self, val: i64) {
        self.inner.add_field("value", Value::Integer(val));
    }
}
