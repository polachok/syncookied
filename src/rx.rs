/// Receiver thread
use std::time::{self,Duration};
use std::thread;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};
use ::netmap::{self, NetmapDescriptor, RxSlot};
use ::ForwardedPacket;
use ::packet::{self,IngressPacket};
use ::pnet::util::MacAddr;
use ::util;
use ::libc;
use ::spsc;
use ::packet::Action;
use ::metrics;

#[derive(Debug,Default)]
struct RxStats {
    pub received: u32,
    pub dropped: u32,
    pub forwarded: u32,
    pub queued: u32,
    pub overflow: u32,
    pub failed: u32,
}

impl RxStats {
    pub fn empty() -> Self {
        Default::default()
    }

    pub fn clear(&mut self) {
        *self = Default::default();
    }

}

pub struct Receiver<'a> {
    cpu: usize,
    chan_reply: spsc::Producer<IngressPacket>,
    chan_fwd: spsc::Producer<ForwardedPacket>,
    netmap: &'a mut NetmapDescriptor,
    stats: RxStats,
    lock: Arc<AtomicUsize>,
    mac: MacAddr,
    ring_num: u16,
    metrics: Option<metrics::Client>,
}

#[inline(always)]
fn adaptive_push<T>(chan: &spsc::Producer<T>, pkt: T, retries: usize) -> Option<T> {
    // fast path
    let mut packet = pkt;
    for _ in 0..retries - 1 {
        if let Some(pkt) = chan.try_push(packet) {
            packet = pkt;
            unsafe { libc::sched_yield() };
        } else {
            return None;
        }
    }
    // no luck
    chan.try_push(packet)
}


impl<'a> Receiver<'a> {
    pub fn new(ring_num: u16, cpu: usize,
               chan_fwd: spsc::Producer<ForwardedPacket>,
               chan_reply: spsc::Producer<IngressPacket>,
               netmap: &'a mut NetmapDescriptor,
               lock: Arc<AtomicUsize>,
               mac: MacAddr,
               metrics: Option<mpsc::Sender<metrics::Message>>) -> Self {
        Receiver {
            ring_num: ring_num,
            cpu: cpu,
            chan_fwd: chan_fwd,
            chan_reply: chan_reply,
            netmap: netmap,
            lock: lock,
            stats: RxStats::empty(),
            mac: mac,
            metrics: metrics.map(metrics::Client::new),
        }
    }

    fn update_routing_cache(&mut self) {
        ::RoutingTable::sync_tables();
    }

    fn send_metrics(chan: &mut metrics::Client,
                    stats: &RxStats, seconds: u32,
                    tags: &[(&'static str, String)]) {
      
        let mut ms = vec![
            metrics::Metric::new_with_tags("rx_pps", tags),
            metrics::Metric::new_with_tags("rx_drop", tags),
            metrics::Metric::new_with_tags("rx_forwarded", tags),
            metrics::Metric::new_with_tags("rx_queued", tags),
            metrics::Metric::new_with_tags("rx_overflow", tags),
            metrics::Metric::new_with_tags("rx_failed", tags),
        ];
        ms[0].set_value((stats.received / seconds) as i64);
        ms[1].set_value((stats.dropped / seconds) as i64);
        ms[2].set_value((stats.forwarded / seconds) as i64);
        ms[3].set_value((stats.queued / seconds) as i64);
        ms[4].set_value((stats.overflow / seconds) as i64);
        ms[5].set_value((stats.failed / seconds) as i64);

        for m in ms.into_iter() {
            chan.send(m);
        }

        for ip in ::RoutingTable::get_ips() {
            let ip_tag = format!("{}", ip);
            let mut m = metrics::Metric::new_with_tags("rx_pps_ip", tags);
            m.add_tag(("dest_ip", ip_tag.clone()));
            ::RoutingTable::with_host_config_mut(ip, |hc| {
                    m.set_value((hc.packets / seconds) as i64);
                    hc.packets = 0;
            });
            chan.send(m);
            for i in 0..65535 {
                let mut m = metrics::Metric::new_with_tags("rx_pps_ip_port", tags);
                m.add_tag(("dest_ip", ip_tag.clone()));
                m.add_tag(("dest_port", i.to_string()));

                ::RoutingTable::with_host_config_mut(ip, |hc| {
                        let val = (hc.packets_per_port[i] / seconds) as i64;
                        if val > 0 {
                            m.set_value(val);
                            hc.packets_per_port[i] = 0;
                        }
                });
                chan.send(m);
            }
        }
    }

    // main RX loop
    pub fn run(mut self) {
        let hostname = util::get_host_name().unwrap();
        let queue = format!("{}", self.ring_num);
        let ifname = self.netmap.get_ifname();
        let tags = vec![("queue", queue), ("host", hostname), ("iface", ifname)];

        info!("RX loop for ring {:?}", self.ring_num);
        info!("Rx rings: {:?}", self.netmap.get_rx_rings());
        util::set_thread_name(&format!("syncookied/rx{:02}", self.ring_num));

        util::set_cpu_prio(self.cpu, 20);

        /* wait for card to reinitialize */
        thread::sleep(Duration::new(1, self.ring_num as u32 * 100));
        info!("[RX#{}] started", self.ring_num);

        self.update_routing_cache();

        let mut before = time::Instant::now();
        let seconds: u32 = 5;
        let mut rate: u32 = 0;
        let ival = time::Duration::new(seconds as u64, 0);

        loop {
            if let Some(_) = self.netmap.poll(netmap::Direction::Input) {
                if let Some(ring) = self.netmap.rx_iter().next() {
                    let mut fw = false;
                    for (slot, buf) in ring.iter() {
                        self.stats.received += 1;
                        if rate < 1000 {
                            ::RoutingTable::sync_tables();
                        }
                        match packet::handle_input(buf, self.mac) {
                            Action::Drop => {
                                self.stats.dropped += 1;
                            },
                            Action::Forward(fwd_mac) => {
                                let to_forward = &self.lock;

                                let slot_ptr: usize = slot as *mut RxSlot as usize;
                                let buf_ptr: usize = buf.as_ptr() as usize;

/*
                                println!("[RX#{}]: forwarded slot: {:x} buf: {:x}, buf_idx: {}",
                                    ring_num, slot_ptr, buf_ptr, slot.get_buf_idx());
*/
                                to_forward.fetch_add(1, Ordering::SeqCst);
                                let chan = &self.chan_fwd;
                                let packet = ForwardedPacket {
                                    slot_ptr: slot_ptr,
                                    buf_ptr: buf_ptr,
                                    destination_mac: fwd_mac,
                                };
                                match adaptive_push(chan, packet, 1) {
                                    Some(_) => self.stats.failed += 1,
                                    None => {
                                        self.stats.forwarded += 1;
                                        fw = true;
                                    },
                                }
                            },
                            Action::Reply(packet) => {
                                match self.chan_reply.try_push(packet) {
                                    Some(_) => {
                                        self.stats.overflow += 1;
                                        self.stats.failed += 1;
                                    },
                                    None => self.stats.queued += 1,
                                }
                            },
                        }
                    }
                    /*
                     * // forwarding to host ring is not yet implemented
                     * if fw {
                     *  ring.set_flags(netmap::NR_FORWARD as u32);
                     *  }
                     */
                    if fw {
                        let to_forward = &self.lock;
                        while to_forward.load(Ordering::SeqCst) != 0 {
                            unsafe { libc::sched_yield() };
                            //println!("[RX#{}]: waiting for forwarding to happen, {} left", ring_num, to_forward.load(Ordering::SeqCst));
                        }
                    }
                }
            }
            if before.elapsed() >= ival {
                if let Some(ref mut metrics_chan) = self.metrics {
                    let stats = &self.stats;
                    Self::send_metrics(metrics_chan, &stats, seconds, &tags[..]);
                }
                rate = self.stats.received/seconds;
                debug!("[RX#{}]: received: {}Pkts/s, dropped: {}Pkts/s, forwarded: {}Pkts/s, queued: {}Pkts/s, overflowed: {}Pkts/s, failed: {}Pkts/s",
                            self.ring_num, rate, self.stats.dropped/seconds,
                            self.stats.forwarded/seconds, self.stats.queued/seconds,
                            self.stats.overflow/seconds, self.stats.failed/seconds);
                self.stats.clear();
                before = time::Instant::now();
                self.update_routing_cache();
            }
        }
    }
}
