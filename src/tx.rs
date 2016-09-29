/// Transfer thread
use std::mem;
use std::time::{self,Duration};
use std::thread;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};
use ::netmap::{self, NetmapDescriptor, TxSlot, NetmapSlot};
use ::packet::{self, IngressPacket};
use ::ForwardedPacket;
use ::pnet::util::MacAddr;
use ::pnet::packet::ethernet::MutableEthernetPacket;
use ::spsc;
use ::util;
use ::metrics;

#[derive(Debug,Default)]
struct TxStats {
    pub sent: u32,
    pub failed: u32,
}

impl TxStats {
    pub fn empty() -> Self {
        Default::default()
    }

    pub fn clear(&mut self) {
        *self = Default::default();
    }
}

pub struct Sender<'a> {
    ring_num: u16,
    cpu: usize,
    chan: Option<spsc::Consumer<IngressPacket>>,
    fwd_chan: Option<spsc::Consumer<ForwardedPacket>>,
    netmap: &'a mut NetmapDescriptor,
    lock: Arc<AtomicUsize>,
    source_mac: MacAddr,
    stats: TxStats,
    metrics: Option<metrics::Client>,
}

impl<'a> Sender<'a> {
    pub fn new(ring_num: u16, cpu: usize,
               chan: Option<spsc::Consumer<IngressPacket>>,
               fwd_chan: Option<spsc::Consumer<ForwardedPacket>>,
               netmap: &'a mut NetmapDescriptor,
               lock: Arc<AtomicUsize>,
               source_mac: MacAddr,
               metrics: Option<mpsc::Sender<metrics::Message>>) -> Sender<'a> {
        Sender {
            ring_num: ring_num,
            cpu: cpu,
            chan: chan,
            fwd_chan: fwd_chan,
            netmap: netmap,
            lock: lock,
            source_mac: source_mac,
            stats: TxStats::empty(),
            metrics: metrics.map(metrics::Client::new),
        }
    }

    fn send_metrics(chan: &mut metrics::Client,
                    stats: &TxStats, seconds: u32,
                    tags: &[(&'static str, String)]) {
        let mut ms = vec![
            metrics::Metric::new_with_tags("tx_pps", tags),
            metrics::Metric::new_with_tags("tx_failed", tags),
        ];
        ms[0].set_value((stats.sent / seconds) as i64);
        ms[1].set_value((stats.failed / seconds) as i64);
        for m in ms.into_iter() {
            chan.send(m);
        }
    }

    // main transfer loop
    pub fn run(mut self) {
        info!("TX loop for ring {:?} starting. Rings: {:?}", self.ring_num, self.netmap.get_tx_rings());
        let hostname = util::get_host_name().unwrap();
        let queue = format!("{}", self.ring_num);
        let ifname = self.netmap.get_ifname();
        let tags = [("queue", queue), ("host", hostname), ("iface", ifname)];

        util::set_thread_name(&format!("syncookied/tx{:02}", self.ring_num));
        util::set_cpu_prio(self.cpu, 20);

        /* wait for card to reinitialize */
        thread::sleep(Duration::new(1, self.ring_num as u32 * 100));
        info!("[TX#{}] started", self.ring_num);

        let mut before = time::Instant::now();
        let seconds: u32 = 7;
        let ival = time::Duration::new(seconds as u64, 0);
        let mut rate: u32 = 0;

        Self::update_routing_cache();

        loop {
            /* block and wait for packet in queue */
            if let Some(_) = self.netmap.poll(netmap::Direction::Output) {
                if let Some(ring) = self.netmap.tx_iter().next() {
                    let mut tx_iter = ring.iter();

                    /* send one packet */
                    if let Some((slot, buf)) = tx_iter.next() {
                        let stats = &mut self.stats;
                        let lock = &mut self.lock;
                        let ring_num = self.ring_num;
                        let source_mac = self.source_mac;
                        let reply_chan = self.chan.as_ref();
                        let fwd_chan = self.fwd_chan.as_ref();

                        /* try fwd chan first */
                        if let Some(fwd_chan) = fwd_chan {
                            if let None = fwd_chan.try_pop_with(|pkt|
                                Self::forward(pkt, slot, stats, lock,
                                           ring_num, source_mac)
                            ) { /* if nothing in it, try reply_chan */
                                if let Some(reply_chan) = reply_chan {
                                    if let None = reply_chan.try_pop_with(|pkt|
                                        Self::reply(pkt, slot, buf, stats,
                                           ring_num, source_mac)) {
                                        if rate <= 1000 {
                                            Self::update_routing_cache();
                                        }
                                        thread::sleep(Duration::new(0, 100));
                                    }
                                }
                            }
                        } else {
                            if let Some(reply_chan) = reply_chan {
                                if let None = reply_chan.try_pop_with(|pkt|
                                    Self::reply(pkt, slot, buf, stats,
                                       ring_num, source_mac)) {
                                    if rate <= 1000 {
                                        Self::update_routing_cache();
                                    }
                                    thread::sleep(Duration::new(0, 100));
                                }
                            }
                        }
                    }
                    /* try to send more if we have any (non-blocking) */
                    for (slot, buf) in tx_iter {
                        let stats = &mut self.stats;
                        let lock = &mut self.lock;
                        let ring_num = self.ring_num;
                        let source_mac = self.source_mac;
                        let reply_chan = self.chan.as_ref();
                        let fwd_chan = self.fwd_chan.as_ref();

                        /* try fwd chan first */
                        if let Some(fwd_chan) = fwd_chan {
                            if let None = fwd_chan.try_pop_with(|pkt|
                                Self::forward(pkt, slot, stats, lock,
                                           ring_num, source_mac)
                            ) { /* if nothing in it, try reply_chan */
                                if let Some(reply_chan) = reply_chan {
                                    if let None = reply_chan.try_pop_with(|pkt|
                                        Self::reply(pkt, slot, buf, stats,
                                           ring_num, source_mac)) {
                                        thread::sleep(Duration::new(0, 100));
                                    }
                                }
                            }
                        } else {
                            if let Some(reply_chan) = reply_chan {
                                if let None = reply_chan.try_pop_with(|pkt|
                                    Self::reply(pkt, slot, buf, stats,
                                       ring_num, source_mac)) {
                                    thread::sleep(Duration::new(0, 100));
                                }
                            }
                        }
/*
                        if rate <= 1000 {
                            break; // do tx sync on every packet if we receive
                            // small amount of packets
                        } else if rate <= 10_000 && self.stats.sent % 64 == 0 {
                            break;
                        } else if rate <= 100_000 && self.stats.sent % 128 == 0 {
                            break;
                        }
*/
                    }
                }
            }
            if before.elapsed() >= ival {
                if let Some(ref mut metrics) = self.metrics {
                    let stats = &self.stats;
                    Self::send_metrics(metrics, stats, seconds, &tags);
                }
                rate = self.stats.sent/seconds;
                debug!("[TX#{}]: sent {}Pkts/s, failed {}Pkts/s", self.ring_num, rate, self.stats.failed/seconds);
                self.stats.clear();
                before = time::Instant::now();
                Self::update_routing_cache();
            }
        }
    }

    fn update_routing_cache() {
        ::RoutingTable::sync_tables();
    }

    #[inline]
    fn reply(pkt: &packet::IngressPacket, slot: &mut TxSlot, buf: &mut [u8], stats: &mut TxStats,
            _ring_num: u16, source_mac: MacAddr) {
        if let Some(len) = packet::handle_reply(pkt, source_mac, buf) {
            //debug!("[TX#{}] SENDING PACKET\n", ring_num);
            slot.set_flags(0); //netmap::NS_BUF_CHANGED as u16 /* | netmap::NS_REPORT as u16 */);
            slot.set_len(len as u16);
            stats.sent += 1;
        } else {
            stats.failed += 1;
        }
    }

    #[inline]
    fn forward(pkt: &ForwardedPacket, slot: &mut TxSlot, stats: &mut TxStats,
            lock: &mut Arc<AtomicUsize>, _ring_num: u16, source_mac: MacAddr) {
        use std::slice;
        /* swap buffers (zero copy) */
        let rx_slot: &mut TxSlot = unsafe { mem::transmute(pkt.slot_ptr as *mut TxSlot) };
        let tx_idx = slot.get_buf_idx();
        let tx_len = slot.get_len();

        slot.set_buf_idx(rx_slot.get_buf_idx());
        slot.set_len(rx_slot.get_len());
        slot.set_flags(netmap::NS_BUF_CHANGED);

        rx_slot.set_buf_idx(tx_idx);
        rx_slot.set_len(tx_len);
        rx_slot.set_flags(netmap::NS_BUF_CHANGED as u16);

        let to_forward = &lock;
        to_forward.fetch_sub(1, Ordering::SeqCst);

        let mut buf = unsafe { slice::from_raw_parts_mut::<u8>(pkt.buf_ptr as *mut u8, slot.get_len() as usize) };
        /*
           {
           packet::dump_input(&buf);
           debug!("[TX#{}]: received slot: {:x} buf: {:x}, buf_idx: {} (was buf_idx: {})",
           ring_num, slot_ptr, buf_ptr, slot.get_buf_idx(), tx_idx);
           }
           */
        {
            let mut eth = MutableEthernetPacket::new(&mut buf[0..]).unwrap();
            eth.set_source(source_mac);
            eth.set_destination(pkt.destination_mac);
        }
        stats.sent += 1;
    }

    /*
    #[inline]
    fn send(pkt: &OutgoingPacket, slot: &mut TxSlot, buf: &mut [u8], stats: &mut TxStats,
            lock: &mut Arc<AtomicUsize>, _ring_num: u16, source_mac: MacAddr) {
        match pkt {
            &OutgoingPacket::Ingress(ref pkt) => {
                if let Some(len) = packet::handle_reply(&pkt, source_mac, buf) {
                    //debug!("[TX#{}] SENDING PACKET\n", ring_num);
                    slot.set_flags(0); //netmap::NS_BUF_CHANGED as u16 /* | netmap::NS_REPORT as u16 */);
                    slot.set_len(len as u16);
                    stats.sent += 1;
                } else {
                    stats.failed += 1;
                }
            },
            &OutgoingPacket::Forwarded((slot_ptr, buf_ptr, destination_mac)) => {
                use std::slice;
                /* swap buffers (zero copy) */
                let rx_slot: &mut TxSlot = unsafe { mem::transmute(slot_ptr as *mut TxSlot) };
                let tx_idx = slot.get_buf_idx();
                let tx_len = slot.get_len();

                slot.set_buf_idx(rx_slot.get_buf_idx());
                slot.set_len(rx_slot.get_len());
                slot.set_flags(netmap::NS_BUF_CHANGED);

                rx_slot.set_buf_idx(tx_idx);
                rx_slot.set_len(tx_len);
                rx_slot.set_flags(netmap::NS_BUF_CHANGED as u16);

                let to_forward = &lock;
                to_forward.fetch_sub(1, Ordering::SeqCst);
                
                let mut buf = unsafe { slice::from_raw_parts_mut::<u8>(buf_ptr as *mut u8, slot.get_len() as usize) };
    /*
                {
                    packet::dump_input(&buf);
                    debug!("[TX#{}]: received slot: {:x} buf: {:x}, buf_idx: {} (was buf_idx: {})",
                        ring_num, slot_ptr, buf_ptr, slot.get_buf_idx(), tx_idx);
                }
    */
                {
                    let mut eth = MutableEthernetPacket::new(&mut buf[0..]).unwrap();
                    eth.set_source(source_mac);
                    eth.set_destination(destination_mac);
                }
                stats.sent += 1;
            }
        }
    }
    */
}
