use std::mem;
use std::time::{self,Duration};
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::Write;
use ::ForwardedPacket;
use ::pnet::util::MacAddr;
use ::pnet::packet::ethernet::MutableEthernetPacket;
use ::spsc;
use ::util;
use ::metrics;
use ::netmap::{NetmapSlot,RxSlot};
use ::tuntap::Queue;

pub struct TapForwarder<'a> {
    ring_num: u16,
    cpu: usize,
    fwd_chan: spsc::Consumer<ForwardedPacket>,
    tap: Queue,
    lock: Arc<AtomicUsize>,
    source_mac: MacAddr,
    //stats: TxStats,
    metrics_addr: Option<&'a str>,
}

impl<'a> TapForwarder<'a> {
    pub fn new(ring_num: u16, cpu: usize,
               fwd_chan: spsc::Consumer<ForwardedPacket>,
               tap: Queue,
               lock: Arc<AtomicUsize>,
               source_mac: MacAddr,
               metrics_addr: Option<&'a str>) -> Self {
        TapForwarder {
            ring_num: ring_num,
            cpu: cpu,
            fwd_chan: fwd_chan,
            tap: tap,
            lock: lock,
            source_mac: source_mac,
            metrics_addr: metrics_addr,
        }
    }

    pub fn run(mut self) {
        info!("TAP loop for ring {:?} starting.", self.ring_num);

        util::set_thread_name(&format!("syncookied/tx{:02}", self.ring_num));
        util::set_cpu_prio(self.cpu, 20);

        loop {
            let fwd_chan = &self.fwd_chan;
            let lock = &mut self.lock;
            let ring_num = self.ring_num;
            let source_mac = self.source_mac;
            let tap = &mut self.tap;
            if let None = fwd_chan.try_pop_with(|pkt|
                Self::forward(pkt, tap, /*stats, */lock, ring_num, source_mac)
                ) {
            }
        }
    }

    fn forward(pkt: &ForwardedPacket/*, stats: &mut TxStats*/, tap: &mut Queue,
            lock: &mut Arc<AtomicUsize>, _ring_num: u16, source_mac: MacAddr) {
        use std::slice;

        let rx_slot: &mut RxSlot = unsafe { mem::transmute(pkt.slot_ptr as *mut RxSlot) };
        let mut buf = unsafe { slice::from_raw_parts_mut::<u8>(pkt.buf_ptr as *mut u8, rx_slot.get_len() as usize) };
        {
            let mut eth = MutableEthernetPacket::new(&mut buf[0..]).unwrap();
            eth.set_destination(source_mac); /* XXX */
        }

        tap.as_mut().write(buf);

        let to_forward = &lock;
        to_forward.fetch_sub(1, Ordering::SeqCst);
    }
}
