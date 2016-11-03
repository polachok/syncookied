// State table for TCP connections

use std::fmt;
use std::thread;
use std::net::Ipv4Addr;
use std::time::Duration;
use std::hash::BuildHasherDefault;
use concurrent_hash_map::ConcurrentHashMap;

#[derive(Debug,Eq,PartialEq,Copy,Clone)]
pub enum ConnState {
    Established, // first ACK received and valid
    Closing, // FIN received
}

impl From<u64> for ConnState {
    fn from(x: u64) -> Self {
        match x {
            0 => ConnState::Established,
            1 => ConnState::Closing,
            x => panic!("invalid connection state {}", x),
        }
    }
}

#[derive(Clone)]
pub struct StateTable {
    map: ConcurrentHashMap<u64,u64,BuildHasherDefault<::fnv::FnvHasher>>,
}

impl fmt::Debug for StateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.map.len() == 0 {
            try!(write!(f, "StateTable empty\n"));
        }
        let entries = self.map.entries();
        fn decode_key(k: u64) -> (Ipv4Addr, u16, u16) {
            let ip = Ipv4Addr::from((k >> 32) as u32);
            let src_port = ((k & 0xffffffff) >> 16) as u16;
            let dst_port = k as u16;
            (ip, src_port, dst_port)
        }
        fn decode_val(v: u64) -> ConnState {
            ConnState::from((v & 0xffffffff) - 1)
        }
        for entry in &entries {
            try!(write!(f, "{:?} -> {:?}\n", decode_key(entry.0), decode_val(entry.1)));
        }
        write!(f, "StateTable: {} entries\n", self.map.len())
    }
}

impl StateTable {
    pub fn new(size: usize) -> Self {
        StateTable {
            map: ConcurrentHashMap::new_with_options(size as u32,
                 1024, 0.8,
                 BuildHasherDefault::<::fnv::FnvHasher>::default()),
        }
    }

    pub fn set_state(&mut self, ip: Ipv4Addr, source_port: u16, dest_port: u16, ts: u32, state: ConnState) {
        let int_ip = u32::from(ip) as u64;
        let key: u64 = int_ip << 32
                         | (source_port as u64) << 16
                         | dest_port as u64;
        let val: u64 = (ts as u64) << 32 | ((state as u64) + 1);
        self.map.insert(key, val);
    }

    pub fn get_state(&self, ip: Ipv4Addr, source_port: u16, dest_port: u16) -> Option<(u32,ConnState)> {
        let int_ip = u32::from(ip) as u64;
        let key: u64 = int_ip << 32
                         | (source_port as u64) << 16
                         | dest_port as u64;
        self.map.get(key).map(|val| ((val >> 32) as u32, ConnState::from((val & 0xffffffff) - 1)))
    }

    pub fn delete_state(&mut self, ip: Ipv4Addr, source_port: u16, dest_port: u16) {
        let int_ip = u32::from(ip) as u64;
        let key: u64 = int_ip << 32
                         | (source_port as u64) << 16
                         | dest_port as u64;
        self.map.remove(key);
    }
}

pub fn state_table_gc() {
    const CLOSING_TIMEOUT: u32 = 120;
    const ESTABLISHED_TIMEOUT: u32 = 600;

    fn decode_val(val: u64) -> (ConnState, u32) {
        let ts = (val >> 32) as u32;
        let cs = ConnState::from((val & 0xffffffff) - 1);
        (cs, ts)
    }
    fn decode_key(k: u64) -> (Ipv4Addr, u16, u16) {
            let ip = Ipv4Addr::from((k >> 32) as u32);
            let src_port = ((k & 0xffffffff) >> 16) as u16;
            let dst_port = k as u16;
            (ip, src_port, dst_port)
    }
    loop {
        thread::sleep(Duration::new(30, 0));
        ::RoutingTable::sync_tables();
        debug!("Dumping table states");
        ::RoutingTable::dump_states();
        debug!("Starting GC");
        let ips = ::RoutingTable::get_ips();
        for ip in ips {
            let mut entries = vec![];
            let mut timestamp = 0;
            let mut hz = 300;
            ::RoutingTable::with_host_config(ip, |hc| {
                entries = hc.state_table.map.entries();
                timestamp = hc.tcp_timestamp;
                hz = hc.hz;
            });
            for e in entries {
                let k = e.0;
                let (cs, ts) = decode_val(e.1);
                debug!("Curr. ts: {}, entry ts: {}", timestamp, ts);
                match (cs, ts) {
                    (ConnState::Closing, ts) => if ts < timestamp - CLOSING_TIMEOUT * hz {
                        ::RoutingTable::with_host_config_mut(ip, |hc| {
                            let (ip, sport, dport) = decode_key(k);
                            debug!("Deleting state for {:?} {} {}", ip, sport, dport);
                            hc.state_table.delete_state(ip, sport, dport);
                        });
                    },
                    (ConnState::Established, ts) => if ts < timestamp - ESTABLISHED_TIMEOUT * hz {
                        ::RoutingTable::with_host_config_mut(ip, |hc| {
                            let (ip, sport, dport) = decode_key(k);
                            debug!("Deleting state for {:?} {} {}", ip, sport, dport);
                            hc.state_table.delete_state(ip, sport, dport);
                        });
                    },
                }
            }
        }
        debug!("Dumping table states");
        ::RoutingTable::dump_states();
        debug!("End of GC");
    }
}
