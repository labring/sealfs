use crossbeam_channel::{bounded, Receiver, Sender};
use dashmap::DashMap;

#[derive(PartialEq, Debug, Clone)]
pub enum FdType {
    File,
    Dir,
}

#[derive(Clone)]
pub struct FdAttr {
    pub pathname: String,
    pub r#type: FdType,
    pub offset: i64,
    pub flags: i32,
}

lazy_static::lazy_static! {
    static ref IDLE_FD: (Sender<i32>, Receiver<i32>) = {
        let (s, r) = bounded(1024);
        for i in 10000..11024 {
            s.send(i).unwrap();
        }
        (s, r)
    };
    static ref FD_TB: DashMap<i32, FdAttr> = DashMap::new();
}

pub fn insert_attr(attr: FdAttr) -> Option<i32> {
    let fd = match IDLE_FD.1.recv() {
        Ok(value) => value,
        Err(_) => return None,
    };

    FD_TB.insert(fd, attr);
    return Some(fd);
}

pub fn remove_attr(fd: i32) -> bool {
    match FD_TB.remove(&fd) {
        Some(_) => {
            IDLE_FD.0.send(fd).unwrap();
            true
        }
        None => false,
    }
}

pub fn get_attr(fd: i32) -> Option<FdAttr> {
    match FD_TB.get(&fd) {
        Some(value) => Some((*value).clone()),
        None => None,
    }
}

pub fn set_attr(fd: i32, attr: FdAttr) -> bool {
    match FD_TB.get_mut(&fd) {
        Some(mut value) => {
            *value = attr;
            true
        }
        None => false,
    }
}

pub fn set_offset(fd: i32, offset: i64) {
    FD_TB.get_mut(&fd).unwrap().offset = offset as i64
}
