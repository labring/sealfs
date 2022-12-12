pub mod oldcall;

use crate::oldcall::{
    old_close, old_creat, old_fdopen, old_fdopendir, old_fopen, old_fread, old_freopen, old_fseek,
    old_ftell, old_ftruncate, old_fwrite, old_link, old_linkat, old_lseek, old_mkdir, old_mkdirat,
    old_open, old_openat, old_opendir, old_pwrite, old_read, old_readdir, old_rename, old_renameat,
    old_rewind, old_rewinddir, old_rmdir, old_seekdir, old_truncate, old_unlink, old_unlinkat,
    old_write,
};
use crossbeam_channel::{bounded, Receiver, Sender};
use lazy_static::lazy_static;
use libc::{
    c_char, c_int, c_long, c_void, mode_t, off_t, size_t, ssize_t, O_ACCMODE, O_APPEND, O_CREAT,
    O_RDONLY, O_RDWR, O_TRUNC, O_WRONLY,
};
use oldcall::{old_fclose, old_pread};
use std::collections::HashMap;
use std::env;
use std::ffi::{CStr, CString};
use std::ptr::{null, null_mut};
use std::sync::{Mutex, RwLock};

// TODO: design StreamAttr instead of FILE and DIR struct
struct StreamAttr {
    remote_flag: c_int,
    path: String,
    mode: c_int,
    // size: size_t,
    // offset: size_t,
    // next_filepos: off_t,
    // lock: Mutex<()>,
}
impl StreamAttr {
    pub fn new(path: String, mode: c_int) -> Self {
        Self {
            remote_flag: -1,
            path: path,
            mode: mode,
        }
    }
    pub fn mode_str2int(mode: *const c_char) -> c_int {
        let mode = unsafe { CStr::from_ptr(mode).to_str().unwrap() };
        if mode.starts_with("r+") {
            O_RDWR
        } else if mode.starts_with("w+") {
            O_RDWR | O_CREAT | O_TRUNC
        } else if mode.starts_with("a+") {
            O_RDWR | O_CREAT | O_APPEND
        } else if mode.starts_with("r") {
            O_RDONLY
        } else if mode.starts_with("w") {
            O_WRONLY | O_CREAT | O_TRUNC
        } else if mode.starts_with("a") {
            O_WRONLY | O_CREAT | O_APPEND
        } else {
            -1
        }
    }
}

lazy_static! {
    static ref IDLE_FD: (Sender<c_int>, Receiver<c_int>) = {
        let (s, r) = bounded(1024);
        for i in 1..1024 {
            s.send(i).unwrap();
        }
        (s, r)
    };
    static ref MOUNT_POINT: String = env::var("SEALFS_MOUNT_POINT").unwrap_or("/mnt/sealfs".to_string());
    static ref FD_TB: RwLock<HashMap<c_int, StreamAttr>> = {
        let mut v = HashMap::new();
        // mount dir
        v.insert(
            0,
            StreamAttr::new(MOUNT_POINT.clone(), 0),
        );
        RwLock::new(v)
    };
}

fn is_mounted(path: *const c_char) -> bool {
    let path = unsafe { std::fs::canonicalize(CStr::from_ptr(path).to_str().unwrap()).unwrap() };
    path.starts_with(MOUNT_POINT.clone())
}

#[no_mangle]
pub extern "C" fn open(path: *const c_char, flags: c_int, mode: mode_t) -> c_int {
    if !is_mounted(path) {
        return old_open(path, flags, mode);
    }

    println!("open intercept");
    let accmod = flags & O_ACCMODE;
    if accmod != O_RDONLY && accmod != O_WRONLY && accmod != O_RDWR {
        return -1;
    }
    match IDLE_FD.1.try_recv() {
        Ok(fd) => {
            FD_TB.write().unwrap().insert(
                fd,
                StreamAttr::new(
                    unsafe { CStr::from_ptr(path).to_str().unwrap().to_string() },
                    flags,
                ),
            );
            if (flags & O_CREAT) == O_CREAT {
                // TODO: call client
            }
            fd
        }
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn openat(
    dirfd: c_int,
    pathname: *const c_char,
    flags: c_int,
    mode: mode_t,
) -> c_int {
    if !FD_TB.read().unwrap().contains_key(&dirfd) {
        return old_openat(dirfd, pathname, flags, mode);
    }
    println!("openat intercept");
    let accmod = flags & O_ACCMODE;
    if accmod != O_RDONLY && accmod != O_WRONLY && accmod != O_RDWR {
        return -1;
    }
    match IDLE_FD.1.try_recv() {
        Ok(fd) => {
            FD_TB.write().unwrap().insert(
                fd,
                StreamAttr::new(
                    unsafe { CStr::from_ptr(pathname).to_str().unwrap().to_string() },
                    flags,
                ),
            );
            if (flags & O_CREAT) == O_CREAT {
                // TODO: call client
            }
            fd
        }
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn fopen(pathname: *const c_char, mode: *const c_char) -> *mut c_void {
    if !is_mounted(pathname) {
        return old_fopen(pathname, mode);
    }
    println!("fopen intercept");
    // TODO
    null_mut()
}

pub extern "C" fn fdopen(fd: c_int, mode: *const c_char) -> *mut c_void {
    if !FD_TB.read().unwrap().contains_key(&fd) {
        return old_fdopen(fd, mode);
    }
    println!("fdopen intercept");
    // TODO
    null_mut()
}

pub extern "C" fn freopen(
    pathname: *const c_char,
    mode: *const c_char,
    stream: *mut c_void,
) -> *mut c_void {
    if !is_mounted(pathname) {
        return old_freopen(pathname, mode, stream);
    }
    println!("freopen intercept");
    // TODO
    null_mut()
}

#[no_mangle]
pub extern "C" fn opendir(pathname: *const c_char) -> *mut c_void {
    if !is_mounted(pathname) {
        return old_opendir(pathname);
    }
    // TODO: call client
    println!("opendir intercept");
    null_mut()
}

#[no_mangle]
pub extern "C" fn fdopendir(fd: c_int) -> *mut c_void {
    if !FD_TB.read().unwrap().contains_key(&fd) {
        return old_fdopendir(fd);
    }

    println!("fdopendir intercept");
    // TODO: call client
    null_mut()
}

#[no_mangle]
pub extern "C" fn creat(pathname: *const c_char, mode: mode_t) -> c_int {
    if !is_mounted(pathname) {
        return old_creat(pathname, mode);
    }
    println!("creat intercept");
    // tips: (O_CREAT|O_WRONLY|O_TRUNC)
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn close(fd: c_int) -> c_int {
    if !FD_TB.read().unwrap().contains_key(&fd) {
        return old_close(fd);
    }

    match FD_TB.write().unwrap().remove(&fd) {
        Some(_) => {
            IDLE_FD.0.send(fd).unwrap();
            0
        }
        None => -1,
    }
}

#[no_mangle]
pub extern "C" fn fclose(stream: *mut c_void) -> c_int {
    if stream == null_mut() {
        return 0;
    }
    let flag = unsafe { (*(stream as *mut StreamAttr)).remote_flag };
    if flag == -1 {
        return old_fclose(stream);
    }
    // TODO: flush and free
    0
}

#[no_mangle]
pub extern "C" fn rename(oldpath: *const c_char, newpath: *const c_char) -> c_int {
    if !is_mounted(oldpath) && !is_mounted(newpath) {
        return old_rename(oldpath, newpath);
    } else if is_mounted(oldpath) && is_mounted(newpath) {
        // TODO: call client
        return 0;
    } else {
        // TODO: move between local and remote
    }
    println!("rename intercept");
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn renameat(
    olddirfd: c_int,
    oldpath: *const c_char,
    newdirfd: c_int,
    newpath: *const c_char,
) -> c_int {
    if !FD_TB.read().unwrap().contains_key(&olddirfd)
        || !FD_TB.read().unwrap().contains_key(&newdirfd)
    {
        return old_renameat(olddirfd, oldpath, newdirfd, newpath);
    }

    println!("renameat intercept");
    let oldpath = match FD_TB.read().unwrap().get(&olddirfd) {
        Some(attr) => {
            let path = attr.path.clone() + unsafe { CStr::from_ptr(oldpath).to_str().unwrap() };
            path
        }
        None => return -1,
    };
    let newpath = match FD_TB.read().unwrap().get(&newdirfd) {
        Some(attr) => {
            let path = attr.path.clone() + unsafe { CStr::from_ptr(newpath).to_str().unwrap() };
            path
        }
        None => return -1,
    };
    rename(
        CString::new(oldpath).unwrap().as_ptr(),
        CString::new(newpath).unwrap().as_ptr(),
    )
    // TODO: call client
}

#[no_mangle]
pub extern "C" fn truncate(path: *const c_char, length: off_t) -> c_int {
    if !is_mounted(path) {
        return old_truncate(path, length);
    }
    println!("truncate intercept");
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn ftruncate(fd: c_int, length: off_t) -> c_int {
    if !FD_TB.read().unwrap().contains_key(&fd) {
        return old_ftruncate(fd, length);
    }
    println!("truncate intercept");
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn mkdir(path: *const c_char, mode: mode_t) -> c_int {
    if !is_mounted(path) {
        return old_mkdir(path, mode);
    }
    println!("mkdir intercept");
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn mkdirat(dirfd: c_int, path: *const c_char, mode: mode_t) -> c_int {
    if !FD_TB.write().unwrap().contains_key(&dirfd) {
        return old_mkdirat(dirfd, path, mode);
    }
    println!("mkdirat intercept");
    let pathname = match FD_TB.read().unwrap().get(&dirfd) {
        Some(attr) => {
            let path = attr.path.clone() + unsafe { CStr::from_ptr(path).to_str().unwrap() };
            path
        }
        None => return -1,
    };
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn rmdir(path: *const c_char) -> c_int {
    if !is_mounted(path) {
        return old_rmdir(path);
    }
    println!("rmdir intercept");
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn link(oldpath: *const c_char, newpath: *const c_char) -> c_int {
    if !is_mounted(oldpath) && !is_mounted(newpath) {
        return old_link(oldpath, newpath);
    }
    println!("link intercept");
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn linkat(
    olddirfd: c_int,
    oldpath: *const c_char,
    newdirfd: c_int,
    newpath: *const c_char,
    flags: c_int,
) -> c_int {
    if !FD_TB.read().unwrap().contains_key(&olddirfd)
        && !FD_TB.read().unwrap().contains_key(&newdirfd)
    {
        return old_linkat(olddirfd, oldpath, newdirfd, newpath, flags);
    }
    println!("link intercept");
    let oldpath = match FD_TB.read().unwrap().get(&olddirfd) {
        Some(attr) => {
            let path = attr.path.clone() + unsafe { CStr::from_ptr(oldpath).to_str().unwrap() };
            Some(path)
        }
        None => None,
    };
    let newpath = match FD_TB.read().unwrap().get(&newdirfd) {
        Some(attr) => {
            let path = attr.path.clone() + unsafe { CStr::from_ptr(newpath).to_str().unwrap() };
            Some(path)
        }
        None => None,
    };

    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn unlink(path: *const c_char) -> c_int {
    if !is_mounted(path) {
        return old_unlink(path);
    }
    println!("unlink intercept");
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn unlinkat(dirfd: c_int, path: *const c_char, flags: c_int) -> c_int {
    if !FD_TB.read().unwrap().contains_key(&dirfd) {
        return old_unlinkat(dirfd, path, flags);
    }

    println!("unlink intercept");
    let path = match FD_TB.read().unwrap().get(&dirfd) {
        Some(attr) => {
            let path = attr.path.clone() + unsafe { CStr::from_ptr(path).to_str().unwrap() };
            Some(path)
        }
        None => None,
    };
    // TODO: call client
    0
}

#[no_mangle]
pub extern "C" fn read(fd: c_int, buf: *mut c_void, count: size_t) -> ssize_t {
    if !FD_TB.read().unwrap().contains_key(&fd) {
        return old_read(fd, buf, count);
    }

    println!("read intercept");
    match FD_TB.read().unwrap().get(&fd) {
        Some(attr) => {
            // TODO: call client and modify Attr
        }
        None => {
            return -1;
        }
    }

    count.try_into().unwrap()
}

#[no_mangle]
pub extern "C" fn pread(fd: c_int, buf: *mut c_void, count: size_t, offset: off_t) -> ssize_t {
    if !FD_TB.read().unwrap().contains_key(&fd) {
        return old_pread(fd, buf, count, offset);
    }

    println!("pread intercept");
    match FD_TB.read().unwrap().get(&fd) {
        Some(attr) => {
            // TODO: call client and modify Attr
        }
        None => {
            return -1;
        }
    }

    count.try_into().unwrap()
}

#[no_mangle]
pub extern "C" fn fread(
    buf: *mut c_void,
    size: size_t,
    count: size_t,
    stream: *mut c_void,
) -> size_t {
    if stream == null_mut() {
        return 0;
    }
    let flag = unsafe { (*(stream as *mut StreamAttr)).remote_flag };
    if flag == -1 {
        return old_fread(buf, size, count, stream);
    }
    println!("fread intercept");
    // TODO: call client and modify stream
    0
}

#[no_mangle]
pub extern "C" fn readdir(dirp: *mut c_void) -> *mut c_void {
    if dirp == null_mut() {
        return null_mut();
    }
    let flag = unsafe { (*(dirp as *mut StreamAttr)).remote_flag };
    if flag == -1 {
        return old_readdir(dirp);
    }

    println!("readdir intercept");
    // TODO: call client and modify Attr
    null_mut()
}

#[no_mangle]
pub extern "C" fn write(fd: c_int, buf: *const c_void, count: size_t) -> ssize_t {
    if !FD_TB.read().unwrap().contains_key(&fd) {
        return old_write(fd, buf, count);
    }
    println!("write intercept");
    match FD_TB.write().unwrap().get_mut(&fd) {
        Some(attr) => {
            // TODO: call client and modify Attr
        }
        None => {
            return -1;
        }
    }
    count.try_into().unwrap()
}

#[no_mangle]
pub extern "C" fn pwrite(fd: c_int, buf: *const c_void, count: size_t, offset: off_t) -> ssize_t {
    if !FD_TB.read().unwrap().contains_key(&fd) {
        return old_pwrite(fd, buf, count, offset);
    }
    println!("pwrite intercept");
    match FD_TB.write().unwrap().get_mut(&fd) {
        Some(attr) => {
            // TODO: call client and modify Attr
        }
        None => {
            return -1;
        }
    }
    count.try_into().unwrap()
}

#[no_mangle]
pub extern "C" fn fwrite(
    buf: *const c_void,
    size: size_t,
    count: size_t,
    stream: *mut c_void,
) -> size_t {
    if stream == null_mut() {
        return 0;
    }
    let flag = unsafe { (*(stream as *mut StreamAttr)).remote_flag };
    if flag == -1 {
        return old_fwrite(buf, size, count, stream);
    }
    println!("fwrite intercept");
    // TODO: call client and modify Attr
    0
}

#[no_mangle]
pub extern "C" fn lseek(fd: c_int, offset: c_long, whence: c_int) -> off_t {
    if !FD_TB.read().unwrap().contains_key(&fd) {
        return old_lseek(fd, offset, whence);
    }
    println!("lseek intercept");
    // TODO: modify Attr
    0
}

#[no_mangle]
pub extern "C" fn fseek(stream: *mut c_void, offset: c_long, whence: c_int) -> c_int {
    if stream == null_mut() {
        return 0;
    }
    let flag = unsafe { (*(stream as *mut StreamAttr)).remote_flag };
    if flag == -1 {
        return old_fseek(stream, offset, whence);
    }
    println!("fseek intercept");
    // TODO: modify Attr
    0
}

#[no_mangle]
pub extern "C" fn seekdir(dirp: *mut c_void, loc: c_long) {
    if dirp == null_mut() {
        return;
    }
    let flag = unsafe { (*(dirp as *mut StreamAttr)).remote_flag };
    if flag == -1 {
        return old_seekdir(dirp, loc);
    }
    println!("seekdir intercept");
    // TODO: modify Attr
}

#[no_mangle]
pub extern "C" fn ftell(stream: *mut c_void) -> c_long {
    if stream == null_mut() {
        return 0;
    }
    let flag = unsafe { (*(stream as *mut StreamAttr)).remote_flag };
    if flag == -1 {
        return old_ftell(stream);
    }
    println!("ftell intercept");
    // TODO: return current_pos
    0
}

#[no_mangle]
pub extern "C" fn rewind(stream: *mut c_void) {
    if stream == null_mut() {
        return;
    }
    let flag = unsafe { (*(stream as *mut StreamAttr)).remote_flag };
    if flag == -1 {
        return old_rewind(stream);
    }
    println!("rewind intercept");
    // TODO: modify Attr
}

#[no_mangle]
pub extern "C" fn rewinddir(dirp: *mut c_void) {
    if dirp == null_mut() {
        return;
    }
    let flag = unsafe { (*(dirp as *mut StreamAttr)).remote_flag };
    if flag == -1 {
        return old_rewinddir(dirp);
    }
    println!("rewinddir intercept");
    // TODO: modify Attr
}
