use lazy_static::lazy_static;
use libc::{c_char, c_int, c_long, c_void, dlsym, mode_t, off_t, size_t, ssize_t, RTLD_NEXT};
use std::any::Any;
use std::ffi::CString;
use std::mem::transmute;

struct CVoidPtr {
    ptr: *mut c_void,
}

unsafe impl Send for CVoidPtr {}
unsafe impl Sync for CVoidPtr {}

fn get_old_func(funcname: &str) -> CVoidPtr {
    CVoidPtr {
        ptr: unsafe { dlsym(RTLD_NEXT, CString::new(funcname).unwrap().as_ptr()) },
    }
}

// annotated interfaces may not be implemented on Linux.
lazy_static! {
    static ref OLD_OPEN: CVoidPtr = get_old_func("open");
    static ref OLD_OPENAT: CVoidPtr = get_old_func("openat");
    static ref OLD_FOPEN: CVoidPtr = get_old_func("fopen");
    static ref OLD_FDOPEN: CVoidPtr = get_old_func("fdopen");
    static ref OLD_FREOPEN: CVoidPtr = get_old_func("freopen");
    static ref OLD_OPENDIR: CVoidPtr = get_old_func("opendir");
    static ref OLD_FDOPENDIR: CVoidPtr = get_old_func("fdopendir");
    static ref OLD_CREAT: CVoidPtr = get_old_func("creat");
    static ref OLD_CLOSE: CVoidPtr = get_old_func("close");
    static ref OLD_FCLOSE: CVoidPtr = get_old_func("fclose");
    static ref OLD_RENAME: CVoidPtr = get_old_func("rename");
    static ref OLD_RENAMEAT: CVoidPtr = get_old_func("renameat");
    static ref OLD_TRUNCATE: CVoidPtr = get_old_func("truncate");
    static ref OLD_FTRUNCATE: CVoidPtr = get_old_func("ftruncate");
    static ref OLD_MKDIR: CVoidPtr = get_old_func("mkdir");
    static ref OLD_MKDIRAT: CVoidPtr = get_old_func("mkdirat");
    static ref OLD_RMDIR: CVoidPtr = get_old_func("rmdir");
    static ref OLD_LINK: CVoidPtr = get_old_func("link");
    static ref OLD_LINKAT: CVoidPtr = get_old_func("linkat");
    static ref OLD_UNLINK: CVoidPtr = get_old_func("unlink");
    static ref OLD_UNLINKAT: CVoidPtr = get_old_func("unlinkat");
    static ref OLD_READ: CVoidPtr = get_old_func("read");
    static ref OLD_PREAD: CVoidPtr = get_old_func("pread");
    static ref OLD_FREAD: CVoidPtr = get_old_func("fread");
    static ref OLD_READDIR: CVoidPtr = get_old_func("readdir");
    static ref OLD_WRITE: CVoidPtr = get_old_func("write");
    static ref OLD_PWRITE: CVoidPtr = get_old_func("pwrite");
    static ref OLD_FWRITE: CVoidPtr = get_old_func("fwrite");
    static ref OLD_LSEEK: CVoidPtr = get_old_func("lseek");
    static ref OLD_FSEEK: CVoidPtr = get_old_func("fseek");
    static ref OLD_SEEKDIR: CVoidPtr = get_old_func("seekdir");
    static ref OLD_FTELL: CVoidPtr = get_old_func("ftell");
    static ref OLD_REWIND: CVoidPtr = get_old_func("rewind");
    static ref OLD_REWINDDIR: CVoidPtr = get_old_func("rewinddir");
}

pub fn old_open(pathname: *const c_char, flags: c_int, mode: mode_t) -> c_int {
    let old_open = unsafe {
        transmute::<*mut c_void, extern "C" fn(*const c_char, c_int, mode_t) -> c_int>(OLD_OPEN.ptr)
    };
    old_open(pathname, flags, mode)
}

pub fn old_openat(dirfd: c_int, pathname: *const c_char, flags: c_int, mode: mode_t) -> c_int {
    let old_openat = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, *const c_char, c_int, mode_t) -> c_int>(
            OLD_OPENAT.ptr,
        )
    };
    old_openat(dirfd, pathname, flags, mode)
}

pub fn old_fopen(pathname: *const c_char, mode: *const c_char) -> *mut c_void {
    let old_fopen = unsafe {
        transmute::<*mut c_void, extern "C" fn(*const c_char, *const c_char) -> *mut c_void>(
            OLD_FOPEN.ptr,
        )
    };
    old_fopen(pathname, mode)
}

pub fn old_fdopen(fd: c_int, mode: *const c_char) -> *mut c_void {
    let old_fdopen = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, *const c_char) -> *mut c_void>(OLD_FDOPEN.ptr)
    };
    old_fdopen(fd, mode)
}

pub fn old_freopen(
    pathname: *const c_char,
    mode: *const c_char,
    stream: *mut c_void,
) -> *mut c_void {
    let old_freopen = unsafe {
        transmute::<
            *mut c_void,
            extern "C" fn(*const c_char, *const c_char, *mut c_void) -> *mut c_void,
        >(OLD_FREOPEN.ptr)
    };
    old_freopen(pathname, mode, stream)
}

pub fn old_opendir(pathname: *const c_char) -> *mut c_void {
    let old_opendir = unsafe {
        transmute::<*mut c_void, extern "C" fn(*const c_char) -> *mut c_void>(OLD_OPENDIR.ptr)
    };
    old_opendir(pathname)
}

pub fn old_fdopendir(fd: c_int) -> *mut c_void {
    let old_old_fdopendir =
        unsafe { transmute::<*mut c_void, extern "C" fn(c_int) -> *mut c_void>(OLD_FDOPENDIR.ptr) };
    old_old_fdopendir(fd)
}

pub fn old_creat(pathname: *const c_char, mode: mode_t) -> c_int {
    let old_creat = unsafe {
        transmute::<*mut c_void, extern "C" fn(*const c_char, mode_t) -> c_int>(OLD_OPEN.ptr)
    };
    old_creat(pathname, mode)
}

pub fn old_close(fd: c_int) -> c_int {
    let old_close =
        unsafe { transmute::<*mut c_void, extern "C" fn(c_int) -> c_int>(OLD_CLOSE.ptr) };
    old_close(fd)
}

pub fn old_fclose(stream: *mut c_void) -> c_int {
    let old_fclose =
        unsafe { transmute::<*mut c_void, extern "C" fn(*mut c_void) -> c_int>(OLD_FCLOSE.ptr) };
    old_fclose(stream)
}

pub fn old_rename(oldpath: *const c_char, newpath: *const c_char) -> c_int {
    let old_rename = unsafe {
        transmute::<*mut c_void, extern "C" fn(*const c_char, *const c_char) -> c_int>(
            OLD_RENAME.ptr,
        )
    };
    old_rename(oldpath, newpath)
}

pub fn old_renameat(
    olddirfd: c_int,
    oldpath: *const c_char,
    newdirfd: c_int,
    newpath: *const c_char,
) -> c_int {
    let old_renameat = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, *const c_char, c_int, *const c_char) -> c_int>(
            OLD_RENAMEAT.ptr,
        )
    };
    old_renameat(olddirfd, oldpath, newdirfd, newpath)
}

pub fn old_truncate(path: *const c_char, length: off_t) -> c_int {
    let old_truncate = unsafe {
        transmute::<*mut c_void, extern "C" fn(*const c_char, c_long) -> c_int>(OLD_TRUNCATE.ptr)
    };
    old_truncate(path, length)
}

pub fn old_ftruncate(fd: c_int, length: off_t) -> c_int {
    let old_ftruncate = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, c_long) -> c_int>(OLD_FTRUNCATE.ptr)
    };
    old_ftruncate(fd, length)
}

pub fn old_mkdir(pathname: *const c_char, mode: mode_t) -> c_int {
    let old_mkdir = unsafe {
        transmute::<*mut c_void, extern "C" fn(*const c_char, mode_t) -> c_int>(OLD_MKDIR.ptr)
    };
    old_mkdir(pathname, mode)
}

pub fn old_mkdirat(dirfd: c_int, pathname: *const c_char, mode: mode_t) -> c_int {
    let old_mkdirat = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, *const c_char, mode_t) -> c_int>(
            OLD_MKDIRAT.ptr,
        )
    };
    old_mkdirat(dirfd, pathname, mode)
}

pub fn old_rmdir(pathname: *const c_char) -> c_int {
    let old_rmdir =
        unsafe { transmute::<*mut c_void, extern "C" fn(*const c_char) -> c_int>(OLD_RMDIR.ptr) };
    old_rmdir(pathname)
}

pub fn old_link(oldpath: *const c_char, newpath: *const c_char) -> c_int {
    let old_link = unsafe {
        transmute::<*mut c_void, extern "C" fn(*const c_char, *const c_char) -> c_int>(OLD_LINK.ptr)
    };
    old_link(oldpath, newpath)
}

pub fn old_linkat(
    olddirfd: c_int,
    oldpath: *const c_char,
    newdirfd: c_int,
    newpath: *const c_char,
    flags: c_int,
) -> c_int {
    let old_linkat = unsafe {
        transmute::<
            *mut c_void,
            extern "C" fn(c_int, *const c_char, c_int, *const c_char, c_int) -> c_int,
        >(OLD_LINKAT.ptr)
    };
    old_linkat(olddirfd, oldpath, newdirfd, newpath, flags)
}

pub fn old_unlink(pathname: *const c_char) -> c_int {
    let old_unlink =
        unsafe { transmute::<*mut c_void, extern "C" fn(*const c_char) -> c_int>(OLD_UNLINK.ptr) };
    old_unlink(pathname)
}

pub fn old_unlinkat(dirfd: c_int, pathname: *const c_char, flags: c_int) -> c_int {
    let old_unlinkat = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, *const c_char, c_int) -> c_int>(
            OLD_UNLINKAT.ptr,
        )
    };
    old_unlinkat(dirfd, pathname, flags)
}

pub fn old_read(fd: c_int, buf: *mut c_void, count: size_t) -> ssize_t {
    let old_read = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, *mut c_void, size_t) -> ssize_t>(OLD_READ.ptr)
    };
    old_read(fd, buf, count)
}

pub fn old_pread(fd: c_int, buf: *mut c_void, count: size_t, offset: off_t) -> ssize_t {
    let old_pread = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, *mut c_void, size_t, off_t) -> ssize_t>(
            OLD_PREAD.ptr,
        )
    };
    old_pread(fd, buf, count, offset)
}

pub fn old_fread(buf: *mut c_void, size: size_t, count: size_t, stream: *mut c_void) -> size_t {
    let old_fread = unsafe {
        transmute::<*mut c_void, extern "C" fn(*mut c_void, size_t, size_t, *mut c_void) -> size_t>(
            OLD_FREAD.ptr,
        )
    };
    old_fread(buf, size, count, stream)
}

pub fn old_readdir(dirp: *mut c_void) -> *mut c_void {
    let old_readdir = unsafe {
        transmute::<*mut c_void, extern "C" fn(*mut c_void) -> *mut c_void>(OLD_READDIR.ptr)
    };
    old_readdir(dirp)
}

pub fn old_write(fd: c_int, buf: *const c_void, count: size_t) -> ssize_t {
    let old_write = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, *const c_void, size_t) -> ssize_t>(
            OLD_WRITE.ptr,
        )
    };
    old_write(fd, buf, count)
}

pub fn old_pwrite(fd: c_int, buf: *const c_void, count: size_t, offset: off_t) -> ssize_t {
    let old_pwrite = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, *const c_void, size_t, off_t) -> ssize_t>(
            OLD_PWRITE.ptr,
        )
    };
    old_pwrite(fd, buf, count, offset)
}

pub fn old_fwrite(buf: *const c_void, size: size_t, count: size_t, stream: *mut c_void) -> size_t {
    let old_fwrite = unsafe {
        transmute::<
            *mut c_void,
            extern "C" fn(*const c_void, size_t, size_t, stream: *mut c_void) -> size_t,
        >(OLD_FWRITE.ptr)
    };
    old_fwrite(buf, size, count, stream)
}

pub fn old_lseek(fd: c_int, offset: c_long, whence: c_int) -> off_t {
    let old_lseek = unsafe {
        transmute::<*mut c_void, extern "C" fn(c_int, c_long, c_int) -> off_t>(OLD_LSEEK.ptr)
    };
    old_lseek(fd, offset, whence)
}

pub fn old_fseek(stream: *mut c_void, offset: c_long, whence: c_int) -> c_int {
    let old_fseek = unsafe {
        transmute::<*mut c_void, extern "C" fn(*mut c_void, c_long, c_int) -> c_int>(OLD_FSEEK.ptr)
    };
    old_fseek(stream, offset, whence)
}

pub fn old_seekdir(dirp: *mut c_void, loc: c_long) {
    let old_seekdir =
        unsafe { transmute::<*mut c_void, extern "C" fn(*mut c_void, c_long)>(OLD_SEEKDIR.ptr) };
    old_seekdir(dirp, loc)
}

pub fn old_ftell(stream: *mut c_void) -> c_long {
    let old_ftell =
        unsafe { transmute::<*mut c_void, extern "C" fn(*mut c_void) -> c_long>(OLD_FTELL.ptr) };
    old_ftell(stream)
}

pub fn old_rewind(stream: *mut c_void) {
    let old_rewind =
        unsafe { transmute::<*mut c_void, extern "C" fn(*mut c_void)>(OLD_REWIND.ptr) };
    old_rewind(stream)
}

pub fn old_rewinddir(dirp: *mut c_void) {
    let old_rewinddir =
        unsafe { transmute::<*mut c_void, extern "C" fn(*mut c_void)>(OLD_REWINDDIR.ptr) };
    old_rewinddir(dirp)
}
