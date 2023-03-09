pub mod client;
pub mod file_desc;
pub mod path;
pub mod syscall_intercept;
pub mod test_log;

use client::CLIENT;
use file_desc::{FdAttr, FdType};
use lazy_static::lazy_static;
use libc::{
    c_char, iovec, stat, statx, SYS_close, SYS_creat, SYS_fstat, SYS_fsync, SYS_ftruncate,
    SYS_getdents, SYS_getdents64, SYS_lseek, SYS_lstat, SYS_mkdir, SYS_mkdirat, SYS_open,
    SYS_openat, SYS_pread64, SYS_preadv, SYS_pwrite64, SYS_pwritev, SYS_read, SYS_readlink,
    SYS_readv, SYS_rename, SYS_renameat, SYS_rmdir, SYS_stat, SYS_statx, SYS_truncate, SYS_unlink,
    SYS_write, SYS_writev, AT_FDCWD, O_CREAT, O_DIRECTORY, O_TRUNC, O_WRONLY, SEEK_CUR, SEEK_END,
    SEEK_SET, S_IFLNK,
};
use log::debug;
use path::{get_absolutepath, get_remotepath, CURRENT_DIR, MOUNT_POINT};
use sealfs::common::distribute_hash_table::build_hash_ring;
use serde::{Deserialize, Serialize};
use std::env;
use std::ffi::CStr;
use std::io::Read;
use syscall_intercept::*;

const STAT_SIZE: usize = std::mem::size_of::<stat>();
const STATX_SIZE: usize = std::mem::size_of::<statx>();

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    manager_address: String,
    all_servers_address: Vec<String>,
    heartbeat: bool,
    log_level: String,
}

pub async fn init_client_wrap(all_servers_address: Vec<String>) {
    for server_address in all_servers_address {
        CLIENT.add_connection(&server_address).await;
    }
}

extern "C" fn initialize() {
    unsafe {
        // let mut builder = env_logger::Builder::from_default_env();
        // builder
        //     .format_timestamp(None)
        //     .filter(None, log::LevelFilter::Debug);
        // builder.init();
        // debug!("intercept init!");
        set_hook_fn(dispatch);
        let config_path =
            env::var("SEALFS_CONFIG_PATH").unwrap_or_else(|_| "/home/client.yaml".to_string());
        let mut config_file = std::fs::File::open(config_path).unwrap();
        let mut config_str = String::new();
        config_file.read_to_string(&mut config_str).unwrap();
        let config: Config = serde_yaml::from_str(&config_str).expect("client.yaml read failed!");
        build_hash_ring(config.all_servers_address.clone());
        RUNTIME.block_on(init_client_wrap(config.all_servers_address));
    }
}

/* There is no __attribute__((constructor)) in rust,
 * it is implemented through .init_array */
#[link_section = ".init_array"]
pub static INITIALIZE_CTOR: extern "C" fn() = self::initialize;

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
}

#[allow(non_upper_case_globals)]
extern "C" fn dispatch(
    syscall_number: isize,
    arg0: isize,
    arg1: isize,
    arg2: isize,
    arg3: isize,
    arg4: isize,
    _arg5: isize,
    result: &mut isize,
) -> InterceptResult {
    match syscall_number as i64 {
        // int close(int fd)
        SYS_close => {
            if file_desc::remove_attr(arg0 as i32) {
                *result = 0;
                InterceptResult::Hook
            } else {
                InterceptResult::Forward
            }
        }
        // int creat(const char *pathname, mode_t mode)
        SYS_creat => {
            let dir_path = &CURRENT_DIR;
            let file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };

            match CLIENT.open_remote(&remote_pathname, O_CREAT | O_WRONLY | O_TRUNC, arg1 as u32) {
                Ok(()) => {
                    let fd = match file_desc::insert_attr(FdAttr {
                        pathname: remote_pathname,
                        r#type: FdType::File,
                        offset: 0,
                        flags: arg1 as i32,
                    }) {
                        Some(value) => value,
                        None => {
                            *result = -libc::EMFILE as isize;
                            return InterceptResult::Hook;
                        }
                    };
                    *result = fd as isize;
                }
                Err(e) => *result = -e as isize,
            }

            InterceptResult::Hook
        }
        // int open(const char *pathname, int flags, mode_t mode)
        SYS_open => {
            let dir_path = &CURRENT_DIR;
            let file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            match CLIENT.open_remote(&remote_pathname, arg1 as i32, arg2 as u32) {
                Ok(()) => {
                    let filetype = match (arg1 as i32) & O_DIRECTORY {
                        0 => FdType::File,
                        _ => FdType::Dir,
                    };

                    *result = match file_desc::insert_attr(FdAttr {
                        pathname: remote_pathname,
                        r#type: filetype,
                        offset: 0,
                        flags: arg1 as i32,
                    }) {
                        Some(value) => value as isize,
                        None => -libc::EMFILE as isize,
                    };
                }
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // int openat(int dirfd, const char *pathname, int flags, mode_t mode)
        SYS_openat => {
            let file_path = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let dir_path = if arg0 as i32 == AT_FDCWD {
                CURRENT_DIR.to_string()
            } else {
                match file_desc::get_attr(arg0 as i32) {
                    Some(value) => MOUNT_POINT.to_string() + &value.pathname,
                    None => {
                        if !file_path.starts_with('/') {
                            return InterceptResult::Forward;
                        } else {
                            "".to_string()
                        }
                    }
                }
            };

            let absolute_pathname = match get_absolutepath(&dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };

            match CLIENT.open_remote(&remote_pathname, arg2 as i32, arg3 as u32) {
                Ok(()) => {
                    let filetype = match (arg2 as i32) & O_DIRECTORY {
                        0 => FdType::File,
                        _ => FdType::Dir,
                    };

                    *result = match file_desc::insert_attr(FdAttr {
                        pathname: remote_pathname,
                        r#type: filetype,
                        offset: 0,
                        flags: arg1 as i32,
                    }) {
                        Some(value) => value as isize,
                        None => -libc::EMFILE as isize,
                    };
                }
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // int rename(const char *oldpath, const char *newpath)
        SYS_rename => {
            // todo other state
            let dir_path = &CURRENT_DIR;
            let old_file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let new_file_path = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let absolute_oldpath = match get_absolutepath(dir_path, old_file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let absolute_newpath = match get_absolutepath(dir_path, new_file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_oldpath = match get_remotepath(&absolute_oldpath) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            let remote_newpath = match get_remotepath(&absolute_newpath) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };

            *result = CLIENT.rename_remote(&remote_oldpath, &remote_newpath) as isize;
            InterceptResult::Hook
        }
        // int renameat(int olddirfd, const char *oldpath,
        //             int newdirfd, const char *newpath)
        SYS_renameat => {
            // todo other state
            let old_file_path = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let new_file_path = unsafe { CStr::from_ptr(arg3 as *const c_char).to_str().unwrap() };

            let old_dir_path = if arg0 as i32 == AT_FDCWD {
                CURRENT_DIR.to_string()
            } else {
                match file_desc::get_attr(arg0 as i32) {
                    Some(value) => MOUNT_POINT.to_string() + &value.pathname,
                    None => {
                        if !old_file_path.starts_with('/') {
                            return InterceptResult::Forward;
                        } else {
                            "".to_string()
                        }
                    }
                }
            };
            let new_dir_path = if arg2 as i32 == AT_FDCWD {
                CURRENT_DIR.to_string()
            } else {
                match file_desc::get_attr(arg0 as i32) {
                    Some(value) => MOUNT_POINT.to_string() + &value.pathname,
                    None => {
                        if !new_file_path.starts_with('/') {
                            return InterceptResult::Forward;
                        } else {
                            "".to_string()
                        }
                    }
                }
            };

            let absolute_oldpath = match get_absolutepath(&old_dir_path, old_file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let absolute_newpath = match get_absolutepath(&new_dir_path, new_file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };

            let remote_oldpath = match get_remotepath(&absolute_oldpath) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            let remote_newpath = match get_remotepath(&absolute_newpath) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };

            *result = CLIENT.rename_remote(&remote_oldpath, &remote_newpath) as isize;
            InterceptResult::Hook
        }
        // int truncate(const char *path, off_t length)
        SYS_truncate => {
            let dir_path = &CURRENT_DIR;
            let file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };

            match CLIENT.truncate_remote(&remote_pathname, arg1 as i64) {
                Ok(()) => *result = 0,
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // int ftruncate(int fd, off_t length)
        SYS_ftruncate => {
            let remote_pathname = match file_desc::get_attr(arg0 as i32) {
                Some(value) => MOUNT_POINT.to_string() + &value.pathname,
                None => return InterceptResult::Forward,
            };
            match CLIENT.truncate_remote(&remote_pathname, arg1 as i64) {
                Ok(()) => *result = 0,
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // int mkdir(const char *pathname, mode_t mode)
        SYS_mkdir => {
            let dir_path = &CURRENT_DIR;
            let file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            match CLIENT.mkdir_remote(&remote_pathname, arg1 as u32) {
                Ok(()) => *result = 0,
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // int mkdirat(int dirfd, const char *pathname, mode_t mode)
        SYS_mkdirat => {
            let dir_path = match file_desc::get_attr(arg0 as i32) {
                Some(value) => MOUNT_POINT.to_string() + &value.pathname,
                None => return InterceptResult::Forward,
            };
            let file_path = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(&dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };

            match CLIENT.mkdir_remote(&remote_pathname, arg2 as u32) {
                Ok(()) => *result = 0,
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // int rmdir(const char *pathname)
        SYS_rmdir => {
            let dir_path = &CURRENT_DIR;
            let file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            match CLIENT.rmdir_remote(&remote_pathname) {
                Ok(()) => *result = 0,
                Err(e) => {
                    if e == 39 {
                        *result = 0;
                    } else {
                        *result = -e as isize;
                    }
                }
            }

            InterceptResult::Hook
        }
        // ssize_t getdents(int fd, void *dirp, size_t count);
        SYS_getdents => {
            let (remote_pathname, offset) = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::Dir {
                            *result = -libc::ENOTDIR as isize;
                            return InterceptResult::Hook;
                        }
                        (attr.pathname.clone(), attr.offset)
                    }
                    _ => return InterceptResult::Forward,
                }
            };
            let dirp = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, arg2 as usize) };

            match CLIENT.getdents_remote(&remote_pathname, dirp, offset) {
                Ok(value) => {
                    if value.0 == 0 {
                        unsafe {
                            *(arg1 as *mut u8) = 0;
                        }
                    }
                    *result = value.0;
                    file_desc::set_offset(arg0 as i32, offset + value.1);
                }
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // ssize_t getdents64(int fd, void *dirp, size_t count);
        SYS_getdents64 => {
            let (remote_pathname, offset) = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::Dir {
                            *result = -libc::ENOTDIR as isize;
                            return InterceptResult::Hook;
                        }
                        (attr.pathname.clone(), attr.offset)
                    }
                    _ => return InterceptResult::Forward,
                }
            };
            let dirp = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, arg2 as usize) };

            match CLIENT.getdents64_remote(&remote_pathname, dirp, offset) {
                Ok(value) => {
                    if value.0 == 0 {
                        unsafe {
                            *(arg1 as *mut u8) = 0;
                        }
                    }
                    *result = value.0;
                    file_desc::set_offset(arg0 as i32, offset + value.1 as i64);
                }
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // int unlink(const char *pathname)
        SYS_unlink => {
            let dir_path = &CURRENT_DIR;
            let file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            match CLIENT.unlink_remote(&remote_pathname) {
                Ok(_) => *result = 0,
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        //    int stat(const char *restrict pathname,
        //             struct stat *restrict statbuf);
        SYS_stat => {
            let dir_path = &CURRENT_DIR;
            let file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            let statbuf = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, STAT_SIZE) };
            match CLIENT.stat_remote(&remote_pathname, statbuf) {
                Ok(_) => *result = 0,
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        //  int lstat(const char *restrict pathname,
        //     struct stat *restrict statbuf);
        SYS_lstat => {
            let dir_path = &CURRENT_DIR;
            let file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            let statbuf = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, STAT_SIZE) };
            match CLIENT.stat_remote(&remote_pathname, statbuf) {
                Ok(_) => *result = 0,
                Err(e) => {
                    *result = -e as isize;
                }
            }

            return InterceptResult::Hook;
        }
        // int fstat(int fd, struct stat *statbuf);
        SYS_fstat => {
            let remote_pathname = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => attr.pathname.clone(),
                    _ => return InterceptResult::Forward,
                }
            };

            let statbuf = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, STAT_SIZE) };
            match CLIENT.stat_remote(&remote_pathname, statbuf) {
                Ok(_) => {
                    *result = 0;
                }
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // ssize_t read(int fd, void *buf, size_t count);
        SYS_read => {
            let (remote_pathname, offset) = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::File {
                            *result = -libc::EBADF as isize;
                            return InterceptResult::Hook;
                        }
                        (attr.pathname.clone(), attr.offset)
                    }
                    _ => return InterceptResult::Forward,
                }
            };

            let buf = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, arg2 as usize) };

            match CLIENT.pread_remote(&remote_pathname, buf, offset) {
                Ok(value) => {
                    *result = value;
                    file_desc::set_offset(arg0 as i32, offset + *result as i64);
                }
                Err(e) => {
                    *result = -e as isize;
                }
            }

            return InterceptResult::Hook;
        }
        // ssize_t pread(int fd, void *buf, size_t count, off_t offset)
        SYS_pread64 => {
            let remote_pathname = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::File {
                            *result = -libc::EBADF as isize;
                            return InterceptResult::Hook;
                        }
                        attr.pathname.clone()
                    }
                    _ => return InterceptResult::Forward,
                }
            };

            let buf = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, arg2 as usize) };
            match CLIENT.pread_remote(&remote_pathname, buf, arg2 as i64) {
                Ok(value) => *result = value,
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // ssize_t readv(int fd, const struct iovec *iov, int iovcnt);
        SYS_readv => {
            let (remote_pathname, offset) = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::File {
                            *result = -libc::EBADF as isize;
                            return InterceptResult::Hook;
                        }
                        (attr.pathname.clone(), attr.offset)
                    }
                    _ => return InterceptResult::Forward,
                }
            };

            let iov = unsafe { std::slice::from_raw_parts(arg1 as *const iovec, arg2 as usize) };
            *result = CLIENT.preadv_remote(&remote_pathname, iov, offset) as isize;
            file_desc::set_offset(arg0 as i32, offset + *result as i64);
            InterceptResult::Hook
        }
        // ssize_t preadv(int fd, const struct iovec *iov, int iovcnt,
        //                    off_t offset);
        SYS_preadv => {
            let remote_pathname = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::File {
                            *result = -libc::EBADF as isize;
                            return InterceptResult::Hook;
                        }
                        attr.pathname.clone()
                    }
                    _ => return InterceptResult::Forward,
                }
            };

            let iov = unsafe { std::slice::from_raw_parts(arg1 as *const iovec, arg2 as usize) };
            *result = CLIENT.preadv_remote(&remote_pathname, iov, arg3 as i64) as isize;

            InterceptResult::Hook
        }

        // ssize_t readlink(const char *restrict pathname, char *restrict buf,
        //                     size_t bufsiz);
        SYS_readlink => {
            let dir_path = &CURRENT_DIR;
            let file_path = unsafe { CStr::from_ptr(arg0 as *const c_char).to_str().unwrap() };
            let absolute_pathname = match get_absolutepath(dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };
            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            let mut statbuf = [0u8; STAT_SIZE];
            match CLIENT.stat_remote(&remote_pathname, &mut statbuf) {
                Ok(_) => {
                    let mode = unsafe { (*(statbuf.as_ptr() as *const stat)).st_mode };
                    if (mode & S_IFLNK) == 0 {
                        *result = -libc::EINVAL as isize;
                        return InterceptResult::Hook;
                    }
                }
                Err(e) => {
                    *result = -e as isize;
                    return InterceptResult::Hook;
                }
            }

            let buf = unsafe { std::slice::from_raw_parts_mut(arg1 as *mut u8, arg2 as usize) };

            match CLIENT.pread_remote(&remote_pathname, buf, 0) {
                Ok(value) => {
                    *result = value;
                }
                Err(e) => {
                    *result = -e as isize;
                }
            }

            return InterceptResult::Hook;
        }
        // ssize_t write(int fd, const void *buf, size_t count);
        SYS_write => {
            let (remote_pathname, offset) = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::File {
                            *result = -libc::EBADF as isize;
                            return InterceptResult::Hook;
                        }
                        (attr.pathname.clone(), attr.offset)
                    }
                    _ => return InterceptResult::Forward,
                }
            };
            let buf = unsafe { std::slice::from_raw_parts(arg1 as *const u8, arg2 as usize) };
            match CLIENT.pwrite_remote(&remote_pathname, buf, offset) {
                Ok(value) => {
                    *result = value;
                    file_desc::set_offset(arg0 as i32, offset + *result as i64);
                }
                Err(e) => {
                    *result = -e as isize;
                }
            }
            InterceptResult::Hook
        }

        // ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
        SYS_pwrite64 => {
            let remote_pathname = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::File {
                            *result = -libc::EBADF as isize;
                            return InterceptResult::Hook;
                        }
                        attr.pathname.clone()
                    }
                    _ => return InterceptResult::Forward,
                }
            };
            let buf = unsafe { std::slice::from_raw_parts(arg1 as *const u8, arg2 as usize) };
            match CLIENT.pwrite_remote(&remote_pathname, buf, arg3 as i64) {
                Ok(value) => *result = value,
                Err(e) => {
                    *result = -e as isize;
                }
            }

            InterceptResult::Hook
        }
        // ssize_t writev(int fd, const struct iovec *iov, int iovcnt);
        SYS_writev => {
            let (remote_pathname, offset) = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::File {
                            *result = -libc::EBADF as isize;
                            return InterceptResult::Hook;
                        }
                        (attr.pathname.clone(), attr.offset)
                    }
                    _ => return InterceptResult::Forward,
                }
            };

            let iov = unsafe { std::slice::from_raw_parts(arg1 as *const iovec, arg2 as usize) };
            *result = CLIENT.pwritev_remote(&remote_pathname, iov, offset) as isize;
            file_desc::set_offset(arg0 as i32, offset + *result as i64);
            InterceptResult::Hook
        }
        // ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt,
        //                    off_t offset);
        SYS_pwritev => {
            let remote_pathname = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::File {
                            *result = -libc::EBADF as isize;
                            return InterceptResult::Hook;
                        }
                        attr.pathname.clone()
                    }
                    _ => return InterceptResult::Forward,
                }
            };
            let iov = unsafe { std::slice::from_raw_parts(arg1 as *const iovec, arg2 as usize) };
            *result = CLIENT.pwritev_remote(&remote_pathname, iov, arg3 as i64) as isize;

            InterceptResult::Hook
        }
        // off_t lseek(int fd, off_t offset, int whence);
        SYS_lseek => {
            let (remote_pathname, offset) = {
                match file_desc::get_attr(arg0 as i32) {
                    Some(attr) => {
                        if attr.r#type != FdType::File {
                            *result = -libc::EBADF as isize;
                            return InterceptResult::Hook;
                        }
                        (attr.pathname.clone(), attr.offset)
                    }
                    _ => return InterceptResult::Forward,
                }
            };

            match arg2 as i32 {
                SEEK_SET => file_desc::set_offset(arg0 as i32, arg1 as i64),
                SEEK_CUR => file_desc::set_offset(arg0 as i32, offset + arg1 as i64),
                SEEK_END => {
                    let mut statbuf = [0u8; STAT_SIZE];
                    match CLIENT.stat_remote(&remote_pathname, &mut statbuf) {
                        Ok(_) => {
                            let filesize = unsafe { (*(statbuf.as_ptr() as *const stat)).st_size };

                            file_desc::set_offset(arg0 as i32, filesize + arg1 as i64);
                        }
                        Err(e) => {
                            *result = -e as isize;
                        }
                    }
                }
                _ => {}
            };

            InterceptResult::Hook
        }
        //    int newfstatat(int dirfd, const char *restrict pathname,
        //     struct stat *restrict statbuf, int flags);
        262 => {
            let file_path = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let dir_path = if arg0 as i32 == AT_FDCWD {
                CURRENT_DIR.to_string()
            } else {
                match file_desc::get_attr(arg0 as i32) {
                    Some(value) => MOUNT_POINT.to_string() + &value.pathname,
                    None => {
                        if !file_path.starts_with('/') {
                            return InterceptResult::Forward;
                        } else {
                            "".to_string()
                        }
                    }
                }
            };
            let absolute_pathname = match get_absolutepath(&dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };

            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };
            let statbuf = unsafe { std::slice::from_raw_parts_mut(arg2 as *mut u8, STAT_SIZE) };
            match CLIENT.stat_remote(&remote_pathname, statbuf) {
                Ok(_) => *result = 0,
                Err(e) => {
                    *result = -e as isize;
                }
            }
            InterceptResult::Hook
        }
        // int statx(int dirfd, const char *pathname, int flags,
        //   unsigned int mask, struct statx *statxbuf);
        SYS_statx => {
            let file_path = unsafe { CStr::from_ptr(arg1 as *const c_char).to_str().unwrap() };
            let dir_path = if arg0 as i32 == AT_FDCWD {
                CURRENT_DIR.to_string()
            } else {
                match file_desc::get_attr(arg0 as i32) {
                    Some(value) => MOUNT_POINT.to_string() + &value.pathname,
                    None => {
                        if !file_path.starts_with('/') {
                            return InterceptResult::Forward;
                        } else {
                            "".to_string()
                        }
                    }
                }
            };
            let absolute_pathname = match get_absolutepath(&dir_path, file_path) {
                Ok(value) => value,
                Err(0) => return InterceptResult::Forward,
                Err(value) => {
                    *result = value as isize;
                    return InterceptResult::Hook;
                }
            };

            let remote_pathname = match get_remotepath(&absolute_pathname) {
                Some(value) => value,
                None => return InterceptResult::Forward,
            };

            let statxbuf = unsafe { std::slice::from_raw_parts_mut(arg4 as *mut u8, STATX_SIZE) };
            match CLIENT.statx_remote(&remote_pathname, statxbuf) {
                Ok(_) => *result = 0,
                Err(e) => {
                    *result = -e as isize;
                }
            }
            InterceptResult::Hook
        }
        // int fsync(int fd);
        SYS_fsync => {
            if file_desc::get_attr(arg0 as i32).is_none() {
                return InterceptResult::Forward;
            }
            *result = 0;
            InterceptResult::Hook
        }
        _ => InterceptResult::Forward,
    }
}
