use libc::SYS_write;

use crate::syscall_intercept::syscall_no_intercept;

struct CStrPointer {
    _p: *const u8,
}

unsafe impl std::marker::Sync for CStrPointer {}
unsafe impl std::marker::Send for CStrPointer {}
lazy_static::lazy_static! {
    static ref LOG_BUF: CStrPointer = CStrPointer {
        _p: vec![0u8; 10].as_slice().as_ptr()
    };
}

unsafe fn _print_log(mut num: i32, pre_char: char, suf_char: char) {
    let c = LOG_BUF._p as *mut u8;
    let mut cnt = 1;
    *c = pre_char as u8;
    while num != 0 {
        *c.offset(cnt) = ((num % 10) + 48) as u8;
        num /= 10;
        cnt += 1;
    }
    *c.offset(cnt) = suf_char as u8;
    *c.offset(cnt + 1) = 0 as u8;
    syscall_no_intercept(SYS_write as isize, 1, c, cnt + 2);
}
