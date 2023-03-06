#[link(name = "syscall_intercept")]
extern "C" {
    static mut intercept_hook_point: Option<HookFn>;

    pub fn syscall_no_intercept(num: isize, ...) -> isize;
}

/// Set syscall intercept hook function.
///
/// # Safety
///
/// This function will change all syscall behavior!
pub unsafe fn set_hook_fn(f: HookFn) {
    intercept_hook_point = Some(f);
}

/// Clear syscall intercept hook function.
///
/// # Safety
///
/// This function will change all syscall behavior!
pub unsafe fn unset_hook_fn() {
    intercept_hook_point = None;
}

/// The type of hook function.
pub type HookFn = extern "C" fn(
    num: isize,
    a0: isize,
    a1: isize,
    a2: isize,
    a3: isize,
    a4: isize,
    a5: isize,
    result: &mut isize,
) -> InterceptResult;

/// The return value of hook function.
#[repr(i32)]
pub enum InterceptResult {
    /// The user takes over the system call. The return value should be set via `result`.
    Hook = 0,
    /// The specific system call was ignored by the user and the original syscall should be executed.
    Forward = 1,
}
