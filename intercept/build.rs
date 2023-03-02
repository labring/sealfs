fn main() {
    let dst = cmake::build("../syscall_intercept");

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=syscall_intercept");
    println!("cargo:rustc-link-lib=capstone");
}
