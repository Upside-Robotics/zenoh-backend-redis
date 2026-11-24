fn main() {
    let version = rustc_version::version_meta().expect("could not determine rustc version");
    println!(
        "cargo:rustc-env=RUSTC_VERSION={}",
        version.short_version_string
    );
}
