fn main() {
    let output = std::process::Command::new("git")
        .args(&["rev-parse", "HEAD"])
        .output()
        .expect("failed to run 'git rev-parse HEAD'");
    println!("cargo:rustc-env=GIT_HASH={}", std::str::from_utf8(&output.stdout).unwrap());
}
