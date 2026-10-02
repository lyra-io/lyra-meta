use prost_build::Config;
use std::io;

fn main() -> io::Result<()> {
    println!("cargo:rerun-if-changed=proto/pb_meta.proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    Config::new().compile_protos(&["proto/pb_meta.proto"], &["proto"])
}
