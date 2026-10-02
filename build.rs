use prost_build::Config;
use std::io;

fn main() -> io::Result<()> {
    println!("cargo:rerun-if-changed=proto/pb_meta.proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    Config::new()
        .skip_debug([
            ".io.lyra.meta.v1.User",
            ".io.lyra.meta.v1.ScramSha256Verifier",
        ])
        .compile_protos(&["proto/pb_meta.proto"], &["proto"])
}
