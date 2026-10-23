fn main() {
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    config.type_attribute(".io.lyra.proto.catalog.v1.Scram", "#[derive(Eq)]");
    config.skip_debug([
        ".io.lyra.meta.v1.User",
        ".io.lyra.meta.v1.ScramSha256Verifier",
    ]);

    config
        .compile_protos(
            &[
                "proto/pb_catalog_identity.proto",
                "proto/pb_catalog_io.proto",
                "proto/pb_catalog_namespace.proto",
                "proto/pb_catalog_stream.proto",
                "proto/pb_catalog_value.proto",
                "proto/pb_meta.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
