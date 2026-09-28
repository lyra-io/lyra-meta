fn main() {
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    config.type_attribute(".io.lyra.proto.catalog.v1.Scram", "#[derive(Eq)]");

    config
        .compile_protos(
            &[
                "proto/pb_catalog_identity.proto",
                "proto/pb_catalog_io.proto",
                "proto/pb_catalog_namespace.proto",
                "proto/pb_catalog_stream.proto",
                "proto/pb_catalog_value.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
