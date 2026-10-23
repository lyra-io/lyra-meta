pub mod pb_catalog {
    include!(concat!(env!("OUT_DIR"), "/io.lyra.proto.catalog.v1.rs"));
}

pub mod pb_meta {
    include!(concat!(env!("OUT_DIR"), "/io.lyra.meta.v1.rs"));
}
