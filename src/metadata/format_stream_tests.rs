use crate::metadata::{MemoryMetadata, Metadata};
use crate::proto::pb_meta::{Format, FormatType, Stream, StreamFragment};
use crate::utils::verifier::make_verifier;

#[tokio::test]
async fn creates_formats_and_streams_and_switches_fragment_owners() {
    let meta = MemoryMetadata::new();
    meta.initialize(make_verifier("test-password").unwrap())
        .await
        .unwrap();
    let database_id = meta.get_database("public").await.unwrap().unwrap().id();

    let format = Format::new(
        database_id,
        "order_fmt",
        FormatType::Avro as i32,
        b"{\"type\":\"record\"}".to_vec(),
    );
    let created = meta.create_format(format).await.unwrap();
    assert_eq!(created.value().name, "order_fmt");

    let mut stream = Stream::new(database_id, "public", "orders");
    stream.format_name = Some("order_fmt".into());
    stream.fragments = vec![StreamFragment {
        fragment_id: 1,
        start_offset: 0,
        owners: vec!["node-1".into()],
    }];
    let created = meta.create_stream(stream).await.unwrap();
    assert_eq!(created.value().effective_replication(), 2);

    let mut updated = created.value().clone();
    updated.fragments[0].owners = vec!["node-2".into()];
    let updated = meta
        .update_stream(created.id(), updated, created.version())
        .await
        .unwrap();
    assert_eq!(
        updated.value().fragments[0].owners,
        vec!["node-2".to_string()]
    );

    assert_eq!(meta.list_formats(database_id).await.unwrap().len(), 1);
    assert_eq!(meta.list_streams(database_id).await.unwrap().len(), 1);
}
