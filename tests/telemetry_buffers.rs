use hopash::constants::{CORE_LOG_LINE_MAX_BYTES, LOG_CAPACITY};
use hopash::domain::{CoreInstanceGeneration, SampleState, TrafficSample};
use hopash::telemetry::{CoreLogRecord, LogBuffer, LogFilter, LogLevel, LogSource, TelemetryStore};

#[test]
fn log_buffer_evicts_oldest_records_and_reports_resync_gaps() {
    let mut buffer = LogBuffer::new(3, 128).expect("fixture limits should be valid");
    for index in 0..5 {
        buffer
            .push(
                index,
                LogLevel::Info,
                LogSource::CoreApi,
                format!("record-{index}"),
            )
            .expect("fixture record should fit");
    }

    let tail = buffer.tail_after(Some(0));

    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.capacity(), 3);
    assert_eq!(tail.dropped_total, 2);
    assert!(tail.gap);
    assert_eq!(
        messages(&tail.records),
        ["record-2", "record-3", "record-4"]
    );
    assert_eq!(tail.earliest_sequence, Some(3));
    assert_eq!(tail.latest_sequence, Some(5));
}

#[test]
fn log_queries_filter_by_level_time_and_content() {
    let mut buffer = LogBuffer::new(8, 128).expect("fixture limits should be valid");
    buffer
        .push(10, LogLevel::Info, LogSource::CoreApi, "connected")
        .expect("fixture record should fit");
    buffer
        .push(20, LogLevel::Warn, LogSource::Stderr, "Retry pending")
        .expect("fixture record should fit");
    buffer
        .push(30, LogLevel::Warn, LogSource::Stderr, "retry complete")
        .expect("fixture record should fit");

    let records = buffer.query(&LogFilter {
        level: Some(LogLevel::Warn),
        contains: Some("retry".to_owned()),
        since_unix_ms: Some(15),
        until_unix_ms: Some(25),
    });

    assert_eq!(messages(&records), ["Retry pending"]);
}

#[test]
fn oversized_log_lines_are_rejected_without_changing_the_buffer() {
    let mut buffer = LogBuffer::new(2, 4).expect("fixture limits should be valid");

    let error = buffer
        .push(1, LogLevel::Error, LogSource::Stdout, "12345")
        .expect_err("oversized record should be rejected");

    assert_eq!(error.to_string(), "Core Log line exceeds the 4-byte limit");
    assert!(buffer.is_empty());
    assert_eq!(buffer.dropped_total(), 0);
}

#[test]
fn telemetry_accepts_only_the_current_core_generation() {
    let first = CoreInstanceGeneration(4);
    let second = CoreInstanceGeneration(5);
    let mut telemetry =
        TelemetryStore::new(first, 4, 8, 3).expect("fixture limits should be valid");

    assert!(telemetry.publish_traffic(first, traffic(10, 20, 1)));
    assert!(telemetry.publish_connections(first, 7));
    assert!(
        telemetry
            .publish_log(first, 1, LogLevel::Info, LogSource::CoreApi, "ready")
            .expect("fixture log should fit")
    );

    telemetry.replace_core(second);

    assert!(!telemetry.publish_traffic(first, traffic(30, 40, 2)));
    assert!(!telemetry.publish_connections(first, 99));
    assert!(
        !telemetry
            .publish_log(first, 2, LogLevel::Warn, LogSource::Stderr, "stale")
            .expect("stale generation is a normal discard")
    );
    assert_eq!(telemetry.connection_count(), None);
    assert_eq!(telemetry.latest_traffic(), None);
    assert_eq!(messages(&telemetry.logs().records()), ["ready"]);
}

#[test]
fn sustained_traffic_input_keeps_a_fixed_capacity_series() {
    let generation = CoreInstanceGeneration(9);
    let mut telemetry =
        TelemetryStore::new(generation, 2, 16, 5).expect("fixture limits should be valid");

    for index in 0..100_000 {
        assert!(telemetry.publish_traffic(generation, traffic(index, index * 2, index)));
    }

    assert_eq!(telemetry.traffic_history().len(), 5);
    assert_eq!(
        telemetry
            .latest_traffic()
            .expect("latest sample should exist")
            .upload_bytes_per_second,
        99_999
    );
}

#[test]
fn sustained_core_log_input_keeps_the_release_capacity_and_reports_eviction() {
    let total_records = LOG_CAPACITY * 10;
    let mut buffer =
        LogBuffer::new(LOG_CAPACITY, CORE_LOG_LINE_MAX_BYTES).expect("release limits should work");

    for index in 0..total_records {
        buffer
            .push(
                u64::try_from(index).expect("fixture index should fit"),
                LogLevel::Info,
                LogSource::CoreApi,
                "steady-state Core Log",
            )
            .expect("fixture log should fit");
    }

    let tail = buffer.tail_after(Some(0));
    let expected_earliest =
        u64::try_from(total_records - LOG_CAPACITY + 1).expect("fixture sequence should fit");
    assert_eq!(buffer.len(), LOG_CAPACITY);
    assert_eq!(buffer.capacity(), LOG_CAPACITY);
    assert_eq!(
        buffer.dropped_total(),
        (total_records - LOG_CAPACITY) as u64
    );
    assert!(tail.gap);
    assert_eq!(tail.records.len(), LOG_CAPACITY);
    assert_eq!(tail.earliest_sequence, Some(expected_earliest));
    assert_eq!(
        tail.latest_sequence,
        Some(u64::try_from(total_records).expect("fixture sequence should fit"))
    );
}

#[test]
fn log_record_debug_output_omits_message_content() {
    let record = CoreLogRecord::new(
        1,
        2,
        LogLevel::Debug,
        LogSource::CoreApi,
        "https://user:secret@example.test/token",
    );

    let debug = format!("{record:?}");

    assert!(debug.contains("message_bytes"));
    assert!(!debug.contains("secret"));
    assert!(!debug.contains("token"));
}

fn traffic(upload: u64, download: u64, sampled_at_unix_ms: u64) -> TrafficSample {
    TrafficSample {
        upload_bytes_per_second: upload,
        download_bytes_per_second: download,
        sampled_at_unix_ms: Some(sampled_at_unix_ms),
        state: SampleState::Fresh,
    }
}

fn messages(records: &[CoreLogRecord]) -> Vec<&str> {
    records.iter().map(CoreLogRecord::message).collect()
}
