-- Outgoing request topic read by the HTTP Sink connector (one record per part number to look up).
-- `key` is the raw string key (CC Flink requires that name for a raw key); part_number is also in
-- the value so the connector can template it into the URL path.
CREATE TABLE part_requests (
  `key`        STRING,
  part_number  STRING,
  requested_at TIMESTAMP_LTZ(3)
) DISTRIBUTED BY (`key`) INTO 3 BUCKETS
WITH ('changelog.mode' = 'append', 'key.format' = 'raw', 'value.format' = 'avro-registry');
