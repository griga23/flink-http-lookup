CREATE TABLE machine_events (
  event_id      STRING,
  machine_id    STRING,
  plant         STRING,
  severity      STRING,
  criticality   STRING,
  message       STRING,
  part_numbers  ARRAY<STRING>,
  event_time    TIMESTAMP_LTZ(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH ('value.format' = 'avro-registry');
