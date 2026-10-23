-- Latest lookup result per part number. part_responses / part_lookup_errors are written by the
-- connector without a schema: key and value are JSON-encoded strings, so we unwrap them twice.
CREATE TABLE part_details (
  part_number   STRING,
  lookup_status STRING,
  details       STRING,
  looked_up_at  TIMESTAMP_LTZ(3),
  PRIMARY KEY (part_number) NOT ENFORCED
) DISTRIBUTED BY (part_number) INTO 3 BUCKETS
WITH ('changelog.mode' = 'upsert', 'key.format' = 'avro-registry', 'value.format' = 'avro-registry');

INSERT INTO part_details
SELECT JSON_VALUE(CAST(`key` AS STRING), '$')  AS part_number,
       'OK'                                   AS lookup_status,
       JSON_VALUE(CAST(val AS STRING), '$')   AS details,
       $rowtime                               AS looked_up_at
FROM part_responses
UNION ALL
SELECT JSON_VALUE(CAST(`key` AS STRING), '$'),
       'LOOKUP_FAILED',
       JSON_VALUE(CAST(val AS STRING), '$'),
       $rowtime
FROM part_lookup_errors;
