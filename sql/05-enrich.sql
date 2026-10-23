CREATE TABLE machine_events_enriched (
  event_id    STRING,
  machine_id  STRING,
  plant       STRING,
  severity    STRING,
  criticality STRING,
  message     STRING,
  event_time  TIMESTAMP_LTZ(3),
  parts       ARRAY<ROW<
                part_number STRING, lookup_status STRING, description STRING, category STRING,
                material STRING, supplier STRING, weight_kg DOUBLE, lifecycle_status STRING,
                replacement_cost_eur DOUBLE>>,
  PRIMARY KEY (event_id) NOT ENFORCED
) DISTRIBUTED BY (event_id) INTO 3 BUCKETS
WITH ('changelog.mode' = 'upsert', 'key.format' = 'avro-registry', 'value.format' = 'avro-registry');

-- One output row per event, emitted once every part of the event has a lookup result
-- (success or failure). Events and part_details are both kept in join state for the TTL.
SET 'sql.state-ttl' = '2 h';

INSERT INTO machine_events_enriched
SELECT event_id, machine_id, plant, severity, criticality, message,
       CAST(event_time AS TIMESTAMP_LTZ(3)) AS event_time,
       ARRAY_AGG(ROW(
         part_number, lookup_status,
         JSON_VALUE(details, '$.description'),
         JSON_VALUE(details, '$.category'),
         JSON_VALUE(details, '$.material'),
         JSON_VALUE(details, '$.supplier.name'),
         JSON_VALUE(details, '$.weightKg' RETURNING DOUBLE),
         JSON_VALUE(details, '$.lifecycleStatus'),
         JSON_VALUE(details, '$.replacementCostEur' RETURNING DOUBLE)
       )) AS parts
FROM (
  SELECT x.event_id, x.machine_id, x.plant, x.severity, x.criticality, x.message, x.event_time,
         x.expected, x.part_number, d.lookup_status, d.details
  FROM (
    SELECT e.event_id, e.machine_id, e.plant, e.severity, e.criticality, e.message, e.event_time,
           CARDINALITY(e.part_numbers) AS expected, p.part_number
    FROM machine_events e CROSS JOIN UNNEST(e.part_numbers) AS p(part_number)
  ) x
  JOIN part_details d ON x.part_number = d.part_number
)
GROUP BY event_id, machine_id, plant, severity, criticality, message, event_time
HAVING COUNT(*) = MAX(expected);
