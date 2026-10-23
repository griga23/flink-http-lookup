-- Explode the part_numbers array and emit each part number once per TTL window.
-- The state TTL doubles as the lookup-cache lifetime: after 30 min a part is looked up again.
SET 'sql.state-ttl' = '30 min';
-- Don't let empty/idle partitions of machine_events hold back the watermark.
SET 'sql.tables.scan.idle-timeout' = '5 s';

INSERT INTO part_requests
SELECT part_number AS `key`, part_number, event_time AS requested_at
FROM (
  SELECT part_number, event_time,
         ROW_NUMBER() OVER (PARTITION BY part_number ORDER BY event_time ASC) AS rn
  FROM (
    SELECT p.part_number, e.event_time
    FROM machine_events e CROSS JOIN UNNEST(e.part_numbers) AS p(part_number)
  )
)
WHERE rn = 1;
