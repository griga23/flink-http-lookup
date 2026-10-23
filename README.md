# Flink SQL + managed HTTP connector: part-number lookup demo

```
produce.py ─► machine_events ─► [03 unnest + dedupe] ─► part_requests ─► HTTP Sink V2 (GET) ─► mock API (fly.io)
                  │                                                              │
                  │                                         part_responses / part_lookup_errors
                  │                                                              │
                  │                                           [04] part_details (latest per part, upsert)
                  └──────────────► [05 join + ARRAY_AGG] ◄───────────────────────┘
                                          │
                                  machine_events_enriched
```

Runs on a Confluent Cloud Kafka cluster and Flink compute pool in Azure, plus a mock part API hosted on any public HTTPS endpoint (the demo uses Fly.io).

## Run
```
cd terraform && terraform init && terraform apply                      # keys + response/error/DLQ topics
terraform output -raw secrets_env > ../secrets.env                     # used by producer/ and sql/flink.py
cd .. && python3 -m venv .venv && .venv/bin/pip install requests "confluent-kafka[avro,schemaregistry]" faker
.venv/bin/python sql/flink.py sql/01-tables.sql
.venv/bin/python sql/flink.py sql/02-part-requests.sql
cd terraform && terraform apply -var deploy_connector=true && cd ..    # connector needs part_requests to exist
.venv/bin/python sql/flink.py sql/03-prepare-requests.sql              # long-running INSERT
.venv/bin/python sql/flink.py sql/04-part-details.sql
.venv/bin/python sql/flink.py sql/05-enrich.sql
.venv/bin/python producer/produce.py --count 200 --rate 3              # keep it running during the demo
```
Inspect: `producer/peek.py <topic>` (raw records, needs `set -a; . ./secrets.env`), or `SELECT * FROM machine_events_enriched` in the Flink UI.

## Key points
- Success topic: key = request key, value = response body as a JSON-encoded string, no schema. Flink unwraps it with `JSON_VALUE(JSON_VALUE(CAST(val AS STRING), '$'), '$.field')`; no UDF needed.
- Failed lookups (404, retries exhausted) land in `part_lookup_errors` and become `LOOKUP_FAILED` rows, so events still complete.
- `sql.state-ttl` in 03 is the lookup-cache lifetime (30 min); the join in 05 keeps state for 2 h.
- Event-time dedupe emits when the watermark advances, so keep the producer running (a stopped stream leaves its last events unflushed).
- Mock knobs: part numbers ending in `-E404` return 404, `-E503` return 503; `MOCK_LATENCY_MS` env on fly.

## Teardown
```
.venv/bin/python sql/flink.py --stop <statement names>   # 03, 04 insert, 05 insert
cd terraform && terraform destroy -var deploy_connector=true
# drop tables machine_events, part_requests, part_details, machine_events_enriched in Flink
fly apps destroy <your-app-name>
confluent api-key delete <key-id>                        # the Cloud API key used by Terraform
```
