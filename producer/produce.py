#!/usr/bin/env python3
"""Produce fake factory-machine monitoring events to the machine_events topic (Avro).

Usage: produce.py [--count N] [--rate EVENTS_PER_SEC] [--parts-pool N] [--bad-pct PCT]
Reads ../secrets.env. Uses the schema Flink registered for machine_events-value.
"""
import argparse, json, pathlib, random, time, uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from faker import Faker

TOPIC = "machine_events"
env = dict(l.strip().split("=", 1) for l in (pathlib.Path(__file__).parent.parent / "secrets.env").read_text().splitlines() if "=" in l)
fake = Faker()

SEVERITIES = {  # severity -> (weight, criticality choices, message templates)
    "INFO":     (50, ["LOW"], ["Routine vibration check passed", "Lubrication cycle completed", "Calibration within tolerance"]),
    "WARNING":  (30, ["LOW", "MEDIUM"], ["Temperature trending above nominal", "Bearing vibration elevated", "Hydraulic pressure fluctuation"]),
    "ERROR":    (15, ["MEDIUM", "HIGH"], ["Servo drive fault detected", "Torque limit exceeded", "Sensor signal lost"]),
    "FATAL":    (5,  ["HIGH", "SAFETY_CRITICAL"], ["Emergency stop triggered", "Spindle overload shutdown", "Coolant loss - machine halted"]),
}
PLANTS = ["Plant North", "Plant South", "Plant East", "Plant West", "Plant Central", "Plant Coastal"]


def part_pool(size, bad_pct, seed=42):
    """Deterministic pool of part numbers; a few use the -E404 suffix the mock turns into a 404."""
    r = random.Random(seed)
    pool = [f"PN-{r.randint(10**8, 10**9 - 1)}-{r.randint(100, 999)}" for _ in range(size)]
    n_bad = max(0, round(size * bad_pct / 100))
    bad = [f"PN-{r.randint(10**8, 10**9 - 1)}-{r.randint(100, 999)}-E404" for _ in range(n_bad)]
    return pool, bad


def make_event(pool, bad, machines):
    sev = random.choices(list(SEVERITIES), weights=[v[0] for v in SEVERITIES.values()])[0]
    _, crits, msgs = SEVERITIES[sev]
    parts = random.sample(pool, random.randint(1, 4))
    if bad and random.random() < 0.15:
        parts[random.randrange(len(parts))] = random.choice(bad)
    machine_id, plant = random.choice(machines)
    return {
        "event_id": str(uuid.uuid4()),
        "machine_id": machine_id,
        "plant": plant,
        "severity": sev,
        "criticality": random.choice(crits),
        "message": random.choice(msgs),
        "part_numbers": list(dict.fromkeys(parts)),
        "event_time": datetime.now(timezone.utc),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=50)
    ap.add_argument("--rate", type=float, default=2.0)
    ap.add_argument("--parts-pool", type=int, default=30)
    ap.add_argument("--bad-pct", type=float, default=5.0, help="percent of pool size added as unknown parts (404)")
    a = ap.parse_args()

    sr = SchemaRegistryClient({"url": env["SR_URL"], "basic.auth.user.info": f"{env['SR_API_KEY']}:{env['SR_API_SECRET']}"})
    schema_str = sr.get_latest_version(f"{TOPIC}-value").schema.schema_str
    ser = AvroSerializer(sr, schema_str, conf={"auto.register.schemas": False, "use.latest.version": True})
    p = Producer({
        "bootstrap.servers": env["KAFKA_BOOTSTRAP"].replace("SASL_SSL://", ""),
        "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
        "sasl.username": env["KAFKA_API_KEY"], "sasl.password": env["KAFKA_API_SECRET"]})

    pool, bad = part_pool(a.parts_pool, a.bad_pct)
    machines = [(f"{fake.bothify('??-###').upper()}", random.choice(PLANTS)) for _ in range(12)]
    for i in range(a.count):
        ev = make_event(pool, bad, machines)
        p.produce(TOPIC, value=ser(ev, SerializationContext(TOPIC, MessageField.VALUE)))
        p.poll(0)
        print(f"{i+1:>4} {ev['severity']:<7} {ev['criticality']:<16} {ev['machine_id']} parts={len(ev['part_numbers'])}")
        time.sleep(1 / a.rate)
    p.flush()


if __name__ == "__main__":
    main()
