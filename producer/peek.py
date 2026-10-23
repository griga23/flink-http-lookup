#!/usr/bin/env python3
"""Dump raw records (key, headers, value bytes) from a topic. Usage: peek.py TOPIC [max]"""
import os, sys, time
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"].replace("SASL_SSL://", ""),
    "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"], "sasl.password": os.environ["KAFKA_API_SECRET"],
    "group.id": f"peek-{time.time()}", "auto.offset.reset": "earliest"})
c.subscribe([sys.argv[1]])
n, limit, idle = 0, int(sys.argv[2]) if len(sys.argv) > 2 else 5, 0
while n < limit and idle < 15:
    m = c.poll(1.0)
    if m is None:
        idle += 1; continue
    if m.error():
        print(m.error()); continue
    n += 1
    print(f"--- partition={m.partition()} offset={m.offset()}\nkey={m.key()!r}\nheaders={m.headers()}\nvalue={m.value()!r}")
c.close()
