"""Mock of a part-master HTTP API (response shape is invented)."""
import asyncio
import hashlib
import os
import random
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException

app = FastAPI(title="part API mock")

LATENCY_MS = int(os.getenv("MOCK_LATENCY_MS", "150"))
JITTER_MS = int(os.getenv("MOCK_JITTER_MS", "100"))

MATERIALS = ["Aluminium 6082", "Steel S355", "Stainless 1.4301", "PA66-GF30", "Copper Cu-ETP", "Titanium Gr5"]
SUPPLIERS = ["Bosch Rexroth", "ZF Friedrichshafen", "Schaeffler", "Continental", "Mahle", "Festo", "SKF"]
CATEGORIES = ["Gearbox", "Hydraulic pump", "Servo motor", "Bearing assembly", "Sensor module", "Drive shaft", "Valve block"]
PLANTS = ["Plant North", "Plant South", "Plant East", "Plant West", "Plant Central", "Plant Coastal"]


def _rng(part: str) -> random.Random:
    return random.Random(int(hashlib.sha256(part.encode()).hexdigest(), 16))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/v1/parts/{part_number}")
async def physical_part(part_number: str):
    await asyncio.sleep((LATENCY_MS + random.randint(0, JITTER_MS)) / 1000)
    # Demo knobs: part numbers with these suffixes simulate failures.
    if part_number.endswith("-E404"):
        raise HTTPException(status_code=404, detail=f"physical part {part_number} not found")
    if part_number.endswith("-E503"):
        raise HTTPException(status_code=503, detail="part backend unavailable")

    r = _rng(part_number)
    category = r.choice(CATEGORIES)
    produced = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=r.randint(0, 600))
    return {
        "partNumber": part_number,
        "description": f"{category} {part_number[-6:]}",
        "category": category,
        "material": r.choice(MATERIALS),
        "supplier": {"name": r.choice(SUPPLIERS), "supplierId": f"SUP{r.randint(10000, 99999)}"},
        "plant": r.choice(PLANTS),
        "weightKg": round(r.uniform(0.2, 85.0), 2),
        "lifecycleStatus": r.choice(["SERIES", "SERIES", "SERIES", "PHASE_OUT", "PROTOTYPE"]),
        "producedAt": produced.isoformat(),
        "replacementCostEur": round(r.uniform(20, 12000), 2),
        "assembly": {
            "level": r.randint(2, 6),
            "parentPartNumber": f"PN-{r.randint(10**8, 10**9 - 1)}-{r.randint(100, 999)}",
            "childCount": r.randint(0, 24),
        },
    }
