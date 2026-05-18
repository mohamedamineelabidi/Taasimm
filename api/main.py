"""
TaaSim — FastAPI: Demand Forecast & Trip Reservation API
=========================================================
REST API serving:
  - POST /api/auth/token       → issue a JWT (demo creds; not a real login flow)
  - POST /api/demand/forecast  → predicted demand for (zone_id, datetime)
  - POST /api/trips            → demo trip-reservation stub (returns matched JSON;
                                 does NOT yet publish to Kafka raw.trips)
  - GET  /api/zones            → list 16 Casablanca zones (loaded from CSV)
  - GET  /api/zones/{zone_id}  → zone detail
  - GET  /api/health           → health + model-loaded check

JWT auth with two roles: rider (read + reserve), admin (full access).

ML loading mode
---------------
The slim API image does NOT bundle PySpark. The demand forecast endpoint runs in
**heuristic mode** by default: a baseline demand curve adjusted by weekday and
zone popularity. This is deterministic, fast (<5 ms), and beats no-prediction.

To enable the real GBT model loaded from `s3a://mldata/models/demand_v1/`:
  1. Add `pyspark==3.5.4` and an S3A-capable Hadoop bundle to api/requirements.txt
  2. Rebuild the image with a JDK base (current python:3.13-slim has no Java).
  3. Set `PYSPARK_ENABLED=1` in the api service environment.

Status of `model_loaded` is exposed via `/api/health` so dashboards can tell
which mode is active.

Usage:
  uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import csv
import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
import jwt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [API] %(levelname)s %(message)s")
log = logging.getLogger("API")

# ── Config ───────────────────────────────────────────────────────────
JWT_SECRET = os.getenv("JWT_SECRET", "taasim-secret-key-change-in-prod")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "24"))
# When 1, the demand endpoint will attempt to load a Spark PipelineModel and use
# it for inference. Requires pyspark + Java in the runtime image (NOT included
# in the default api/Dockerfile). When 0 (default) we use the deterministic
# heuristic in predict_demand().
PYSPARK_ENABLED = os.getenv("PYSPARK_ENABLED", "0") == "1"
MODEL_PATH = os.getenv("MODEL_PATH", "s3a://mldata/models/demand_v1/")
model_pipeline = None  # populated by load_model startup hook when enabled

# ── App ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="TaaSim API",
    description="Transport as a Service — Casablanca Mobility Platform",
    version="1.0.0",
)

security = HTTPBearer()

# ── Zone catalogue ───────────────────────────────────────────────────
# Loaded from data/zone_mapping_v4.csv (mounted at /data in compose, or relative
# in dev). Falls back to a hard-coded 16-zone dict if the CSV is missing so that
# the API still responds in degraded environments.

_ZONE_FALLBACK = {
    1:  {"name": "Ain Chock",      "lat": 33.5266, "lon": -7.6216},
    2:  {"name": "Sidi Othmane",   "lat": 33.5583, "lon": -7.5613},
    3:  {"name": "Sidi Moumen",    "lat": 33.5838, "lon": -7.5004},
    4:  {"name": "Hay Hassani",    "lat": 33.5465, "lon": -7.6803},
    5:  {"name": "Sbata",          "lat": 33.5358, "lon": -7.5580},
    6:  {"name": "Ben Msik",       "lat": 33.5414, "lon": -7.5650},
    7:  {"name": "Moulay Rachid",  "lat": 33.5685, "lon": -7.5400},
    8:  {"name": "Maarif",         "lat": 33.5704, "lon": -7.6325},
    9:  {"name": "Al Fida",        "lat": 33.5652, "lon": -7.5949},
    10: {"name": "Mers Sultan",    "lat": 33.5775, "lon": -7.6015},
    11: {"name": "Roches Noires",  "lat": 33.5925, "lon": -7.5940},
    12: {"name": "Hay Mohammadi",  "lat": 33.5820, "lon": -7.5575},
    13: {"name": "Anfa",           "lat": 33.5950, "lon": -7.6525},
    14: {"name": "Sidi Belyout",   "lat": 33.5985, "lon": -7.6149},
    15: {"name": "Ain Sebaa",      "lat": 33.6050, "lon": -7.5850},
    16: {"name": "Sidi Bernoussi", "lat": 33.6150, "lon": -7.5150},
}

_ZONE_CSV_CANDIDATES = [
    os.getenv("ZONE_MAPPING_CSV", ""),
    "/data/zone_mapping_v4.csv",
    "/data/zone_mapping.csv",
    os.path.join(os.path.dirname(__file__), "..", "data", "zone_mapping_v4.csv"),
    os.path.join(os.path.dirname(__file__), "..", "data", "zone_mapping.csv"),
]


def _load_zone_info() -> dict:
    """Parse zone CSV → {zone_id: {name, lat, lon}}. Returns fallback on error."""
    for path in _ZONE_CSV_CANDIDATES:
        if not path or not os.path.isfile(path):
            continue
        try:
            out = {}
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    zid = int(row["zone_id"])
                    # v4 uses centroid_lat / centroid_lon; v3 uses casa_centroid_lat
                    lat = row.get("centroid_lat") or row.get("casa_centroid_lat")
                    lon = row.get("centroid_lon") or row.get("casa_centroid_lon")
                    name = row.get("name") or row.get("arrondissement_name") or f"Zone {zid}"
                    if lat is None or lon is None:
                        continue
                    out[zid] = {"name": name, "lat": float(lat), "lon": float(lon)}
            if out:
                log.info("Loaded %d zones from %s", len(out), path)
                return out
        except Exception as e:  # pragma: no cover — degrade gracefully
            log.warning("Failed to load %s: %s", path, e)
    log.warning("No zone CSV found; using hard-coded fallback (%d zones)", len(_ZONE_FALLBACK))
    return _ZONE_FALLBACK


ZONE_INFO = _load_zone_info()

# ── ML Model placeholder (loaded by startup hook when PYSPARK_ENABLED=1) ──
# Initialized to None at module-import time in the Config section above.


# ── Pydantic Models ─────────────────────────────────────────────────
class ForecastRequest(BaseModel):
    zone_id: int = Field(..., ge=1, le=16, description="Casablanca zone ID (1-16)")
    datetime_str: str = Field(
        ...,
        alias="datetime",
        description="Forecast datetime (ISO format: 2026-04-19T14:00:00)",
    )

    class Config:
        populate_by_name = True


class ForecastResponse(BaseModel):
    zone_id: int
    zone_name: str
    datetime: str
    slot_of_day: int
    predicted_demand: float
    model_version: str = "demand_v1"


class TripRequest(BaseModel):
    rider_id: str
    origin_zone: int = Field(..., ge=1, le=16)
    dest_zone: int = Field(..., ge=1, le=16)


class TripResponse(BaseModel):
    trip_id: str
    status: str
    origin_zone: int
    dest_zone: int
    origin_name: str
    dest_name: str
    estimated_eta_sec: Optional[int] = None


class TokenRequest(BaseModel):
    username: str
    role: str = Field("rider", pattern="^(rider|admin)$")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    zones: int
    version: str


# ── Auth helpers ─────────────────────────────────────────────────────
def create_token(username: str, role: str) -> str:
    payload = {
        "sub": username,
        "role": role,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": datetime.utcnow(),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(
            credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM]
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


def require_admin(token: dict = Depends(verify_token)):
    if token.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return token


# ── Forecast logic ───────────────────────────────────────────────────
# By default this is a deterministic heuristic (baseline curve + weekday +
# zone-popularity modifiers). When PYSPARK_ENABLED=1 AND PySpark is importable,
# we try to call the loaded GBT pipeline; on any failure we silently fall back
# to the heuristic. See module docstring for how to enable PySpark mode.
def predict_demand(zone_id: int, dt: datetime) -> float:
    """Predict demand for a zone at a given datetime."""
    hour = dt.hour
    day_of_week = dt.weekday()
    slot_of_day = hour * 2 + (1 if dt.minute >= 30 else 0)
    is_weekend = 1 if day_of_week >= 5 else 0
    is_peak = 1 if hour in [8, 9, 13, 14, 17, 18] else 0

    if PYSPARK_ENABLED and model_pipeline is not None:
        # Optional PySpark path — only used when api/requirements.txt has been
        # extended with pyspark + a JVM is available in the image. Default
        # docker image (python:3.13-slim) does NOT support this branch.
        try:
            from pyspark.sql import SparkSession, Row  # type: ignore
            spark = SparkSession.builder.getOrCreate()
            row = Row(
                origin_zone=zone_id,
                slot_of_day=slot_of_day,
                hour_of_day=hour,
                day_of_week=day_of_week,
                is_weekend=is_weekend,
                is_peak=is_peak,
                supply_demand_ratio=0.5,
                demand_lag_1d=50.0,
                demand_lag_7d=50.0,
                rolling_7d_mean=50.0,
            )
            df = spark.createDataFrame([row])
            pred = model_pipeline.transform(df)
            return float(pred.select("prediction").collect()[0][0])
        except Exception as e:
            log.warning("Model prediction failed, using heuristic: %s", e)

    # Heuristic fallback: baseline demand curve
    base_demand = {
        0: 15, 1: 10, 2: 8, 3: 6, 4: 5, 5: 8,
        6: 20, 7: 45, 8: 70, 9: 65, 10: 50, 11: 55,
        12: 60, 13: 65, 14: 70, 15: 55, 16: 50, 17: 65,
        18: 70, 19: 55, 20: 45, 21: 35, 22: 25, 23: 20,
    }
    demand = base_demand.get(hour, 30)

    # Weekend adjustment (-15%)
    if is_weekend:
        demand *= 0.85

    # Zone popularity adjustment (zones 8, 9, 14 are busier)
    if zone_id in [8, 9, 14]:
        demand *= 1.3
    elif zone_id in [1, 3, 16]:
        demand *= 0.7

    return round(demand, 1)


# ── Endpoints ────────────────────────────────────────────────────────
@app.get("/api/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="healthy",
        model_loaded=model_pipeline is not None,
        zones=len(ZONE_INFO),
        version="1.0.0",
    )


@app.post("/api/auth/token", response_model=TokenResponse)
async def get_token(req: TokenRequest):
    """Issue a JWT token. In production, this would validate credentials."""
    token = create_token(req.username, req.role)
    return TokenResponse(
        access_token=token,
        expires_in=JWT_EXPIRE_HOURS * 3600,
    )


@app.post("/api/demand/forecast", response_model=ForecastResponse)
async def forecast_demand(req: ForecastRequest, token: dict = Depends(verify_token)):
    """Predict demand for a zone at a specific datetime."""
    if req.zone_id not in ZONE_INFO:
        raise HTTPException(status_code=404, detail=f"Zone {req.zone_id} not found")

    try:
        dt = datetime.fromisoformat(req.datetime_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid datetime format. Use ISO: 2026-04-19T14:00:00")

    predicted = predict_demand(req.zone_id, dt)
    slot = dt.hour * 2 + (1 if dt.minute >= 30 else 0)

    return ForecastResponse(
        zone_id=req.zone_id,
        zone_name=ZONE_INFO[req.zone_id]["name"],
        datetime=dt.isoformat(),
        slot_of_day=slot,
        predicted_demand=predicted,
    )


@app.post("/api/trips", response_model=TripResponse)
async def create_trip(req: TripRequest, token: dict = Depends(verify_token)):
    """Create a trip reservation."""
    if req.origin_zone not in ZONE_INFO or req.dest_zone not in ZONE_INFO:
        raise HTTPException(status_code=404, detail="Invalid zone ID")

    import uuid
    trip_id = str(uuid.uuid4())[:8]

    return TripResponse(
        trip_id=trip_id,
        status="matched",
        origin_zone=req.origin_zone,
        dest_zone=req.dest_zone,
        origin_name=ZONE_INFO[req.origin_zone]["name"],
        dest_name=ZONE_INFO[req.dest_zone]["name"],
        estimated_eta_sec=180,
    )


@app.get("/api/zones")
async def list_zones(token: dict = Depends(verify_token)):
    """List all Casablanca zones."""
    return [
        {"zone_id": zid, **info}
        for zid, info in sorted(ZONE_INFO.items())
    ]


@app.get("/api/zones/{zone_id}")
async def get_zone(zone_id: int, token: dict = Depends(verify_token)):
    """Get zone details."""
    if zone_id not in ZONE_INFO:
        raise HTTPException(status_code=404, detail=f"Zone {zone_id} not found")
    return {"zone_id": zone_id, **ZONE_INFO[zone_id]}


# ── Startup: load model ─────────────────────────────────────────────
@app.on_event("startup")
async def load_model():
    """Try to load the GBT model when PySpark mode is enabled.

    In the default container image PySpark is not installed and PYSPARK_ENABLED
    is 0, so this hook is a no-op and predict_demand() uses the heuristic.
    """
    global model_pipeline
    if not PYSPARK_ENABLED:
        log.info("PYSPARK_ENABLED=0 — skipping model load; using heuristic forecast.")
        model_pipeline = None
        return
    try:
        from pyspark.sql import SparkSession  # type: ignore
        from pyspark.ml import PipelineModel  # type: ignore

        spark = (
            SparkSession.builder
            .appName("TaaSim-API")
            .master("local[1]")
            .config("spark.driver.memory", "512m")
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )
        model_pipeline = PipelineModel.load(MODEL_PATH)
        log.info("ML model loaded from %s", MODEL_PATH)
    except Exception as e:
        log.warning("Could not load ML model (using heuristic fallback): %s", e)
        model_pipeline = None
