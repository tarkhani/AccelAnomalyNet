from __future__ import annotations

import asyncio
import json
import os
import re
from collections import deque
from datetime import datetime, timezone
from math import sqrt
from pathlib import Path
from threading import Lock
from typing import Any

import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

app = FastAPI(title="Live Data Dashboard API", version="1.0.0")

# Student project defaults (env vars override when set).
_DEFAULT_MQTT_HOST = "606be9cdd83841ab8aa160b075157595.s1.eu.hivemq.cloud"
_DEFAULT_MQTT_PORT = 8883
_DEFAULT_MQTT_TOPIC = "iotproject/accelerometer"
_DEFAULT_MQTT_USER = "tarkhani"
_DEFAULT_MQTT_PASSWORD = "Artorias1376!"

# If device clock is unsynced (common on Pico without NTP), timestamps can be years off.
# When the first sample in a batch is farther than this from server UTC, re-anchor to "now"
# while preserving deltas between samples in that batch.
_TIMESTAMP_DRIFT_MAX_SEC = float(os.getenv("TIMESTAMP_DRIFT_MAX_SEC", "300"))

# === In-memory dashboard state ===
MAX_POINTS = 2000
data_points: deque[dict[str, Any]] = deque(maxlen=MAX_POINTS)
subscribers: set[asyncio.Queue[str]] = set()
data_lock = Lock()

# === Statistical anomaly detection (per-label trailing history) ===
# Z-score vs mean/std of values seen *before* the current sample (no lookahead).
ANOMALY_MIN_SAMPLES = 30
ANOMALY_Z_THRESHOLD = 3.0
ANOMALY_HISTORY_MAX = 2000
_label_value_history: dict[str, deque[float]] = {}


def _history_deque_for_label(label: str) -> deque[float]:
    if label not in _label_value_history:
        _label_value_history[label] = deque(maxlen=ANOMALY_HISTORY_MAX)
    return _label_value_history[label]


def _mean_std(values: list[float]) -> tuple[float, float]:
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mean = sum(values) / n
    if n == 1:
        return mean, 0.0
    var = sum((x - mean) ** 2 for x in values) / (n - 1)
    std = sqrt(var) if var > 0 else 0.0
    return mean, std


def anomaly_fields_for_value(label: str, value: float) -> dict[str, Any]:
    """
    Classify `value` using z-score against prior samples for `label`, then record
    `value` in history for future points.
    """
    hist = _history_deque_for_label(label)
    prior = list(hist)
    n_prior = len(prior)
    ready = n_prior >= ANOMALY_MIN_SAMPLES
    mean: float | None = None
    std: float | None = None
    z: float | None = None
    lower: float | None = None
    upper: float | None = None
    is_anomaly = False

    if ready:
        mean, std = _mean_std(prior)
        if std < 1e-12:
            std = 1e-12
        z = (value - mean) / std
        lower = mean - ANOMALY_Z_THRESHOLD * std
        upper = mean + ANOMALY_Z_THRESHOLD * std
        is_anomaly = abs(z) > ANOMALY_Z_THRESHOLD

    hist.append(value)

    out: dict[str, Any] = {
        "anomaly": is_anomaly,
        "anomaly_ready": ready,
        "z_score": round(z, 4) if z is not None else None,
        "baseline_mean": round(mean, 6) if mean is not None else None,
        "baseline_std": round(std, 6) if std is not None else None,
        "normal_lower": round(lower, 6) if lower is not None else None,
        "normal_upper": round(upper, 6) if upper is not None else None,
    }
    return out


def enrich_point_with_anomaly(row: dict[str, Any]) -> dict[str, Any]:
    label = str(row.get("label") or "stream")
    if "value" not in row:
        return row
    try:
        v = float(row["value"])
    except (TypeError, ValueError):
        return row
    extra = anomaly_fields_for_value(label, v)
    merged = {**row, **extra}
    return merged

dashboard_path = Path(__file__).parent / "templates" / "index.html"
server_loop: asyncio.AbstractEventLoop | None = None
mqtt_client: mqtt.Client | None = None


# === Request payload model ===
class DataPoint(BaseModel):
    value: float = Field(..., description="Numeric value to plot")
    label: str | None = Field(default=None, description="Optional text label")
    timestamp: datetime | None = Field(
        default=None, description="ISO timestamp. Uses server UTC time if missing."
    )


def serialize_point(point: DataPoint) -> dict[str, Any]:
    ts = point.timestamp or datetime.now(timezone.utc)
    return {
        "value": point.value,
        "label": point.label or "stream",
        "timestamp": ts.isoformat(),
    }


def append_point(row: dict[str, Any]) -> dict[str, Any]:
    """Store one row with server-side anomaly fields; returns the enriched dict."""
    with data_lock:
        enriched = enrich_point_with_anomaly(dict(row))
        data_points.append(enriched)
        return enriched


# === MQTT helpers ===
def _timestamp_to_iso(ts_value: Any, fallback: datetime) -> str:
    if isinstance(ts_value, str):
        text = ts_value.strip()
        if text:
            return text
    if isinstance(ts_value, (int, float)):
        try:
            return datetime.fromtimestamp(float(ts_value) / 1000.0, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            pass
    return fallback.isoformat()


def _epoch_ms_from_ts_value(ts_value: Any) -> int | None:
    """Return Unix milliseconds if ts_value is numeric (device convention: ms since epoch)."""
    if isinstance(ts_value, (int, float)):
        try:
            v = float(ts_value)
        except (TypeError, ValueError):
            return None
        # Heuristic: sub-second uptime values are not epoch ms; treat large numbers as ms.
        if abs(v) < 1e11:
            return None
        try:
            datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
        return int(v)
    return None


def _parse_iso_to_utc(text: str) -> datetime | None:
    t = text.strip()
    if not t:
        return None
    try:
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except ValueError:
        return None


def _reanchor_epoch_ms_list(raw_ms: list[int], now: datetime) -> list[int]:
    """If first sample's wall time is far from `now`, shift all ms so the batch lands on server time."""
    if not raw_ms:
        return raw_ms
    try:
        first_dt = datetime.fromtimestamp(raw_ms[0] / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return raw_ms
    drift = abs((first_dt - now).total_seconds())
    if drift <= _TIMESTAMP_DRIFT_MAX_SEC:
        return raw_ms
    anchor_ms = int(now.timestamp() * 1000)
    base = raw_ms[0]
    return [anchor_ms + (x - base) for x in raw_ms]


def _apply_timestamp_reanchor_to_rows(rows: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    """
    Shift timestamps when device wall clock is wrong (e.g. Pico without NTP), using `now`
    (typically when the broker delivered the message) as anchor while preserving deltas.
    """
    if not rows:
        return rows
    ms_list: list[int] = []
    for r in rows:
        ts = r.get("timestamp")
        if ts is None:
            return rows
        p = _parse_iso_to_utc(str(ts)) if not isinstance(ts, datetime) else ts.astimezone(timezone.utc)
        if p is None:
            return rows
        ms_list.append(int(p.timestamp() * 1000))
    anchored = _reanchor_epoch_ms_list(ms_list, now)
    for r, ms in zip(rows, anchored):
        r["timestamp"] = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()
    return rows


def parse_mqtt_batch(payload: str, now: datetime | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    now = now or datetime.now(timezone.utc)

    # Accept JSON payloads too (single object or list), not only CSV lines.
    stripped = payload.strip()
    if stripped:
        try:
            parsed = json.loads(stripped)
            items = parsed if isinstance(parsed, list) else [parsed]
            for item in items:
                if not isinstance(item, dict):
                    continue
                ts = _timestamp_to_iso(item.get("timestamp"), now)
                label = str(item.get("label") or "mpu6050")

                if all(k in item for k in ("ax", "ay", "az")):
                    try:
                        ax_f = float(item["ax"])
                        ay_f = float(item["ay"])
                        az_f = float(item["az"])
                        magnitude = (ax_f * ax_f + ay_f * ay_f + az_f * az_f) ** 0.5
                        rows.append(
                            {
                                "timestamp": ts,
                                "ax": ax_f,
                                "ay": ay_f,
                                "az": az_f,
                                "value": float(item.get("value", magnitude)),
                                "label": label,
                            }
                        )
                    except (TypeError, ValueError):
                        continue
                elif "value" in item:
                    try:
                        rows.append(
                            {
                                "timestamp": ts,
                                "value": float(item["value"]),
                                "label": label,
                            }
                        )
                    except (TypeError, ValueError):
                        continue
            if rows:
                return _apply_timestamp_reanchor_to_rows(rows, now)
        except json.JSONDecodeError:
            pass

    parsed_lines: list[tuple[str, str, str, str]] = []
    # Accept CSV samples whether they are newline-separated or space-separated.
    # Example: "19587,0.000,0.000,1.000 19638,0.000,0.000,1.000 ..."
    csv_pattern = re.compile(
        r"(-?\d+),(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)"
    )
    parsed_lines.extend(csv_pattern.findall(payload))

    if not parsed_lines:
        return rows

    # CSV format from device is "epoch_ms,ax,ay,az". Re-anchor epoch when RTC is wrong
    # (see _reanchor_epoch_ms_list) so wall time matches server receive time `now`.
    line_data: list[tuple[int, float, float, float]] = []
    for ts_raw, ax, ay, az in parsed_lines:
        try:
            line_data.append((int(ts_raw), float(ax), float(ay), float(az)))
        except (TypeError, ValueError):
            continue
    if not line_data:
        return rows
    raw_ms = [t[0] for t in line_data]
    anchored_ms = _reanchor_epoch_ms_list(raw_ms, now)
    for (_, ax_f, ay_f, az_f), ts_ms in zip(line_data, anchored_ms):
        timestamp_iso = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
        magnitude = (ax_f * ax_f + ay_f * ay_f + az_f * az_f) ** 0.5
        rows.append(
            {
                "timestamp": timestamp_iso,
                "ax": ax_f,
                "ay": ay_f,
                "az": az_f,
                "value": magnitude,
                "label": "mpu6050",
            }
        )
    return rows


async def publish(message: dict[str, Any]) -> None:
    payload = f"data: {json.dumps(message)}\n\n"
    for queue in list(subscribers):
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            subscribers.discard(queue)


def on_mqtt_connect(client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: int) -> None:
    if reason_code != 0:
        print(f"MQTT connect failed with code {reason_code}")
        return
    topic = os.getenv("MQTT_TOPIC") or _DEFAULT_MQTT_TOPIC
    client.subscribe(topic)
    print(f"MQTT connected and subscribed to {topic}")


def on_mqtt_message(_client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
    payload = msg.payload.decode("utf-8", errors="ignore")
    received_at = datetime.now(timezone.utc)
    rows = parse_mqtt_batch(payload, now=received_at)
    if not rows:
        return
    # Store all parsed rows before broadcasting to SSE subscribers.
    enriched_rows = [append_point(row) for row in rows]
    if server_loop is not None:
        asyncio.run_coroutine_threadsafe(
            publish({"type": "bulk", "payload": enriched_rows}),
            server_loop,
        )


def start_mqtt_subscriber() -> None:
    global mqtt_client
    host = (os.getenv("MQTT_HOST") or _DEFAULT_MQTT_HOST).strip()
    if not host:
        print("MQTT_HOST is empty; MQTT subscriber disabled.")
        return

    port = int(os.getenv("MQTT_PORT") or str(_DEFAULT_MQTT_PORT))
    username = os.getenv("MQTT_USERNAME") or _DEFAULT_MQTT_USER
    password = os.getenv("MQTT_PASSWORD") or _DEFAULT_MQTT_PASSWORD
    use_tls = os.getenv("MQTT_USE_TLS", "true").lower() != "false"
    tls_insecure = os.getenv("MQTT_TLS_INSECURE", "false").lower() == "true"

    mqtt_client = mqtt.Client(client_id=os.getenv("MQTT_CLIENT_ID", "cloud-dashboard"))
    if username:
        mqtt_client.username_pw_set(username, password)
    if use_tls:
        mqtt_client.tls_set()
        if tls_insecure:
            mqtt_client.tls_insecure_set(True)

    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.connect(host, port, keepalive=60)
    mqtt_client.loop_start()


# === HTTP + SSE endpoints ===
@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    if not dashboard_path.exists():
        raise HTTPException(status_code=500, detail="Dashboard file missing")
    return HTMLResponse(content=dashboard_path.read_text(encoding="utf-8"))


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/data")
async def get_data() -> list[dict[str, Any]]:
    with data_lock:
        return list(data_points)


@app.post("/ingest")
async def ingest_data(point: DataPoint) -> dict[str, Any]:
    row = serialize_point(point)
    stored = append_point(row)
    await publish({"type": "point", "payload": stored})
    return {"message": "stored", "count": len(data_points)}


@app.post("/reset")
async def reset_data() -> dict[str, Any]:
    with data_lock:
        data_points.clear()
        _label_value_history.clear()
    await publish({"type": "reset", "payload": {}})
    return {"message": "reset", "count": 0}


@app.get("/stream")
async def stream(request: Request) -> StreamingResponse:
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=100)
    subscribers.add(queue)

    async def event_generator():
        try:
            with data_lock:
                initial = {"type": "bulk", "payload": list(data_points)}
            # Send current buffered points first so new clients render immediately.
            yield f"data: {json.dumps(initial)}\n\n"

            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                    yield message
                except asyncio.TimeoutError:
                    yield "event: ping\ndata: {}\n\n"
        finally:
            subscribers.discard(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# === App lifecycle hooks ===
@app.on_event("startup")
async def startup_event() -> None:
    global server_loop
    server_loop = asyncio.get_running_loop()
    start_mqtt_subscriber()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    if mqtt_client is not None:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
