import os
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# ============================================================
# 1. CONNECT TO SUPABASE
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

CHUNK_SIZE = 10000  # processing window


# ============================================================
# CHECKPOINT HELPERS
# ============================================================

def get_checkpoint(key: str) -> datetime:
    res = supabase.table("aggregation_state").select("*").eq("key", key).execute()
    if res.data:
        return datetime.fromisoformat(res.data[0]["value"].replace("Z", "+00:00"))
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def set_checkpoint(key: str, dt: datetime):
    supabase.table("aggregation_state").upsert({
        "key": key,
        "value": dt.isoformat()
    }).execute()


# ============================================================
# UTILS
# ============================================================

def floor_time(dt: datetime, minutes: int) -> datetime:
    discard = dt.minute % minutes
    return dt - timedelta(
        minutes=discard,
        seconds=dt.second,
        microseconds=dt.microsecond
    )


# ============================================================
# UNIFIED AGGREGATION ENGINE
# ============================================================

def aggregate_rows(rows, timeframe_minutes):
    """
    Automatically detects timestamp column:
    - minute_ohlc uses "timestamp"
    - aggregated tables use "ts_bucket"
    """
    buckets = {}

    for r in rows:
        ts_str = r.get("timestamp") or r.get("ts_bucket")
        if not ts_str:
            raise Exception(f"❌ Missing timestamp field in row: {r}")

        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        bucket = floor_time(ts, timeframe_minutes)
        bucket_str = bucket.isoformat()

        key = (r["symbol"], bucket_str)

        if key not in buckets:
            buckets[key] = {
                "symbol": r["symbol"],
                "ts_bucket": bucket_str,
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
            }
        else:
            b = buckets[key]
            b["high"] = max(b["high"], float(r["high"]))
            b["low"] = min(b["low"], float(r["low"]))
            b["close"] = float(r["close"])
            b["volume"] += float(r["volume"])

    return list(buckets.values())


# ============================================================
# SAFE BATCH UPSERT
# ============================================================

def batch_upsert(table_name, rows, batch_size=500):
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        supabase.table(table_name).upsert(batch).execute()


# ============================================================
# FETCH MINUTE DATA CHUNKS
# ============================================================

def fetch_minute_chunk(since_timestamp: datetime):
    ts_str = since_timestamp.isoformat()
    res = (
        supabase.table("minute_ohlc")
        .select("*")
        .gt("timestamp", ts_str)
        .order("timestamp", {"ascending": True})
        .limit(CHUNK_SIZE)
        .execute()
    )
    return res.data


# ============================================================
# 5m AGGREGATION
# ============================================================

def process_5m():
    print("→ Processing 5m candles...")

    last_ts = get_checkpoint("last_minute_ts")
    rows = fetch_minute_chunk(last_ts)

    if not rows:
        print("No new minute rows.")
        return False

    agg = aggregate_rows(rows, 5)
    batch_upsert("ohlc_5m", agg)

    last_processed = datetime.fromisoformat(
        rows[-1]["timestamp"].replace("Z", "+00:00")
    )
    set_checkpoint("last_minute_ts", last_processed)

    print(f"Inserted {len(agg)} new 5m candles.")
    return True


# ============================================================
# FETCH 5m CHUNKS (FOR 15m/1h)
# ============================================================

def fetch_5m_chunk(since_timestamp: datetime):
    ts_str = since_timestamp.isoformat()
    res = (
        supabase.table("ohlc_5m")
        .select("*")
        .gt("ts_bucket", ts_str)
        .order("ts_bucket", {"ascending": True})
        .limit(CHUNK_SIZE)
        .execute()
    )
    return res.data


# ============================================================
# 15m AGG
# ============================================================

def process_15m():
    print("→ Processing 15m candles...")

    last_ts = get_checkpoint("last_5m_ts")
    rows = fetch_5m_chunk(last_ts)

    if not rows:
        print("No new 5m rows.")
        return False

    agg = aggregate_rows(rows, 15)
    batch_upsert("ohlc_15m", agg)

    last_processed = datetime.fromisoformat(
        rows[-1]["ts_bucket"].replace("Z", "+00:00")
    )
    set_checkpoint("last_5m_ts", last_processed)

    print(f"Inserted {len(agg)} new 15m candles.")
    return True


# ============================================================
# 1h AGG
# ============================================================

def process_1h():
    print("→ Processing 1h candles...")

    last_ts = get_checkpoint("last_15m_ts")
    rows = fetch_5m_chunk(last_ts)

    if not rows:
        print("No new data for 1h.")
        return False

    agg = aggregate_rows(rows, 60)
    batch_upsert("ohlc_1h", agg)

    last_processed = datetime.fromisoformat(
        rows[-1]["ts_bucket"].replace("Z", "+00:00")
    )
    set_checkpoint("last_15m_ts", last_processed)

    print(f"Inserted {len(agg)} new 1h candles.")
    return True


# ============================================================
# FETCH 1h CHUNKS → 4h/1d
# ============================================================

def fetch_1h_chunk(since_timestamp: datetime):
    ts_str = since_timestamp.isoformat()
    res = (
        supabase.table("ohlc_1h")
        .select("*")
        .gt("ts_bucket", ts_str)
        .order("ts_bucket", {"ascending": True})
        .limit(CHUNK_SIZE)
        .execute()
    )
    return res.data


# ============================================================
# 4h AGG
# ============================================================

def process_4h():
    print("→ Processing 4h candles...")

    last_ts = get_checkpoint("last_1h_ts")
    rows = fetch_1h_chunk(last_ts)

    if not rows:
        print("No new 1h rows.")
        return False

    agg = aggregate_rows(rows, 240)
    batch_upsert("ohlc_4h", agg)

    last_processed = datetime.fromisoformat(
        rows[-1]["ts_bucket"].replace("Z", "+00:00")
    )
    set_checkpoint("last_1h_ts", last_processed)

    print(f"Inserted {len(agg)} new 4h candles.")
    return True


# ============================================================
# 1d AGG
# ============================================================

def process_1d():
    print("→ Processing 1d candles...")

    last_ts = get_checkpoint("last_4h_ts")
    rows = fetch_1h_chunk(last_ts)

    if not rows:
        print("No new 1h for daily.")
        return False

    agg = aggregate_rows(rows, 1440)
    batch_upsert("ohlc_1d", agg)

    last_processed = datetime.fromisoformat(
        rows[-1]["ts_bucket"].replace("Z", "+00:00")
    )
    set_checkpoint("last_4h_ts", last_processed)

    print(f"Inserted {len(agg)} new daily candles.")
    return True


# ============================================================
# MAIN WORKER LOOP
# ============================================================

print("🔥 OHLC Aggregation Worker Started")

while True:
    try:
        print("==========================================")
        print("🚀 Aggregation cycle starting")
        print("==========================================")

        processed_5m = process_5m()
        if processed_5m:
            process_15m()
            process_1h()
            process_4h()
            process_1d()

        print("⏳ Sleeping 60s...\n")
        time.sleep(60)

    except Exception as e:
        print("❌ ERROR:", e)
        print("Retrying in 15 seconds...\n")
        time.sleep(15)
