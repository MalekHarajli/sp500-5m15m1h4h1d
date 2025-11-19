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

CHUNK_SIZE = 10000  # how many rows to process at once


# ============================================================
# CHECKPOINT HELPERS
# ============================================================

def get_checkpoint(key: str) -> datetime:
    """Load last processed timestamp from checkpoint table."""
    res = supabase.table("aggregation_state").select("*").eq("key", key).execute()
    if res.data:
        return datetime.fromisoformat(res.data[0]["value"].replace("Z", "+00:00"))
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def set_checkpoint(key: str, dt: datetime):
    """Save checkpoint."""
    supabase.table("aggregation_state").upsert({
        "key": key,
        "value": dt.isoformat()
    }).execute()


# ============================================================
# TIME HELPERS
# ============================================================

def floor_time(dt: datetime, minutes: int) -> datetime:
    """Round timestamp down to nearest N-minute bucket."""
    discard = dt.minute % minutes
    return dt - timedelta(
        minutes=discard,
        seconds=dt.second,
        microseconds=dt.microsecond
    )


# ============================================================
# MAIN FETCH FUNCTIONS
# ============================================================

def fetch_minute_chunk(since_timestamp: datetime):
    """
    Fetch CHUNK_SIZE minute rows newer than checkpoint.
    Uses the correct table: minute_ohlc.
    """
    ts_str = since_timestamp.isoformat()

    res = (
        supabase.table("minute_ohlc")              # ✅ FIXED
        .select("*")
        .gt("timestamp", ts_str)
        .order("timestamp", {"ascending": True})   # ✅ FIXED order syntax
        .limit(CHUNK_SIZE)
        .execute()
    )

    return res.data


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
# UNIVERSAL AGGREGATION ENGINE
# ============================================================

def aggregate_rows(rows, timeframe_minutes):
    """
    Convert input rows into OHLCV candles for the desired timeframe.
    Automatically detects `timestamp` or `ts_bucket`.
    """

    buckets = {}

    for r in rows:

        # Detect correct timestamp field
        ts_str = r.get("timestamp") or r.get("ts_bucket")

        if not ts_str:
            raise Exception(f"Missing timestamp in row: {r}")

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
                "volume": float(r["volume"])
            }
        else:
            b = buckets[key]
            b["high"] = max(b["high"], float(r["high"]))
            b["low"] = min(b["low"], float(r["low"]))
            b["close"] = float(r["close"])
            b["volume"] += float(r["volume"])

    return list(buckets.values())


# ============================================================
# SAFE UPSERT
# ============================================================

def batch_upsert(table_name, rows, batch_size=500):
    """Insert rows in safe 500-row chunks."""
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        supabase.table(table_name).upsert(batch).execute()


# ============================================================
# PROCESSORS
# ============================================================

def process_5m():
    print("→ Processing 5m...")

    last_ts = get_checkpoint("last_minute_ts")
    rows = fetch_minute_chunk(last_ts)

    if not rows:
        print("No new minute data.")
        return False

    agg = aggregate_rows(rows, 5)
    batch_upsert("ohlc_5m", agg)

    last_processed = datetime.fromisoformat(rows[-1]["timestamp"].replace("Z", "+00:00"))
    set_checkpoint("last_minute_ts", last_processed)

    print(f"Inserted {len(agg)} 5m candles.")
    return True


def process_15m():
    print("→ Processing 15m...")

    last_ts = get_checkpoint("last_5m_ts")
    rows = fetch_5m_chunk(last_ts)

    if not rows:
        print("No new data for 15m.")
        return False

    agg = aggregate_rows(rows, 15)
    batch_upsert("ohlc_15m", agg)

    last_processed = datetime.fromisoformat(rows[-1]["ts_bucket"].replace("Z", "+00:00"))
    set_checkpoint("last_5m_ts", last_processed)

    print(f"Inserted {len(agg)} 15m candles.")
    return True


def process_1h():
    print("→ Processing 1h...")

    last_ts = get_checkpoint("last_15m_ts")
    rows = fetch_5m_chunk(last_ts)

    if not rows:
        print("No new data for 1h.")
        return False

    agg = aggregate_rows(rows, 60)
    batch_upsert("ohlc_1h", agg)

    last_processed = datetime.fromisoformat(rows[-1]["ts_bucket"].replace("Z", "+00:00"))
    set_checkpoint("last_15m_ts", last_processed)

    print(f"Inserted {len(agg)} 1h candles.")
    return True


def process_4h():
    print("→ Processing 4h...")

    last_ts = get_checkpoint("last_1h_ts")
    rows = fetch_1h_chunk(last_ts)

    if not rows:
        print("No new 1h data.")
        return False

    agg = aggregate_rows(rows, 240)
    batch_upsert("ohlc_4h", agg)

    last_processed = datetime.fromisoformat(rows[-1]["ts_bucket"].replace("Z", "+00:00"))
    set_checkpoint("last_1h_ts", last_processed)

    print(f"Inserted {len(agg)} 4h candles.")
    return True


def process_1d():
    print("→ Processing 1d...")

    last_ts = get_checkpoint("last_4h_ts")
    rows = fetch_1h_chunk(last_ts)

    if not rows:
        print("No new 1h data for daily.")
        return False

    agg = aggregate_rows(rows, 1440)
    batch_upsert("ohlc_1d", agg)

    last_processed = datetime.fromisoformat(rows[-1]["ts_bucket"].replace("Z", "+00:00"))
    set_checkpoint("last_4h_ts", last_processed)

    print(f"Inserted {len(agg)} 1d candles.")
    return True


# ============================================================
# MAIN LOOP
# ============================================================

print("🔥 Aggregation Worker Started")

while True:
    try:
        print("\n==========================================")
        print("🚀 Aggregation Cycle Starting")
        print("==========================================")

        if process_5m():
            process_15m()
            process_1h()
            process_4h()
            process_1d()

        print("⏳ Sleeping 60s...")
        time.sleep(60)

    except Exception as e:
        print("❌ ERROR:", e)
        print("Retrying in 15 seconds...\n")
        time.sleep(15)
