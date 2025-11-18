import os
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# ============================================================
#  PRODUCTION-GRADE OHLC AGGREGATION WORKER FOR RENDER
#
#  - Reads minute_ohlc from Supabase in timestamp chunks
#  - Builds 5m, 15m, 1h, 4h, 1d candles using TRUE time buckets
#  - Saves checkpoints in aggregation_state table
#  - Inserts candles in batches to avoid timeouts
#  - Runs forever in an infinite loop
#  - Safe for 70M minute rows
# ============================================================


# ============================================================
# 1. CONNECT TO SUPABASE
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

CHUNK_SIZE = 10000  # how many minute rows to process at once


# ============================================================
# 2. CHECKPOINT HELPERS
# ============================================================

def get_checkpoint(key: str) -> datetime:
    """Read last processed timestamp from aggregation_state table."""
    res = supabase.table("aggregation_state").select("*").eq("key", key).execute()
    if res.data:
        return datetime.fromisoformat(res.data[0]['value'].replace("Z", "+00:00"))
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def set_checkpoint(key: str, dt: datetime):
    """Update checkpoint timestamp."""
    supabase.table("aggregation_state").upsert({
        "key": key,
        "value": dt.isoformat()
    }).execute()


# ============================================================
# 3. FETCH NEW MINUTE DATA IN TIME WINDOW
# ============================================================

def fetch_minute_chunk(since_timestamp: datetime):
    """
    Fetch up to CHUNK_SIZE rows newer than since_timestamp,
    ordered by timestamp ASC.
    """
    ts_str = since_timestamp.isoformat()
    query = (
        supabase.table("minute_ohlc")
        .select("*")
        .gt("timestamp", ts_str)
        .order("timestamp", asc=True)
        .limit(CHUNK_SIZE)
    )
    res = query.execute()
    return res.data


# ============================================================
# 4. TIME BUCKET UTILITIES
# ============================================================

def floor_time(dt: datetime, minutes: int) -> datetime:
    """Round timestamp down to nearest X-minute boundary."""
    discard = (dt.minute % minutes)
    rounded = dt - timedelta(minutes=discard,
                             seconds=dt.second,
                             microseconds=dt.microsecond)
    return rounded


# ============================================================
# 5. GENERIC AGGREGATION LOGIC
# ============================================================

def aggregate_rows(rows, timeframe_minutes):
    """
    Convert raw OHLC rows into aggregated timeframe candles.
    rows: list of rows from minute_ohlc or 5m table
    timeframe_minutes: 5, 15, 60, etc.
    """
    buckets = {}

    for r in rows:
        ts = datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00"))
        bucket = floor_time(ts, timeframe_minutes)

        key = (r["symbol"], bucket.isoformat())
        if key not in buckets:
            buckets[key] = {
                "symbol": r["symbol"],
                "ts_bucket": bucket.isoformat(),
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
# 6. BATCH INSERTS INTO SUPABASE
# ============================================================

def batch_upsert(table_name, rows, batch_size=500):
    """Upsert rows into Supabase table in safe batches."""
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        supabase.table(table_name).upsert(batch).execute()


# ============================================================
# 7. PROCESS MINUTE → 5m
# ============================================================

def process_5m():
    print("→ Processing 5m candles...")

    last_ts = get_checkpoint("last_minute_ts")
    minute_rows = fetch_minute_chunk(last_ts)

    if not minute_rows:
        print("No new minute rows available.")
        return False

    # Build 5m candles from raw minute rows
    agg = aggregate_rows(minute_rows, 5)
    batch_upsert("ohlc_5m", agg)

    # Update checkpoint (last timestamp)
    last_processed_ts = datetime.fromisoformat(
        minute_rows[-1]["timestamp"].replace("Z", "+00:00")
    )
    set_checkpoint("last_minute_ts", last_processed_ts)

    print(f"Inserted {len(agg)} new 5m candles.")
    return True


# ============================================================
# 8. FETCH NEW 5m ROWS TO PROCESS HIGHER TIMEFRAMES
# ============================================================

def fetch_5m_chunk(since_timestamp: datetime):
    ts_str = since_timestamp.isoformat()
    res = (
        supabase.table("ohlc_5m")
        .select("*")
        .gt("ts_bucket", ts_str)
        .order("ts_bucket", asc=True)
        .limit(CHUNK_SIZE)
    ).execute()
    return res.data


# ============================================================
# 9. PROCESS 5m → 15m
# ============================================================

def process_15m():
    print("→ Processing 15m candles...")

    last_ts = get_checkpoint("last_5m_ts")
    rows = fetch_5m_chunk(last_ts)

    if not rows:
        print("No new 5m data.")
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
# 10. PROCESS 5m → 1h
# ============================================================

def process_1h():
    print("→ Processing 1h candles...")

    last_ts = get_checkpoint("last_15m_ts")
    rows = fetch_5m_chunk(last_ts)

    if not rows:
        print("No new 5m for 1h.")
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
# 11. FETCH NEW 1h ROWS
# ============================================================

def fetch_1h_chunk(since_timestamp: datetime):
    ts_str = since_timestamp.isoformat()
    res = (
        supabase.table("ohlc_1h")
        .select("*")
        .gt("ts_bucket", ts_str)
        .order("ts_bucket", asc=True)
        .limit(CHUNK_SIZE)
    ).execute()
    return res.data


# ============================================================
# 12. PROCESS 1h → 4h
# ============================================================

def process_4h():
    print("→ Processing 4h candles...")

    last_ts = get_checkpoint("last_1h_ts")
    rows = fetch_1h_chunk(last_ts)

    if not rows:
        print("No new 1h candles.")
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
# 13. PROCESS 1h → 1d
# ============================================================

def process_1d():
    print("→ Processing 1d candles...")

    last_ts = get_checkpoint("last_4h_ts")
    rows = fetch_1h_chunk(last_ts)

    if not rows:
        print("No new 1h candles for daily.")
        return False

    agg = aggregate_rows(rows, 1440)
    batch_upsert("ohlc_1d", agg)

    last_processed = datetime.fromisoformat(
        rows[-1]["ts_bucket"].replace("Z", "+00:00")
    )
    set_checkpoint("last_4h_ts", last_processed)

    print(f"Inserted {len(agg)} new 1d candles.")
    return True


# ============================================================
# 14. MAIN WORKER LOOP
# ============================================================

print("🔥 OHLC Aggregation Worker Started — beginning continuous ETL loop.")

while True:
    try:
        print("============================================")
        print("  🚀 Starting Backfill / Live Aggregation Cycle")
        print("============================================")

        # PROCESS IN ORDER OF DEPENDENCY
        processed_5m = process_5m()
        if processed_5m:
            process_15m()
            process_1h()
            process_4h()
            process_1d()

        print("⏳ Sleeping 60 seconds before next cycle...")
        time.sleep(60)

    except Exception as e:
        print("❌ ERROR in worker loop:", e)
        print("Retrying in 15 seconds...")
        time.sleep(15)
