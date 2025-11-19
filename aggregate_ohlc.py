import os
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

CHUNK_SIZE = 10000


def get_checkpoint(key: str) -> datetime:
    res = supabase.table("aggregation_state").select("*").eq("key", key).execute()
    if res.data:
        return datetime.fromisoformat(res.data[0]['value'].replace("Z", "+00:00"))
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def set_checkpoint(key: str, dt: datetime):
    supabase.table("aggregation_state").upsert({
        "key": key,
        "value": dt.isoformat()
    }).execute()


def fetch_minute_chunk(since_timestamp: datetime):
    ts_str = since_timestamp.isoformat()
    res = (
        supabase.table("minute_ohlcv")
        .select("*")
        .gt("ts", ts_str)
        .order("ts")
        .limit(CHUNK_SIZE)
        .execute()
    )
    return res.data


def floor_time(dt: datetime, minutes: int) -> datetime:
    discard = (dt.minute % minutes)
    return dt - timedelta(minutes=discard, seconds=dt.second, microseconds=dt.microsecond)


def aggregate_rows(rows, timeframe_minutes):
    buckets = {}

    for r in rows:
        ts = datetime.fromisoformat(r["ts_bucket"].replace("Z", "+00:00"))
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


def batch_upsert(table_name, rows, batch_size=500):
    for i in range(0, len(rows), batch_size):
        supabase.table(table_name).upsert(rows[i:i+batch_size]).execute()


def process_5m():
    print("→ Processing 5m...")

    last_ts = get_checkpoint("last_minute_ts")
    rows = fetch_minute_chunk(last_ts)

    if not rows:
        print("No new minute rows")
        return False

    # convert minute timestamp → ts_bucket
    for r in rows:
        r["ts_bucket"] = r["ts"]

    agg = aggregate_rows(rows, 5)
    batch_upsert("ohlc_5m", agg)

    set_checkpoint("last_minute_ts", datetime.fromisoformat(rows[-1]["ts"].replace("Z", "+00:00")))
    print(f"Inserted {len(agg)} 5m candles.")
    return True


def fetch_5m_chunk(since_timestamp: datetime):
    ts_str = since_timestamp.isoformat()
    res = (
        supabase.table("ohlc_5m")
        .select("*")
        .gt("ts_bucket", ts_str)
        .order("ts_bucket")
        .limit(CHUNK_SIZE)
        .execute()
    )
    return res.data


def process_15m():
    print("→ Processing 15m...")

    last_ts = get_checkpoint("last_5m_ts")
    rows = fetch_5m_chunk(last_ts)

    if not rows:
        print("No new 5m rows")
        return False

    agg = aggregate_rows(rows, 15)
    batch_upsert("ohlc_15m", agg)

    # FIXED HERE
    set_checkpoint("last_5m_ts", datetime.fromisoformat(rows[-1]["ts_bucket"].replace("Z","+00:00")))
    print(f"Inserted {len(agg)} 15m candles.")
    return True


def process_1h():
    print("→ Processing 1h...")

    last_ts = get_checkpoint("last_15m_ts")
    rows = fetch_5m_chunk(last_ts)

    if not rows:
        return False

    agg = aggregate_rows(rows, 60)
    batch_upsert("ohlc_1h", agg)

    # FIXED HERE
    set_checkpoint("last_15m_ts", datetime.fromisoformat(rows[-1]["ts_bucket"].replace("Z","+00:00")))
    print(f"Inserted {len(agg)} 1h candles.")
    return True


def fetch_1h_chunk(since_timestamp: datetime):
    ts_str = since_timestamp.isoformat()
    res = (
        supabase.table("ohlc_1h")
        .select("*")
        .gt("ts_bucket", ts_str)
        .order("ts_bucket")
        .limit(CHUNK_SIZE)
        .execute()
    )
    return res.data


def process_4h():
    print("→ Processing 4h...")

    last_ts = get_checkpoint("last_1h_ts")
    rows = fetch_1h_chunk(last_ts)

    if not rows:
        return False

    agg = aggregate_rows(rows, 240)
    batch_upsert("ohlc_4h", agg)

    # FIXED HERE
    set_checkpoint("last_1h_ts", datetime.fromisoformat(rows[-1]["ts_bucket"].replace("Z","+00:00")))
    print(f"Inserted {len(agg)} 4h candles.")
    return True


def process_1d():
    print("→ Processing 1d...")

    last_ts = get_checkpoint("last_4h_ts")
    rows = fetch_1h_chunk(last_ts)

    if not rows:
        return False

    agg = aggregate_rows(rows, 1440)
    batch_upsert("ohlc_1d", agg)

    # FIXED HERE
    set_checkpoint("last_4h_ts", datetime.fromisoformat(rows[-1]["ts_bucket"].replace("Z","+00:00")))
    print(f"Inserted {len(agg)} 1d candles.")
    return True


print("🔥 Aggregation worker started")

while True:
    try:
        print("============================================")
        print("🚀 Aggregation cycle starting")
        print("============================================")

        if process_5m():
            process_15m()
            process_1h()
            process_4h()
            process_1d()

        time.sleep(60)

    except Exception as e:
        print("❌ ERROR:", e)
        time.sleep(15)
