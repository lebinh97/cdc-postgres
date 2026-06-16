"""CDC producer — pushes 5k-10k synthetic user_log rows/sec into the production DB."""
import os, random, time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

ACTS = ["click","page_view","scroll","add_to_cart","purchase","login","logout","search","filter","share"]
MOBILE  = ["iphone","android","ipad"]
DESKTOP = ["mac","windows","linux"]
SCRS = ["home","product page","search results","checkout","account","orders","wishlist","cart","settings","help"]
ALL_DEVS = MOBILE + DESKTOP

import json
BATCH, FREQ = json.load(open("config.json")).values()

def _conn_str():
    return f"host={os.getenv('PG_HOST','localhost')} port={os.getenv('PG_PORT','5433')} " \
           f"dbname={os.getenv('PG_DB','cdc_db')} user={os.getenv('PG_USER','admin')} " \
           f"password={os.getenv('PG_PASS','admin123')}"


def _gen_batch(n: int):
    """Generate *n* rows split by device type. Returns (mobile_rows, desktop_rows)."""
    devs  = random.choices(ALL_DEVS, k=n)
    acts  = random.choices(ACTS, k=n)
    scrs  = random.choices(SCRS, k=n)
    uids  = [random.randint(1, 10000) for _ in range(n)]

    mobile, desktop = [], []
    for i in range(n):
        row = (uids[i], acts[i], devs[i], scrs[i])
        if devs[i] in MOBILE:
            mobile.append(row)
        else:
            desktop.append(row)
    return mobile, desktop


def main():
    for i in range(1, 31):
        try:
            psycopg2.connect(_conn_str()).close()
            break
        except psycopg2.OperationalError:
            print(f"[producer] waiting for DB ({i}/30)")
            time.sleep(2)

    conn = psycopg2.connect(_conn_str())
    cur = conn.cursor()
    rate = BATCH * FREQ
    print(f"[producer] running — {BATCH} rows × {FREQ}/s = ~{BATCH*FREQ} rows/s target")

    total_m = total_d = 0
    tick = time.monotonic()
    while True:
        t0 = time.monotonic()
        mobile_rows, desktop_rows = _gen_batch(BATCH)
        if mobile_rows:
            execute_values(cur, "INSERT INTO user_log_mobile (uid, activity, device, screen) VALUES %s", mobile_rows, page_size=1000)
        if desktop_rows:
            execute_values(cur, "INSERT INTO user_log_desktop (uid, activity, device, screen) VALUES %s", desktop_rows, page_size=1000)
        conn.commit()
        total_m += len(mobile_rows)
        total_d += len(desktop_rows)
        if time.monotonic() - tick >= 5:
            now = datetime.now(timezone.utc).strftime('%H:%M:%S')
            total = total_m + total_d
            print(f"[producer] {now} | {total:,} rows in 5s | {total/5:,.0f} rows/s | mobile={total_m:,}  desktop={total_d:,}")
            total_m = total_d = 0
            tick = time.monotonic()
        time.sleep(max(0, (1.0 / FREQ) - (time.monotonic() - t0)))


if __name__ == "__main__":
    main()
