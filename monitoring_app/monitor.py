"""CDC Monitor — Airflow-style logs to logs/ folder + stdout."""
import json, os, sys, time
from datetime import datetime, timezone

import docker
import psycopg2

CFG = json.load(open("config.json"))
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def _conn(host):
    return f"host={host} port={CFG['PG_PORT']} dbname={CFG['PG_DB']} user={CFG['PG_USER']} password={CFG['PG_PASS']}"

SQL_TABLES = "SELECT relname, n_tup_ins FROM pg_stat_user_tables ORDER BY relname;"
SQL_SUB    = "SELECT subname, received_lsn, last_msg_send_time FROM pg_stat_subscription;"
SQL_LAG    = "SELECT COALESCE(EXTRACT(epoch FROM replay_lag)::int, 0) AS lag_sec FROM pg_stat_replication WHERE application_name = 'cdc_sub';"
SQL_LAST   = "SELECT MAX(ts) FROM {};"

def _airflow_ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + ",000"

def _log(msg, fh):
    line = f"[{_airflow_ts()}] {{monitor.py}} INFO - {msg}"
    fh.write(line + "\n")
    fh.flush()
    sys.stdout.write(line + "\n")
    sys.stdout.flush()

def _hr(b):
    if b >= 1_000_000_000: return f"{b/1e9:.1f}GB"
    if b >= 1_000_000:      return f"{b/1e6:.1f}MB"
    if b >= 1_000:          return f"{b/1e3:.0f}kB"
    return f"{b}B"

def _hr_rate(bps):
    if bps >= 1_000_000: return f"{bps/1e6:.1f}MB/s"
    if bps >= 1_000:     return f"{bps/1e3:.0f}kB/s"
    return f"{bps:.0f}B/s"

def _docker_stats(docker_client):
    """Return dict of {container_name: {cpu, mem_mb, mem_pct, rx, tx}}."""
    result = {}
    for c in docker_client.containers.list():
        try:
            s = c.stats(stream=False)
            name = s["name"].lstrip("/")
            mem_used = s["memory_stats"]["usage"]
            mem_limit = s["memory_stats"]["limit"]
            cpu_delta = s["cpu_stats"]["cpu_usage"]["total_usage"] - \
                        s["precpu_stats"]["cpu_usage"]["total_usage"]
            sys_delta = s["cpu_stats"]["system_cpu_usage"] - \
                        s["precpu_stats"]["system_cpu_usage"]
            num_cpus = len(s["cpu_stats"]["cpu_usage"].get("percpu_usage", [1])) or 1
            rx = tx = 0
            for iface in s.get("networks", {}).values():
                rx += iface.get("rx_bytes", 0)
                tx += iface.get("tx_bytes", 0)
            result[name] = {
                "cpu": (cpu_delta / sys_delta * num_cpus) * 100 if sys_delta else 0,
                "mem_mb": mem_used / (1024 * 1024),
                "mem_pct": (mem_used / mem_limit * 100) if mem_limit else 0,
                "rx": rx, "tx": tx,
            }
        except Exception as e:
            result[c.name] = {"err": str(e)}
    return result

def main():
    for h in (CFG["PG_HOST_PROD"], CFG["PG_HOST_REPL"]):
        for _ in range(30):
            try:
                psycopg2.connect(_conn(h)).close()
                break
            except psycopg2.OperationalError:
                time.sleep(2)

    prod = psycopg2.connect(_conn(CFG["PG_HOST_PROD"]))
    prod.autocommit = True
    repl = psycopg2.connect(_conn(CFG["PG_HOST_REPL"]))
    repl.autocommit = True
    prev_p = {}
    prev_net = {}

    dkr = docker.from_env()

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    fh = open(os.path.join(LOG_DIR, f"cdc_{ts}.log"), "w")
    tick = time.monotonic()

    _log(f"CDC Monitor started | refresh={CFG['INTERVAL']}s  log_rotate={CFG['LOG_INTERVAL']}s", fh)

    while True:
        pcur = prod.cursor()
        rcur = repl.cursor()
        pcur.execute(SQL_TABLES)
        rcur.execute(SQL_TABLES)
        p_data = dict(pcur.fetchall())
        r_data = dict(rcur.fetchall())

        rcur.execute(SQL_SUB)
        subs = rcur.fetchall()
        sub_active = "YES" if subs and subs[0][1] else "NO"
        sub_last   = str(subs[0][2]) if subs else "-"

        # Time-based replication lag (seconds behind)
        pcur.execute(SQL_LAG)
        lag_row = pcur.fetchone()
        lag_sec = lag_row[0] if lag_row else 0

        _log("─" * 60, fh)
        _log(f"Subscription | active={sub_active} | last_msg={sub_last} | lag={lag_sec}s", fh)
        for tbl in sorted(set(p_data) | set(r_data)):
            p = p_data.get(tbl, 0)
            r = r_data.get(tbl, 0)
            ins_s = (p - prev_p.get(tbl, p)) / CFG["INTERVAL"]
            prev_p[tbl] = p

            # Last update timestamps + delay in ms
            pcur.execute(SQL_LAST.format(tbl))
            last_prod = pcur.fetchone()[0]
            rcur.execute(SQL_LAST.format(tbl))
            last_repl = rcur.fetchone()[0]
            lp = last_prod.strftime("%H:%M:%S") if last_prod else "-"
            rp = last_repl.strftime("%H:%M:%S") if last_repl else "-"
            lag_ms = ""
            if last_prod and last_repl:
                delta = int((last_prod - last_repl).total_seconds() * 1000)
                lag_ms = f"lag={delta}ms"

            _log(f"{tbl:<25} prod={p:>12,}  repl={r:>12,}  last_prod={lp}  last_repl={rp}  {lag_ms}", fh)

        # Docker container stats — show rate + cumulative
        d_stats = _docker_stats(dkr)
        for name in ("production", "read_replica", "cdc-producer-1"):
            s = d_stats.get(name)
            if not s:
                continue
            if "err" in s:
                _log(f"  docker/{name:<20} err={s['err']}", fh)
                continue
            prev = prev_net.get(name, {"rx": s["rx"], "tx": s["tx"]})
            rx_s = (s["rx"] - prev["rx"]) / CFG["INTERVAL"]
            tx_s = (s["tx"] - prev["tx"]) / CFG["INTERVAL"]
            prev_net[name] = {"rx": s["rx"], "tx": s["tx"]}
            _log(f"  docker/{name:<20} CPU={s['cpu']:.1f}%  MEM={s['mem_mb']:.0f}MB/{s['mem_pct']:.1f}%  NET rx={_hr_rate(rx_s)} tx={_hr_rate(tx_s)}  (total rx={_hr(s['rx'])} tx={_hr(s['tx'])})", fh)

        _log("─" * 60, fh)

        if time.monotonic() - tick >= CFG["LOG_INTERVAL"]:
            fh.close()
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
            fh = open(os.path.join(LOG_DIR, f"cdc_{ts}.log"), "w")
            tick = time.monotonic()

        time.sleep(CFG["INTERVAL"])

if __name__ == "__main__":
    main()
