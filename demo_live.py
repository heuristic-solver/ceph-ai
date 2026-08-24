"""
demo_live.py  --  Ceph AI Monitoring Live Dashboard
====================================================
Keep this running in one terminal window during the demo.
It polls the ML models every 2 seconds and prints a live status.

When a fault is injected (via demo_inject.py in another terminal),
this window will immediately show the anomaly flag.

Usage:
    python demo_live.py
    python demo_live.py --fast        # 1-second updates
"""

import os, sys, time, json, sqlite3, argparse
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
import paramiko
from datetime import datetime, timezone
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import metrics_collector
import ml_anomaly_detector
import diagnostic_engine
import llm_analyst
try:
    import ceph_semantic_baseline
    V8_AVAILABLE = True
except Exception:
    V8_AVAILABLE = False

load_dotenv(dotenv_path=os.path.join(ROOT, ".env"))

HOST     = os.getenv("VM_SSH_HOST", "127.0.0.1")
PORT     = int(os.getenv("VM_SSH_PORT", "2222"))
USER     = os.getenv("VM_SSH_USER", "vboxuser")
PASSWORD = os.getenv("VM_SSH_PASSWORD", "admin")
DB_PATH  = os.getenv("DB_PATH", os.path.join(ROOT, "ceph_monitor.db"))

# ANSI Colors
RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
RED="\033[91m"; GREEN="\033[92m"; YELLOW="\033[93m"
BLUE="\033[94m"; MAGENTA="\033[95m"; CYAN="\033[96m"; WHITE="\033[97m"
BG_RED="\033[41m"; BG_GREEN="\033[42m"

def clr(text, *codes):
    return "".join(codes) + str(text) + RESET

def now_str():
    return datetime.now().strftime("%H:%M:%S")

def clear_screen():
    print("\033[2J\033[H", end="", flush=True)

BANNER = """
\033[96m\033[1m  +==================================================================+
  |          CEPH AI AUTONOMOUS MONITORING SYSTEM                    |
  |          v7 Host Layer  +  v8 Ceph Semantic Layer                |
  +==================================================================+\033[0m
"""
SEP = "\033[2m  " + "-" * 68 + "\033[0m"

_ssh = None

def ensure_ssh():
    global _ssh
    try:
        if _ssh and _ssh.get_transport() and _ssh.get_transport().is_active():
            return _ssh
    except Exception:
        pass
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=HOST, port=PORT, username=USER, password=PASSWORD, timeout=6)
        _ssh = ssh
    except Exception:
        _ssh = None
    return _ssh

def format_metric(label, value, unit="", warn_above=None):
    if isinstance(value, float):
        display = f"{value:>8.2f} {unit}"
    else:
        display = f"{str(value):>10} {unit}"
    if warn_above is not None and isinstance(value, (int, float)):
        color = RED if value > warn_above else GREEN
    else:
        color = WHITE
    return f"  {clr(label+':', DIM):<40} {clr(display, color, BOLD)}"

def run_demo(interval=2.0, max_ticks=0):
    tick = 0
    history = []
    last_anomaly = None
    llm_cache = None
    llm_cache_ts = 0

    print(BANNER)
    print(clr("  Connecting to VM and initialising models...", DIM))
    ensure_ssh()
    vm_status = "CONNECTED" if _ssh else "OFFLINE (Netdata-only mode)"
    print(clr(f"  VM SSH: {vm_status}", GREEN if _ssh else YELLOW))
    print(clr("  Starting live monitor. Press Ctrl+C to stop.\n", DIM))
    time.sleep(1.5)

    while True:
        try:
            tick += 1
            ts_start = time.time()

            ssh = ensure_ssh()
            snap = metrics_collector.collect_snapshot(ssh)

            if snap:
                ts_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                try:
                    conn = sqlite3.connect(DB_PATH)
                    conn.execute(
                        "INSERT INTO metrics_timeseries (timestamp, data, is_baseline) VALUES (?, ?, 0)",
                        (ts_str, json.dumps(snap))
                    )
                    conn.execute(
                        "DELETE FROM metrics_timeseries WHERE is_baseline=0 AND timestamp < datetime('now', '-7 days')"
                    )
                    conn.commit()
                    conn.close()
                except Exception:
                    pass

            v7_result = ml_anomaly_detector.detect_anomalies()

            v8_result = None
            if V8_AVAILABLE:
                try:
                    v8_result = ceph_semantic_baseline.detect_anomalies()
                except Exception:
                    pass

            # Raw layer status
            v7_anom = bool(v7_result and v7_result.get("is_anomaly"))
            v8_anom = bool(v8_result and v8_result.get("is_anomaly"))
            raw_anom = v7_anom or v8_anom

            history.append(raw_anom)
            if len(history) > 8:
                history.pop(0)

            # Require at least 2 consecutive positive ticks or multi-layer consensus to confirm active anomaly
            consec_count = 0
            for h in reversed(history):
                if h:
                    consec_count += 1
                else:
                    break
            is_anom = (consec_count >= 2) or (v7_anom and v8_anom)

            # ── RENDER ────────────────────────────────────────────────────
            clear_screen()
            print(BANNER)
            print(f"  {clr('Updated:', DIM)} {clr(now_str(), WHITE, BOLD)}   "
                  f"{clr('Tick #'+str(tick), DIM)}")
            print(SEP)

            # Status bar
            if is_anom:
                print(clr("  [!]  !!! ANOMALY DETECTED  !!! ANOMALY DETECTED  !!!  [!]",
                           BG_RED, WHITE, BOLD))
                print()
                
                # Dynamic Root Cause Analysis
                try:
                    ctx = diagnostic_engine.build_incident_context(v7_result=v7_result, v8_result=v8_result, raw_snapshot=snap)
                    diag = llm_analyst.diagnose_incident(ctx)
                    
                    print(clr("  +------------------------------------------------------------------+", RED, BOLD))
                    print(clr(f"  |  [!] AI ROOT CAUSE INCIDENT DIAGNOSIS  ::  {diag.get('incident_id', 'INC')}   |", RED, BOLD))
                    print(clr("  +------------------------------------------------------------------+", RED, BOLD))
                    print(f"  {clr('>> ROOT CAUSE:', RED, BOLD)} {clr(diag.get('root_cause_summary', 'Anomaly detected'), WHITE, BOLD)}")
                    print(f"     {clr('Category:', DIM)} {clr(diag.get('fault_category', 'UNKNOWN'), YELLOW, BOLD)}  "
                          f"{clr('Severity:', DIM)} {clr(diag.get('severity', 'WARNING'), RED, BOLD)}  "
                          f"{clr('Source:', DIM)} {clr(diag.get('source', 'AI Engine'), CYAN)}")
                    if diag.get("detailed_explanation"):
                        print(f"     {clr(diag.get('detailed_explanation'), DIM)}")
                    if diag.get("blast_radius"):
                        print(f"  {clr('>> BLAST RADIUS:', MAGENTA, BOLD)} {diag.get('blast_radius')}")
                    if diag.get("remediation_steps"):
                        print(f"  {clr('>> REMEDIATION RUNBOOK:', GREEN, BOLD)}")
                        for s_idx, step in enumerate(diag.get("remediation_steps", []), 1):
                            print(f"     {clr(str(s_idx)+'.', GREEN, BOLD)} {clr(step, WHITE)}")
                    if diag.get("verification_command"):
                        print(f"  {clr('>> VERIFICATION:', CYAN, BOLD)} {clr(diag.get('verification_command'), CYAN)}")
                    print(clr("  +------------------------------------------------------------------+", RED, BOLD))
                except Exception as ex:
                    print(clr(f"  [RCA Diagnostics Error: {ex}]", YELLOW))
            else:
                print(clr("  [OK]  SYSTEM NORMAL  --  AI models show no anomalous activity",
                           BG_GREEN, BOLD))
            print()

            # V7
            v7_tag = clr("[  OK   ]", GREEN, BOLD) if not v7_anom else clr("[ANOMALY]", RED, BOLD)
            print(f"  {clr('HOST LAYER v7 (IsolationForest + SVM + PCA)', CYAN, BOLD)}  {v7_tag}")
            if v7_result:
                sc = v7_result.get("decision_score", "?")
                pc = v7_result.get("pca_reconstruct", "?")
                sc_col = RED if isinstance(sc, float) and sc < 0 else GREEN
                print(f"    IF Score: {clr(str(sc), sc_col, BOLD)}   PCA Error: {pc}   "
                      f"Phase: {clr(v7_result.get('phase','?'), YELLOW)}")
                if v7_anom:
                    for feat, vals in list(v7_result.get("deviated_features", {}).items())[:3]:
                        cur = vals.get("current", "?")
                        bsl = vals.get("baseline_mean", "?")
                        z   = vals.get("z_score", "?")
                        print(f"    {clr('=> DEVIATED:', RED, BOLD)} {feat} "
                              f"[now={clr(str(round(cur,3)), RED, BOLD)}  baseline={round(bsl,3)}  z={z}]")
                    for sa in v7_result.get("sentinel_alerts", [])[:2]:
                        print(f"    {clr('SENTINEL ALERT:', YELLOW, BOLD)} {sa.get('alert','')}")
            print()

            # V8
            if V8_AVAILABLE:
                v8_tag = clr("[  OK   ]", GREEN, BOLD) if not v8_anom else clr("[ANOMALY]", RED, BOLD)
                print(f"  {clr('CEPH SEMANTIC LAYER v8 (OSD Latency + PG + BlueStore)', CYAN, BOLD)}  {v8_tag}")
                if v8_result:
                    sc8 = v8_result.get("decision_score", "?")
                    pe8 = v8_result.get("pca_reconstruct_err", "?")
                    trg = v8_result.get("triggered_models", [])
                    sc8_col = RED if isinstance(sc8, float) and sc8 < 0 else GREEN
                    print(f"    IF Score: {clr(str(sc8), sc8_col, BOLD)}   PCA Error: {pe8}")
                    if v8_anom:
                        if trg:
                            print(f"    {clr('Triggered models:', RED, BOLD)} {trg}")
                        for feat, vals in list(v8_result.get("deviated_features", {}).items())[:3]:
                            cur = vals.get("current", "?")
                            bsl = vals.get("baseline_mean", "?")
                            print(f"    {clr('=> DEVIATED:', RED, BOLD)} {feat} "
                                  f"[now={clr(str(round(cur,3)), RED, BOLD)}  baseline={round(bsl,3)}]")
                print()

            # Live metrics
            print(SEP)
            print(f"  {clr('KEY METRICS (live)', CYAN, BOLD)}")
            m = snap or {}
            print(format_metric("CPU iowait %",      m.get("system_cpu__iowait", 0.0),    "%",   warn_above=10.0))
            print(format_metric("Ceph daemon CPU %",  m.get("apps_cpu__ceph", 0.0),         "%",   warn_above=15.0))
            print(format_metric("Ceph daemon RAM",    m.get("apps_mem__ceph", 0.0),         "MiB", warn_above=700.0))
            pg_deg = m.get("ceph_pg_degraded", 0.0)
            pg_col = RED if pg_deg > 0 else GREEN
            health_code = m.get("ceph_health_status", 0.0)
            health_str = {0.0:"HEALTH_OK", 1.0:"HEALTH_WARN", 2.0:"HEALTH_ERR"}.get(health_code, "UNKNOWN")
            h_col = GREEN if health_str == "HEALTH_OK" else (YELLOW if health_str == "HEALTH_WARN" else RED)
            print(f"  {clr('PG Degraded count:', DIM):<40} {clr(f'{int(pg_deg):>8}', pg_col, BOLD)}")
            print(f"  {clr('Cluster Health:', DIM):<40} {clr(f'{health_str:>16}', h_col, BOLD)}")
            print()

            # History bar
            print(SEP)
            hist_str = " ".join([clr("*", RED, BOLD) if h else clr(".", GREEN) for h in history])
            print(f"  {clr('Detection history:', DIM)} [ {hist_str} ]  "
                  f"{clr('. = NORMAL   * = ANOMALY', DIM)}")
            print()

            # Footer
            elapsed = time.time() - ts_start
            sleep_for = max(0.05, interval - elapsed)
            print(SEP)
            print(f"  {clr('Next refresh in:', DIM)} {sleep_for:.1f}s  |  "
                  f"{clr('Inject fault:', WHITE)} python demo_inject.py [cpu|ram|io|osd_down|osd_kill|pg_target|mon_loss]")
            sys.stdout.flush()

            if max_ticks and tick >= max_ticks:
                break

            time.sleep(sleep_for)

        except KeyboardInterrupt:
            print(f"\n\n  {clr('Monitoring stopped.', DIM)}\n")
            break
        except Exception as e:
            print(clr(f"\n  [ERROR] {e}", RED))
            time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ceph AI Live Monitoring Demo")
    parser.add_argument("--fast", action="store_true", help="1-second refresh")
    parser.add_argument("--ticks", type=int, default=0, help="Run N ticks and exit (0 = infinite)")
    args = parser.parse_args()
    run_demo(interval=1.0 if args.fast else 2.0, max_ticks=args.ticks)
