import os
import time
import json
import sqlite3
import paramiko
import argparse
import time
import json
import sqlite3
import paramiko
from datetime import datetime, timezone
from dotenv import load_dotenv

import metrics_collector
import ml_anomaly_detector

load_dotenv()

HOST     = os.getenv("VM_SSH_HOST", "127.0.0.1")
PORT     = int(os.getenv("VM_SSH_PORT", "2222"))
USER     = os.getenv("VM_SSH_USER", "vboxuser")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.getenv("DB_PATH", os.path.join(ROOT_DIR, "ceph_monitor.db"))


def sudo_exec(ssh, cmd):
    """Synchronous sudo command with exit-status verification."""
    stdin, stdout, stderr = ssh.exec_command(f"sudo -S {cmd}")
    stdin.write(PASSWORD + '\n')
    stdin.flush()
    exit_code = stdout.channel.recv_exit_status()
    err = stderr.read().decode().strip()
    if exit_code != 0 and "no process found" not in err and "pkill" not in cmd:
        print(f"[WARN] sudo failed (exit {exit_code}): {cmd} | {err}", flush=True)
    return stdout.read().decode().strip()


def run_bg_user(ssh, cmd):
    """Non-blocking background process launched as regular user (no sudo hang)."""
    ssh.exec_command(f"nohup bash -c '{cmd}' > /dev/null 2>&1 &")


def verify_process_started(ssh, pattern):
    """Confirm fault process actually spawned on the VM before snapshotting."""
    time.sleep(1.0)
    _, out, _ = ssh.exec_command(f"pgrep -f '{pattern}'")
    pids = out.read().decode().strip().split()
    if not pids:
        print(f"[WARN] Fault process '{pattern}' did not start!", flush=True)
        return []
    print(f"  [VERIFIED] Fault PIDs: {' '.join(pids)}", flush=True)
    return pids


def take_snapshot_and_save(ssh, is_baseline=False):
    """
    Scrapes live Netdata + Ceph metrics and commits to SQLite.
    is_baseline=True tags the row so the ML model uses it for training only.
    """
    scraped = metrics_collector.collect_snapshot(ssh)
    if scraped:
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO metrics_timeseries (timestamp, data, is_baseline) VALUES (?, ?, ?)",
            (ts, json.dumps(scraped), 1 if is_baseline else 0)
        )
        conn.commit()
        conn.close()
        return ts
    return None


def get_dynamic_ram_target_mib(ssh):
    """Calculate 45% of VM total RAM dynamically so scenario stays meaningful across VM sizes."""
    _, out, _ = ssh.exec_command("free -m | awk '/^Mem:/ {print $2}'")
    try:
        total = int(out.read().decode().strip())
        target = int(total * 0.45)
        print(f"[CONFIG] VM RAM: {total} MiB | Drain target (45%): {target} MiB", flush=True)
        return max(target, 400)
    except Exception as e:
        print(f"[WARN] Could not read VM RAM ({e}), using 800 MiB fallback.", flush=True)
        return 800


def run_standardized_test_window(ssh, label="FAULT EVALUATION", n=3, interval=2.0):
    """Standardized snapshot cadence: n snapshots taken interval seconds apart."""
    print(f"Executing standardized {label} window ({n} snapshots, {interval}s apart)...", flush=True)
    last_res = None
    for idx in range(1, n + 1):
        time.sleep(interval)
        take_snapshot_and_save(ssh, is_baseline=False)
        last_res = ml_anomaly_detector.detect_anomalies()
        anom = last_res.get("is_anomaly", False) if last_res else False
        tag = "ANOMALY" if anom else "NORMAL"
        score = last_res.get("decision_score") if last_res else "N/A"
        print(f"  -> [{idx}/{n}] [{tag}] IF Score: {score}", flush=True)
    return last_res


def print_header(title):
    print("\n" + "=" * 80)
    print(f"  [STAGE]: {title}")
    print("=" * 80, flush=True)


def print_result(label, res):
    print(f"\n[AI CONSENSUS RESULT — {label}]:", flush=True)
    print(json.dumps(res, indent=2), flush=True)
    if res and res.get("sentinel_alerts"):
        print(f"[SENTINEL ALERTS]: {res['sentinel_alerts']}", flush=True)
def run_scenario(ssh, name, setup_func, cleanup_func, trials, expected_anom=True):
    print_header(f"{name} ({trials} Trials)")
    tps = 0
    latencies = []
    
    for t in range(1, trials + 1):
        print(f"\n  [Trial {t}/{trials}] Injecting fault...", flush=True)
        pids = setup_func()
        start_ts = time.time()
        
        # Poll up to 5 times (10s window) for detection
        detected = False
        for step in range(1, 6):
            time.sleep(2.0)
            take_snapshot_and_save(ssh, is_baseline=False)
            res = ml_anomaly_detector.detect_anomalies()
            if res and res.get("is_anomaly"):
                lat = time.time() - start_ts
                latencies.append(lat)
                print(f"    -> Detected at step {step} (Lat: {lat:.1f}s)")
                detected = True
                break
                
        if not detected:
            print(f"    -> Not detected in window.")
            
        if detected == expected_anom:
            tps += 1
            
        print("  [Cooldown] Cleaning up and waiting 5s...", flush=True)
        cleanup_func(pids)
        time.sleep(5)
        take_snapshot_and_save(ssh, is_baseline=False)
        
    return tps, latencies

def main(trials):
    print_header("INITIALIZING VM SSH & CLEANING TEST ENVIRONMENT")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=HOST, port=PORT, username=USER, password=PASSWORD, timeout=5)

    try:
        sudo_exec(ssh, "killall -9 sha256sum dd 2>/dev/null; pkill -9 -f ram_drain 2>/dev/null; rm -f /tmp/io_* /tmp/ram_drain*.py 2>/dev/null || true")
        time.sleep(2)

        # ── PHASE 1: HOLD-OUT VALIDATION (Healthy) ────────────────────────────────
        # We assume baseline_injector.py was run to build the model.
        print_header(f"PHASE 1: HEALTHY HOLD-OUT ({trials} Trials)")
        fps = 0
        for t in range(1, trials + 1):
            time.sleep(2.0)
            take_snapshot_and_save(ssh, is_baseline=False)
            res = ml_anomaly_detector.detect_anomalies()
            if res and res.get("is_anomaly"):
                fps += 1
                print(f"  [Trial {t}] False Positive Detected!")
            else:
                print(f"  [Trial {t}] Clean (TN)")
        tn = trials - fps

        # ── SCENARIO 1: CPU THRASHING ─────────────────────────────────────────────
        def setup_s1():
            run_bg_user(ssh, "for i in $(seq 1 8); do sha256sum /dev/zero > /dev/null 2>&1 & done")
            return verify_process_started(ssh, "sha256sum")
        def clean_s1(pids):
            if pids:
                sudo_exec(ssh, f"kill -9 {' '.join(pids)} 2>/dev/null || true")
            
        tp1, lat1 = run_scenario(ssh, "SCENARIO 1: CPU THRASHING", setup_s1, clean_s1, trials)

        # ── SCENARIO 2: DYNAMIC RAM STARVATION ───────────────────────────────────
        ram_target = get_dynamic_ram_target_mib(ssh)
        def setup_s2():
            ram_script = f"import time\nbuf = bytearray({ram_target} * 1024 * 1024)\ntime.sleep(30)\n"
            sftp = ssh.open_sftp()
            with sftp.file("/tmp/ram_drain.py", "w") as f:
                f.write(ram_script)
            sftp.close()
            run_bg_user(ssh, "python3 /tmp/ram_drain.py")
            return verify_process_started(ssh, "ram_drain.py")
        def clean_s2(pids):
            if pids:
                sudo_exec(ssh, f"kill -9 {' '.join(pids)} 2>/dev/null || true")
            sudo_exec(ssh, "rm -f /tmp/ram_drain.py 2>/dev/null || true")
            
        tp2, lat2 = run_scenario(ssh, "SCENARIO 2: RAM STARVATION", setup_s2, clean_s2, trials)

        # ── SCENARIO 3: PURE STORAGE I/O SATURATION ──────────────────────────────
        def setup_s3():
            run_bg_user(ssh, "dd if=/dev/zero of=/tmp/io_test bs=10M count=100 oflag=dsync")
            return verify_process_started(ssh, "dd if=/dev/zero")
        def clean_s3(pids):
            if pids:
                sudo_exec(ssh, f"kill -9 {' '.join(pids)} 2>/dev/null || true")
            sudo_exec(ssh, "rm -f /tmp/io_test 2>/dev/null || true")
            
        tp3, lat3 = run_scenario(ssh, "SCENARIO 3: STORAGE I/O SATURATION", setup_s3, clean_s3, trials)

        # ── CONFUSION MATRIX SUMMARY ──────────────────────────────────────────────
        total_faults = 3 * trials
        total_detected = tp1 + tp2 + tp3
        
        all_lats = lat1 + lat2 + lat3
        med_lat = round(sum(all_lats)/len(all_lats), 1) if all_lats else "N/A"
        
        print_header("FINAL CONFUSION MATRIX & REPORT")
        print(f"Total Healthy Trials : {trials}")
        print(f"  True Negatives (TN): {tn}")
        print(f"  False Positives(FP): {fps}")
        print(f"\nTotal Fault Trials   : {total_faults}")
        print(f"  True Positives (TP): {total_detected}")
        print(f"  False Negatives(FN): {total_faults - total_detected}")
        print(f"\nMedian Detection Latency: {med_lat} seconds")
        print("======================================================================\n", flush=True)

    finally:
        print("\n[CLEANUP] Final safety sweep — removing all stress processes from VM...", flush=True)
        sudo_exec(ssh, "killall -9 sha256sum dd 2>/dev/null; pkill -9 -f 'ram_drain' 2>/dev/null; rm -f /tmp/io_* /tmp/ram_drain*.py 2>/dev/null || true")
        ssh.close()
        print("[CLEANUP] SSH closed cleanly.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=3, help="Number of trials per scenario")
    args = parser.parse_args()
    main(args.trials)
