"""
run_scenario_tests.py
=====================
Modular validator for each of the 7 demo fault scenarios:
1. CPU Thrashing
2. RAM Starvation
3. Storage I/O Saturation
4. OSD Administrative Down
5. OSD Process Kill
6. PG-Level Targeted Fault
7. Monitor Quorum Loss
"""

import os, sys, time, json, sqlite3, paramiko
from datetime import datetime, timezone
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, '.env'))

import metrics_collector
import ml_anomaly_detector
import ceph_semantic_baseline
from ceph_cluster_info import get_cluster_info

HOST     = os.getenv("VM_SSH_HOST", "127.0.0.1")
PORT     = int(os.getenv("VM_SSH_PORT", "2222"))
USER     = os.getenv("VM_SSH_USER", "vboxuser")
PASSWORD = os.getenv("VM_SSH_PASSWORD", "admin")
DB_PATH  = os.getenv("DB_PATH", os.path.join(ROOT, "ceph_monitor.db"))

def get_ssh():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=HOST, port=PORT, username=USER, password=PASSWORD, timeout=6)
    return ssh

def vm_exec(cmd, timeout=20):
    try:
        ssh = get_ssh()
        try:
            stdin, stdout, stderr = ssh.exec_command("sudo -S bash -c \"" + cmd + "\"", timeout=timeout)
            stdin.write(PASSWORD + "\n")
            stdin.flush()
            out = stdout.read().decode("utf-8", errors="ignore").strip()
            return out
        finally:
            ssh.close()
    except Exception:
        return ""

def vm_bg(cmd):
    ssh = get_ssh()
    try:
        ssh.exec_command("nohup bash -c '" + cmd + "' > /dev/null 2>&1 &")
    finally:
        ssh.close()

def tick_system(history, max_hist=8):
    ssh = get_ssh()
    try:
        snap = metrics_collector.collect_snapshot(ssh)
    finally:
        ssh.close()

    if snap:
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute("INSERT INTO metrics_timeseries (timestamp, data, is_baseline) VALUES (?, ?, 0)", (ts, json.dumps(snap)))
            conn.commit()
            conn.close()
        except Exception:
            pass

    v7 = ml_anomaly_detector.detect_anomalies()
    v8 = ceph_semantic_baseline.detect_anomalies()
    
    v7_anom = bool(v7 and v7.get("is_anomaly"))
    v8_anom = bool(v8 and v8.get("is_anomaly"))
    raw_anom = v7_anom or v8_anom
    
    history.append(raw_anom)
    if len(history) > max_hist:
        history.pop(0)
        
    consec = 0
    for h in reversed(history):
        if h: consec += 1
        else: break
    is_anom = (consec >= 2) or (v7_anom and v8_anom)
    
    return {
        "is_anom": is_anom,
        "v7_anom": v7_anom,
        "v8_anom": v8_anom,
        "v7": v7,
        "v8": v8,
        "snap": snap
    }

def wait_for_clean(timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = vm_exec("ceph status --format json 2>/dev/null || echo ''", timeout=8)
        try:
            j = json.loads(out)
            h = j.get("health", {}).get("status", "")
            checks = j.get("health", {}).get("checks", {})
            crit = [k for k in checks if k not in ("POOL_NO_REDUNDANCY", "MON_DISK_LOW", "MON_DISK_CRIT", "OSDMAP_FLAGS", "DEVICE_HEALTH_TOOMANY", "CEPHADM_FAILED_DAEMON")]
            up = j.get("osdmap", {}).get("num_up_osds", 0)
            tot = j.get("osdmap", {}).get("num_osds", 1)
            if (h == "HEALTH_OK" or len(crit) == 0) and up == tot and up > 0:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False

def test_scenario(num, name, start_cmd, stop_cmd, settle_after_stop=6):
    print(f"\n{'='*76}", flush=True)
    print(f"  SCENARIO {num}/7: {name.upper()}", flush=True)
    print(f"{'='*76}", flush=True)
    
    history = []
    
    # 1. Baseline
    print("  [1] PRE-FAULT BASELINE (2 ticks):", flush=True)
    wait_for_clean(30)
    for t in range(1, 3):
        r = tick_system(history)
        tag = "[OK] NORMAL" if not r["is_anom"] else "[!] ANOMALY"
        v7_tag = "OK" if not r["v7_anom"] else "ANOMALY"
        v8_tag = "OK" if not r["v8_anom"] else "ANOMALY"
        print(f"      Tick {t}: Overall={tag:<12} | Host(v7)={v7_tag:<7} Ceph(v8)={v8_tag:<7}", flush=True)
        time.sleep(2)
        
    # 2. Inject
    print(f"  [2] INJECTING FAULT: {name}...", flush=True)
    start_cmd()
    time.sleep(2)
    
    # 3. Anomaly monitoring
    print("  [3] MONITORING WITH FAULT ACTIVE (5 ticks):", flush=True)
    detected = False
    detect_tick = None
    for t in range(1, 6):
        r = tick_system(history)
        tag = "[!] ANOMALY DETECTED" if r["is_anom"] else "[OK] NORMAL"
        v7_tag = "ANOMALY" if r["v7_anom"] else "OK"
        v8_tag = "ANOMALY" if r["v8_anom"] else "OK"
        print(f"      Tick {t}: Overall={tag:<22} | Host(v7)={v7_tag:<7} Ceph(v8)={v8_tag:<7}", flush=True)
        if r["is_anom"] and not detected:
            detected = True
            detect_tick = t
        time.sleep(3)
        
    # 4. Stop
    print("  [4] STOPPING FAULT & CLEANING UP...", flush=True)
    stop_cmd()
    time.sleep(settle_after_stop)
        
    # 5. Recovery
    print("  [5] MONITORING RECOVERY (4 ticks):", flush=True)
    history.clear()
    recovered = False
    for t in range(1, 5):
        r = tick_system(history)
        tag = "[OK] NORMAL" if not r["is_anom"] else "[!] ANOMALY"
        v7_tag = "OK" if not r["v7_anom"] else "ANOMALY"
        v8_tag = "OK" if not r["v8_anom"] else "ANOMALY"
        print(f"      Tick {t}: Overall={tag:<12} | Host(v7)={v7_tag:<7} Ceph(v8)={v8_tag:<7}", flush=True)
        if not r["is_anom"] and not r["v7_anom"] and not r["v8_anom"]:
            recovered = True
        time.sleep(3)
        
    res = "PASS" if (detected and recovered) else ("DETECTED_ONLY" if detected else "FAIL")
    print(f"  >> SCENARIO {num} VERDICT: {res} (Detected={detected}, Recovered={recovered})\n", flush=True)
    return {
        "num": num,
        "name": name,
        "detected": detected,
        "detect_tick": detect_tick,
        "recovered": recovered,
        "verdict": res
    }

def main():
    print("=" * 76)
    print("      CEPH AI DEMO: COMPREHENSIVE 7-FAULT VALIDATION SUITE")
    print("=" * 76)
    
    # Pre-sweep
    vm_exec("killall -9 sha256sum dd python3 2>/dev/null; pkill -9 -f io_demo_test 2>/dev/null; pkill -9 -f ram_drain_demo 2>/dev/null; rm -f /tmp/io_demo_active /tmp/io_demo_test /tmp/ram_drain_demo.py; systemctl restart ceph.target 2>/dev/null; ceph osd in osd.0 2>/dev/null; true")
    wait_for_clean(45)
    time.sleep(3)
    
    results = []
    
    # 1. CPU Thrashing
    def start_cpu(): vm_bg("for i in $(seq 1 8); do sha256sum /dev/zero > /dev/null 2>&1 & done")
    def stop_cpu():
        vm_exec("killall -9 sha256sum 2>/dev/null; true")
    results.append(test_scenario(1, "CPU Thrashing (8x sha256sum)", start_cpu, stop_cpu, settle_after_stop=15))
    
    # 2. RAM Starvation
    def start_ram():
        free_out = vm_exec("free -m | awk 'NR==2{print $7}'")
        try: free_mib = int(free_out.strip())
        except: free_mib = 1500
        target = max(600, free_mib - 250)
        ram_py = f"import time; chunks = [bytearray(1024*1024) for _ in range({target})]; time.sleep(3600)"
        vm_exec(f"echo '{ram_py}' > /tmp/ram_drain_demo.py")
        vm_bg("python3 /tmp/ram_drain_demo.py")
    def stop_ram():
        vm_exec("pkill -9 -f ram_drain_demo 2>/dev/null; rm -f /tmp/ram_drain_demo.py; true")
    results.append(test_scenario(2, "RAM Starvation (Dynamic Allocation)", start_ram, stop_ram, settle_after_stop=6))
    
    # 3. Storage I/O Saturation
    def start_io():
        vm_exec("touch /tmp/io_demo_active")
        vm_bg("while [ -f /tmp/io_demo_active ]; do dd if=/dev/zero of=/tmp/io_demo_test bs=10M count=50 oflag=dsync conv=notrunc 2>/dev/null; done")
    def stop_io():
        vm_exec("rm -f /tmp/io_demo_active; killall -9 dd 2>/dev/null; rm -f /tmp/io_demo_test; true")
    results.append(test_scenario(3, "Storage I/O Saturation (dd write flood)", start_io, stop_io, settle_after_stop=8))
    
    # 4. OSD Administrative Down
    def start_osd_down(): vm_exec(f"ceph osd down osd.0 && ceph osd out osd.0 && systemctl stop '{OSD_SVC}'")
    def stop_osd_down():
        vm_exec("systemctl restart ceph.target; ceph osd in osd.0")
        wait_for_clean(60)
        time.sleep(5)
    results.append(test_scenario(4, "OSD Administrative Down (ceph osd down/out)", start_osd_down, stop_osd_down, settle_after_stop=6))
    
    # 5. OSD Process Kill
    def start_osd_kill(): vm_exec(f"systemctl stop '{OSD_SVC}'")
    def stop_osd_kill():
        vm_exec("systemctl restart ceph.target; ceph osd in osd.0")
        wait_for_clean(60)
        time.sleep(5)
    results.append(test_scenario(5, "OSD Process Kill (systemctl stop ceph-osd@0)", start_osd_kill, stop_osd_kill, settle_after_stop=6))
    
    # 6. PG-Level Targeted Degradation
    def start_pg_target(): vm_exec(f"ceph osd down osd.0 && ceph osd out osd.0 && systemctl stop '{OSD_SVC}'")
    def stop_pg_target():
        vm_exec("systemctl restart ceph.target; ceph osd in osd.0")
        wait_for_clean(60)
        time.sleep(5)
    results.append(test_scenario(6, "PG-Level Targeted Degradation", start_pg_target, stop_pg_target, settle_after_stop=6))
    
    # 7. Monitor Quorum Loss
    def start_mon(): vm_exec(f"systemctl stop '{MON_SVC}'")
    def stop_mon():
        vm_exec("systemctl restart ceph.target; ceph osd in osd.0")
        wait_for_clean(60)
        time.sleep(5)
    results.append(test_scenario(7, "Monitor Quorum Loss (systemctl stop ceph-mon)", start_mon, stop_mon, settle_after_stop=6))
    
    # Final Summary Table
    print("\n" + "=" * 76)
    print("                    FINAL DEMO VALIDATION REPORT")
    print("=" * 76)
    print(f"{'#':<3} {'Scenario Name':<44} {'Detected':<10} {'Recovered':<10} {'Verdict':<8}")
    print("-" * 76)
    for r in results:
        print(f"{r['num']:<3} {r['name']:<44} {str(r['detected']):<10} {str(r['recovered']):<10} {r['verdict']:<8}")
    print("=" * 76)

if __name__ == "__main__":
    main()
