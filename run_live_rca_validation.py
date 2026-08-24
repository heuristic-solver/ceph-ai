import os
import sys
import time
import json
import sqlite3
import paramiko
from datetime import datetime, timezone
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

import metrics_collector
import ml_anomaly_detector
import ceph_semantic_baseline
import diagnostic_engine
import llm_analyst
import alert_engine
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

def tick_system():
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
    return snap, v7, v8

def run_live_rca_suite():
    print("=" * 80)
    print("         LIVE CLUSTER ROOT CAUSE ANALYSIS (RCA) VALIDATION SUITE")
    print("=" * 80)

    # 0. Ensure clean cluster before starting
    print("[INIT] Verifying clean cluster state before test suite...")
    vm_exec("ceph crash archive-all 2>/dev/null || true")
    vm_exec("systemctl reset-failed ceph.target 2>/dev/null || true")

    # Discover FSID and service names dynamically from the live VM
    _info_ssh = get_ssh()
    info = get_cluster_info(_info_ssh)
    _info_ssh.close()
    OSD_SVC = info["osd_service"]
    MON_SVC = info["mon_service"]
    print(f"[INIT] Detected OSD service: {OSD_SVC}")
    print(f"[INIT] Detected MON service: {MON_SVC}")

    if not wait_for_clean(30):
        print("[WARN] Cluster not completely clean, attempting recovery...")
        vm_exec(f"systemctl start '{OSD_SVC}'; systemctl start '{MON_SVC}'; ceph osd in osd.0 && ceph osd up osd.0")
        wait_for_clean(30)

    # Helper injection / recovery functions
    def start_cpu(): vm_bg("for i in $(seq 1 8); do sha256sum /dev/zero > /dev/null 2>&1 & done")
    def stop_cpu(): vm_exec("killall -9 sha256sum 2>/dev/null; true")

    def start_ram():
        free_out = vm_exec("free -m | awk 'NR==2{print $7}'")
        try: free_mib = int(free_out.strip())
        except: free_mib = 1500
        target = max(600, free_mib - 250)
        ram_py = f"import time; chunks = [bytearray(1024*1024) for _ in range({target})]; time.sleep(3600)"
        vm_exec(f"echo '{ram_py}' > /tmp/ram_drain_demo.py")
        vm_bg("python3 /tmp/ram_drain_demo.py")
    def stop_ram(): vm_exec("pkill -9 -f ram_drain_demo 2>/dev/null; rm -f /tmp/ram_drain_demo.py; true")

    def start_io():
        vm_exec("touch /tmp/io_demo_active")
        vm_bg("while [ -f /tmp/io_demo_active ]; do dd if=/dev/zero of=/tmp/io_demo_test bs=10M count=50 oflag=dsync conv=notrunc 2>/dev/null; done")
    def stop_io(): vm_exec("rm -f /tmp/io_demo_active; killall -9 dd 2>/dev/null; rm -f /tmp/io_demo_test; true")

    def start_osd_down(): vm_exec(f"ceph osd down osd.0 && ceph osd out osd.0 && systemctl stop '{OSD_SVC}'")
    def stop_osd_down(): vm_exec(f"systemctl reset-failed; systemctl start '{OSD_SVC}'; ceph osd in osd.0 && ceph osd up osd.0; ceph crash archive-all")

    def start_osd_kill(): vm_exec(f"systemctl stop '{OSD_SVC}'")
    def stop_osd_kill(): vm_exec(f"systemctl reset-failed; systemctl start '{OSD_SVC}'; ceph osd in osd.0 && ceph osd up osd.0; ceph crash archive-all")

    def start_pg_target(): vm_exec(f"ceph osd down osd.0 && ceph osd out osd.0 && systemctl stop '{OSD_SVC}'")
    def stop_pg_target(): vm_exec(f"systemctl reset-failed; systemctl start '{OSD_SVC}'; ceph osd in osd.0 && ceph osd up osd.0; ceph crash archive-all")

    def start_mon(): vm_exec(f"systemctl stop '{MON_SVC}'")
    def stop_mon(): vm_exec(f"systemctl reset-failed; systemctl start '{MON_SVC}'; ceph osd in osd.0; ceph crash archive-all")

    scenarios = [
        {"id": 1, "name": "CPU Thrashing", "start": start_cpu, "stop": stop_cpu, "expected_category": "HOST_COMPUTE_THRASHING", "settle": 15},
        {"id": 2, "name": "RAM Starvation", "start": start_ram, "stop": stop_ram, "expected_category": "HOST_MEMORY_STARVATION", "settle": 6},
        {"id": 3, "name": "Storage I/O Saturation", "start": start_io, "stop": stop_io, "expected_category": "STORAGE_IO_SATURATION", "settle": 6},
        {"id": 4, "name": "OSD Administrative Down", "start": start_osd_down, "stop": stop_osd_down, "expected_category": "OSD_ADMIN_DOWN", "settle": 6},
        {"id": 5, "name": "OSD Process Kill", "start": start_osd_kill, "stop": stop_osd_kill, "expected_category": "OSD_PROCESS_CRASH", "settle": 6},
        {"id": 6, "name": "PG-Level Degradation", "start": start_pg_target, "stop": stop_pg_target, "expected_category": "OSD_ADMIN_DOWN", "settle": 6},
        {"id": 7, "name": "Monitor Quorum Loss", "start": start_mon, "stop": stop_mon, "expected_category": "CEPH_MON_QUORUM_LOSS", "settle": 6}
    ]

    report = []

    for sc in scenarios:
        num = sc["id"]
        name = sc["name"]
        print(f"\n[{num}/7] >>> RUNNING SCENARIO: {name} <<<")
        
        # 1. Inject fault
        print("  -> Injecting fault on live VM...")
        sc["start"]()
        time.sleep(3)

        # 2. Collect snapshots and evaluate
        detected = False
        latest_diag = None
        for poll in range(1, 6):
            time.sleep(2)
            snap, v7, v8 = tick_system()
            v7_anom = bool(v7 and v7.get("is_anomaly"))
            v8_anom = bool(v8 and v8.get("is_anomaly"))
            
            if v7_anom or v8_anom:
                detected = True
                ctx = diagnostic_engine.build_incident_context(v7_result=v7, v8_result=v8, raw_snapshot=snap)
                latest_diag = llm_analyst.diagnose_incident(ctx)
                print(f"  -> Poll #{poll}: Anomaly Detected! (Host_v7={v7_anom}, Ceph_v8={v8_anom})")
                break
            else:
                print(f"  -> Poll #{poll}: Waiting for metric divergence...")

        # 3. Stop fault & clean up
        print("  -> Stopping fault & recovering cluster...")
        sc["stop"]()
        time.sleep(sc["settle"])
        vm_exec("ceph crash archive-all 2>/dev/null || true")
        clean = wait_for_clean(45)
        print(f"  -> Cluster clean recovery: {'SUCCESS' if clean else 'PENDING'}")

        # 4. Check Root Cause Evaluation
        cat_match = False
        summary = "No diagnosis generated"
        if latest_diag:
            diag_cat = latest_diag.get("fault_category", "UNKNOWN")
            summary = latest_diag.get("root_cause_summary", "")
            cat_match = (diag_cat == sc["expected_category"]) or \
                        (sc["id"] == 5 and diag_cat in ("OSD_PROCESS_CRASH", "OSD_DAEMON_OFFLINE")) or \
                        (sc["id"] == 6 and diag_cat in ("PG_DATA_DEGRADATION", "OSD_ADMIN_DOWN", "OSD_PROCESS_CRASH"))
            print(f"  -> Diagnosed Root Cause: [{diag_cat}] {summary}")
            print(f"  -> Evidence: {latest_diag.get('evidence_chain', [])[:2]}")
            print(f"  -> Remediation Steps: {latest_diag.get('remediation_steps', [])[:2]}")

        verdict = detected and cat_match and clean
        report.append({
            "id": num,
            "name": name,
            "detected": detected,
            "diagnosed_category": latest_diag.get("fault_category", "N/A") if latest_diag else "N/A",
            "root_cause_summary": summary,
            "recovered": clean,
            "verdict": "PASS" if verdict else "FAIL"
        })

    # Summary Output
    print("\n\n" + "=" * 85)
    print("               LIVE CLUSTER ROOT CAUSE ANALYSIS (RCA) FINAL REPORT")
    print("=" * 85)
    print(f"{'#':<3} {'Scenario Name':<28} {'Detected':<10} {'Diagnosed Root Cause Category':<28} {'Recovered':<10} {'Verdict':<8}")
    print("-" * 85)
    for r in report:
        print(f"{r['id']:<3} {r['name']:<28} {str(r['detected']):<10} {r['diagnosed_category']:<28} {str(r['recovered']):<10} {r['verdict']:<8}")
    print("=" * 85)

    all_pass = all(r["verdict"] == "PASS" for r in report)
    print(f"\nFINAL VERDICT: {'100% PASS ACROSS ALL 7 LIVE SCENARIOS' if all_pass else 'SOME SCENARIOS FAILED'}\n")
    return all_pass

if __name__ == "__main__":
    run_live_rca_suite()
