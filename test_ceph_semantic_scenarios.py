"""
Ceph-Level Fault Injection Test Harness (v8) -- Phase 3
=======================================================
Five Ceph-semantic fault scenarios.  Completely independent from
test_ml_failure_scenarios.py -- that file is untouched and can still be
run independently.

Uses ceph_semantic_baseline.py for AI detection (ceph_semantic_model.pkl).
Uses osd_provisioner.py to auto-provision OSDs for multi-OSD scenarios.

Prerequisites:
    1. python baseline_injector.py           # fill metrics_timeseries (existing)
    2. python ceph_semantic_baseline.py      # train ceph_semantic_model.pkl (new)
    3. python test_ceph_semantic_scenarios.py [--trials N]

Scenarios:
    1. OSD Administrative Down   (ceph osd down/out -> reversible)
    2. OSD Process Kill          (systemctl stop ceph-osd@0 -> realistic crash)
    3. PG-Level Targeted Degrad. (down the OSD backing a specific PG)
    4. Pool Write Failure        (multi-OSD coordinated fault; auto-provisions 2nd OSD)
    5. Monitor Quorum Loss       (stop ceph-mon; Netdata-only snapshots during fault)
"""

import os
import re
import sys
import time
import json
import sqlite3
import argparse
import requests
import paramiko
from datetime import datetime, timezone
from dotenv import load_dotenv

import metrics_collector
import ceph_semantic_baseline
import osd_provisioner

load_dotenv()

HOST     = os.getenv("VM_SSH_HOST", "127.0.0.1")
PORT     = int(os.getenv("VM_SSH_PORT", "2222"))
USER     = os.getenv("VM_SSH_USER", "vboxuser")
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.getenv("DB_PATH", os.path.join(ROOT_DIR, "ceph_monitor.db"))

NETDATA_URL  = "http://127.0.0.1:19999/api/v1/allmetrics?format=json"
CMD_TIMEOUT  = 30   # default SSH command timeout (seconds)
MON_TIMEOUT  = 5    # tight timeout during mon-loss scenario
FAULT_WINDOW = 6    # max detection poll steps (each 2s -> 12s total window)


# -- SSH helpers ---------------------------------------------------------------

def sudo_exec(ssh, cmd, timeout=CMD_TIMEOUT):
    """
    Runs `sudo -S <cmd>` on the VM.
    Returns (stdout_str, stderr_str). Never raises; returns ("", err) on timeout.
    """
    try:
        stdin, stdout, stderr = ssh.exec_command(f"sudo -S {cmd}", timeout=timeout)
        stdin.write(PASSWORD + "\n")
        stdin.flush()
        out = stdout.read().decode("utf-8", errors="ignore").strip()
        err = stderr.read().decode("utf-8", errors="ignore").strip()
        err_clean = "\n".join(
            l for l in err.splitlines()
            if "password" not in l.lower() or len(l) > 60
        )
        return out, err_clean
    except Exception as exc:
        return "", str(exc)


# -- Snapshot helpers ----------------------------------------------------------

def take_snapshot(ssh, is_baseline=False):
    """
    Collects a full metrics snapshot (Netdata + Ceph CLI via SSH) and saves
    it to the metrics_timeseries SQLite table.
    Returns the ISO timestamp string, or None on failure.
    """
    scraped = metrics_collector.collect_snapshot(ssh)
    if not scraped:
        return None
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO metrics_timeseries (timestamp, data, is_baseline) VALUES (?, ?, ?)",
            (ts, json.dumps(scraped), 1 if is_baseline else 0),
        )
        conn.commit()
        conn.close()
        return ts
    except Exception as e:
        print(f"  [WARN] DB save error: {e}", flush=True)
        return None


def take_snapshot_netdata_only(is_baseline=False):
    """
    Snapshot using ONLY Netdata HTTP (no SSH Ceph CLI calls).
    Used during Scenario 5 (mon quorum loss) when Ceph CLI hangs.
    Returns the ISO timestamp string, or None if Netdata is also unreachable.
    """
    scraped = {}
    try:
        r = requests.get(NETDATA_URL, timeout=2)
        if r.status_code == 200:
            scraped = metrics_collector.parse_netdata_metrics(r.json())
    except Exception:
        pass

    if not scraped:
        return None

    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO metrics_timeseries (timestamp, data, is_baseline) VALUES (?, ?, ?)",
            (ts, json.dumps(scraped), 1 if is_baseline else 0),
        )
        conn.commit()
        conn.close()
        return ts
    except Exception as e:
        print(f"  [WARN] DB save (Netdata-only): {e}", flush=True)
        return None


# -- Cluster-state helpers -----------------------------------------------------

def wait_for_clean(ssh, timeout=120, poll=5):
    """
    Polls ceph pg stat until all PGs are active+clean or timeout expires.
    Returns True if clean within timeout, False otherwise.
    """
    print(f"  [WAIT] Waiting up to {timeout}s for all PGs to be active+clean...", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        out, _ = sudo_exec(ssh, "ceph pg stat", timeout=10)
        if out and "active+clean" in out and "degraded" not in out \
                and "inactive" not in out and "peering" not in out:
            print(f"  [WAIT] [OK] Clean: {out[:80]}", flush=True)
            return True
        print(f"  [WAIT]   {out[:80]}", flush=True)
        time.sleep(poll)
    print("  [WAIT] Timeout -- cluster may still be recovering.", flush=True)
    return False


def wait_for_osd_up(ssh, osd_id, timeout=90, poll=4):
    """Polls until osd.{osd_id} appears as 'up' in ceph osd tree."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out, _ = sudo_exec(ssh, "ceph osd tree", timeout=10)
        for line in out.splitlines():
            if f"osd.{osd_id}" in line and "up" in line:
                print(f"  [WAIT] osd.{osd_id} is up.", flush=True)
                return True
        time.sleep(poll)
    print(f"  [WAIT] Warning: osd.{osd_id} did not come up within {timeout}s.", flush=True)
    return False


def get_mon_name(ssh):
    """Auto-detects the first monitor name from `ceph mon dump --format json`."""
    out, _ = sudo_exec(ssh, "ceph mon dump --format json", timeout=15)
    try:
        dump = json.loads(out)
        mons = dump.get("mons", [])
        if mons:
            name = mons[0].get("name", "")
            if name:
                return name
    except Exception:
        pass
    # Fallback: use VM short hostname
    hostname, _ = sudo_exec(ssh, "hostname -s", timeout=5)
    return hostname.strip() or "unknown"


# -- Pretty-print helpers ------------------------------------------------------

def print_header(title):
    print("\n" + "=" * 80, flush=True)
    print(f"  >  {title}", flush=True)
    print("=" * 80, flush=True)


def print_detection_detail(res):
    """Prints a concise summary of one detection result."""
    if not res:
        return
    anom    = res.get("is_anomaly", False)
    score   = res.get("decision_score", "?")
    pca_err = res.get("pca_reconstruct_err", "?")
    models  = res.get("triggered_models", [])
    devs    = list(res.get("deviated_features", {}).keys())[:4]
    tag     = "[!] ANOMALY" if anom else "[OK] NORMAL"
    print(
        f"    {tag}  IF={score}  PCA={pca_err}  "
        f"triggered={models}  top_devs={devs}",
        flush=True,
    )


# -- Generic scenario runner ---------------------------------------------------

def run_scenario(ssh, name, setup_func, cleanup_func, trials, expected_anom=True):
    """
    Runs `trials` trials of a fault scenario using the standard poll loop.
    Each trial:
      - calls setup_func() -> returns opaque state dict for cleanup
      - polls up to FAULT_WINDOWx2s for AI detection
      - calls cleanup_func(state) -> restores cluster
      - waits for clean PGs before next trial

    Returns: (true_pass_count, detection_latencies_list)
    """
    print_header(f"{name}  ({trials} trials)")
    true_passes = 0
    latencies   = []

    for t in range(1, trials + 1):
        print(f"\n  [Trial {t}/{trials}] Injecting fault: {name}", flush=True)
        state    = setup_func()
        start_ts = time.time()
        detected = False

        for step in range(1, FAULT_WINDOW + 1):
            time.sleep(2.0)
            take_snapshot(ssh, is_baseline=False)
            res = ceph_semantic_baseline.detect_anomalies()
            print(f"    Step {step}/{FAULT_WINDOW}", end="  ", flush=True)
            print_detection_detail(res)

            if res and res.get("is_anomaly"):
                lat = time.time() - start_ts
                latencies.append(lat)
                print(f"    [OK] Detected at step {step}  Latency: {lat:.1f}s", flush=True)
                detected = True
                break

        if not detected:
            print("    [FAIL] NOT detected within poll window.", flush=True)

        if detected == expected_anom:
            true_passes += 1

        print("  [Cleanup] Reverting fault...", flush=True)
        cleanup_func(state)
        wait_for_clean(ssh, timeout=90, poll=5)
        take_snapshot(ssh, is_baseline=False)   # one clean post-fault snapshot

    return true_passes, latencies


def run_scenario_mon_loss(ssh, setup_func, cleanup_func, trials):
    """
    Specialised runner for Scenario 5 (mon quorum loss).
    During the fault: Ceph CLI commands hang, so we use Netdata-only snapshots.
    Detection relies on feature dropout: all Ceph-CLI metrics go to 0,
    which is a recognisable departure from baseline.
    """
    print_header(f"SCENARIO 5: MON QUORUM LOSS  ({trials} trials)")
    true_passes = 0
    latencies   = []

    for t in range(1, trials + 1):
        print(f"\n  [Trial {t}/{trials}] Stopping ceph-mon...", flush=True)
        state    = setup_func()
        start_ts = time.time()
        detected = False

        for step in range(1, FAULT_WINDOW + 1):
            time.sleep(2.0)
            ts = take_snapshot_netdata_only(is_baseline=False)
            if ts is None:
                print(f"    Step {step}: Netdata also unreachable -- deep cluster failure.", flush=True)
            else:
                res = ceph_semantic_baseline.detect_anomalies()
                print(f"    Step {step}/{FAULT_WINDOW}", end="  ", flush=True)
                print_detection_detail(res)
                if res and res.get("is_anomaly"):
                    lat = time.time() - start_ts
                    latencies.append(lat)
                    print(f"    [OK] Detected at step {step}  Latency: {lat:.1f}s", flush=True)
                    detected = True
                    break

        if not detected:
            print("    [FAIL] NOT detected within poll window.", flush=True)

        true_passes += 1 if detected else 0

        print("  [Cleanup] Restarting ceph-mon...", flush=True)
        cleanup_func(state)
        # Wait extra after mon restart -- quorum election takes ~5s
        time.sleep(10)
        wait_for_clean(ssh, timeout=60, poll=5)

    return true_passes, latencies


# ==============================================================================
# Scenario 1 -- OSD Administrative Down (Tier 3, Controlled)
# ==============================================================================

def make_s1_osd_admin_down(ssh):
    """
    Inject:  ceph osd down osd.0  +  ceph osd out osd.0
    Revert:  ceph osd in osd.0   +  ceph osd up osd.0
    Expected: ceph_osd_status change, pg_degraded_count > 0, ceph_health_code > 0
    """
    def setup():
        print("  [S1] Marking osd.0 administratively down + out...", flush=True)
        sudo_exec(ssh, "ceph osd down osd.0")
        sudo_exec(ssh, "ceph osd out osd.0")
        time.sleep(3)
        out, _ = sudo_exec(ssh, "ceph osd stat")
        print(f"  [S1] OSD stat: {out}", flush=True)
        return {"osd_id": 0}

    def cleanup(state):
        oid = state.get("osd_id", 0)
        print(f"  [S1] Restoring osd.{oid}...", flush=True)
        sudo_exec(ssh, f"ceph osd in osd.{oid}")
        sudo_exec(ssh, f"ceph osd up osd.{oid}")
        time.sleep(5)

    return setup, cleanup


# ==============================================================================
# Scenario 2 -- OSD Process Kill (Tier 2, Realistic Crash)
# ==============================================================================

def make_s2_osd_process_kill(ssh):
    """
    Inject:  systemctl stop ceph-osd@0  (daemon crash simulation)
    Note:    Ceph heartbeat timeout (~20s) means OSD is declared 'down' after a delay.
             We give a 10s head-start then start polling -- tests pre-alarm detection.
    Revert:  systemctl start ceph-osd@0  +  ceph osd in osd.0
    """
    def setup():
        print("  [S2] Stopping ceph-osd@0 daemon (simulated crash)...", flush=True)
        sudo_exec(ssh, "systemctl stop ceph-osd@0", timeout=20)
        # Give Ceph heartbeat timeout a head-start so degradation is visible
        print("  [S2] Waiting 10s for heartbeat timeout...", flush=True)
        time.sleep(10)
        out, _ = sudo_exec(ssh, "ceph osd stat")
        print(f"  [S2] OSD stat after stop: {out}", flush=True)
        return {"osd_id": 0}

    def cleanup(state):
        oid = state.get("osd_id", 0)
        print(f"  [S2] Restarting ceph-osd@{oid}...", flush=True)
        sudo_exec(ssh, f"systemctl start ceph-osd@{oid}", timeout=30)
        time.sleep(8)
        # Mark osd.0 in (daemon restart brings it up but may not auto-mark in)
        sudo_exec(ssh, f"ceph osd in osd.{oid}")
        wait_for_osd_up(ssh, oid, timeout=60)

    return setup, cleanup


# ==============================================================================
# Scenario 3 -- PG-Level Targeted Degradation
# ==============================================================================

def make_s3_pg_targeted(ssh):
    """
    Inject:  Identify the acting OSD for a specific PG via `ceph pg dump`,
             then administratively down that specific OSD.
    Revert:  ceph osd in  +  ceph osd up
    Demonstrates: detection of PG-specific degradation via ceph_pg_degraded signals.
    """
    def _pick_target_osd():
        """Returns the acting OSD for the first PG in the cluster."""
        out, _ = sudo_exec(ssh, "ceph pg dump --format json", timeout=20)
        try:
            pg_dump  = json.loads(out)
            pg_stats = pg_dump.get("pg_map", {}).get("pg_stats", [])
            if not pg_stats:
                pg_stats = pg_dump.get("pg_stats", [])
            if pg_stats:
                first_pg = pg_stats[0]
                acting   = first_pg.get("acting", [0])
                pgid     = first_pg.get("pgid", "unknown")
                target   = acting[0] if acting else 0
                print(f"  [S3] Targeting PG {pgid} on osd.{target}", flush=True)
                return target
        except Exception as e:
            print(f"  [S3] PG dump parse error: {e}. Defaulting to osd.0", flush=True)
        return 0

    def setup():
        target_osd = _pick_target_osd()
        print(f"  [S3] Marking osd.{target_osd} down + out (PG targeted fault)...", flush=True)
        sudo_exec(ssh, f"ceph osd down osd.{target_osd}")
        sudo_exec(ssh, f"ceph osd out osd.{target_osd}")
        time.sleep(5)
        out, _ = sudo_exec(ssh, "ceph pg stat")
        print(f"  [S3] PG stat: {out}", flush=True)
        return {"osd_id": target_osd}

    def cleanup(state):
        oid = state.get("osd_id", 0)
        print(f"  [S3] Restoring osd.{oid}...", flush=True)
        sudo_exec(ssh, f"ceph osd in osd.{oid}")
        sudo_exec(ssh, f"ceph osd up osd.{oid}")
        time.sleep(5)

    return setup, cleanup


# ==============================================================================
# Scenario 4 -- Pool Write Failure (Coordinated OSD Fault)
# ==============================================================================

def make_s4_pool_write_failure(ssh, live_osd_ids):
    """
    If 2+ OSDs exist: takes both OSDs below min_size simultaneously,
    forcing PG_AVAILABILITY (all PGs go inactive -> pool writes fail).
    If single OSD (fallback): same as Scenario 1 (all PGs already go inactive
    on a 1-OSD cluster when osd.0 is down). Documented in results.

    Precondition: osd_provisioner.ensure_osd_count(ssh, 2) was called before
                  this factory so live_osd_ids has at least 2 entries.
    """
    multi_osd = len(live_osd_ids) >= 2

    if multi_osd:
        print(
            f"  [S4] Multi-OSD mode. Will fault OSDs: {live_osd_ids[:2]}",
            flush=True,
        )
    else:
        print(
            "  [S4] Single-OSD fallback. On a 1-OSD cluster, osd.0 down = "
            "all PGs inactive (equivalent to pool write failure).",
            flush=True,
        )

    target_osds = live_osd_ids[:2] if multi_osd else [0]

    def setup():
        print(f"  [S4] Marking OSDs {target_osds} down + out...", flush=True)
        for oid in target_osds:
            sudo_exec(ssh, f"ceph osd down osd.{oid}")
            sudo_exec(ssh, f"ceph osd out osd.{oid}")
        time.sleep(5)
        out, _ = sudo_exec(ssh, "ceph pg stat")
        print(f"  [S4] PG stat: {out}", flush=True)
        return {"target_osds": target_osds, "multi_osd": multi_osd}

    def cleanup(state):
        for oid in state.get("target_osds", [0]):
            print(f"  [S4] Restoring osd.{oid}...", flush=True)
            sudo_exec(ssh, f"ceph osd in osd.{oid}")
            sudo_exec(ssh, f"ceph osd up osd.{oid}")
        time.sleep(5)

    return setup, cleanup


# ==============================================================================
# Scenario 5 -- Monitor Quorum Loss
# ==============================================================================

def make_s5_mon_quorum_loss(ssh):
    """
    Inject:  systemctl stop ceph-mon@<mon_name>
    Signal:  All Ceph CLI metrics drop to 0 (feature dropout pattern).
             Netdata itself stays alive (does not need the mon).
    Revert:  systemctl start ceph-mon@<mon_name>  + 15s stabilisation

    NOTE: All ceph CLI calls in this scenario use MON_TIMEOUT (5s) to avoid
          hanging the test run.  Manual recovery: sudo systemctl start ceph-mon@<name>
    """
    mon_name = get_mon_name(ssh)
    print(f"  [S5 PREFLIGHT] Detected monitor name: '{mon_name}'", flush=True)

    if not mon_name or mon_name == "unknown":
        print(
            "  [S5] WARNING: Could not detect mon name. "
            "Scenario 5 may fail. Check ceph mon dump on the VM.",
            flush=True,
        )

    def setup():
        print(f"  [S5] Stopping ceph-mon@{mon_name}...", flush=True)
        # Use a short timeout -- the mon stop itself may not ACK if quorum collapses
        try:
            stdin, stdout, stderr = ssh.exec_command(
                f"sudo -S systemctl stop ceph-mon@{mon_name}", timeout=MON_TIMEOUT
            )
            stdin.write(PASSWORD + "\n")
            stdin.flush()
            # Don't block waiting for stdout -- mon stop may not echo back
        except Exception as e:
            print(f"  [S5] Expected timeout/error stopping mon: {e}", flush=True)
        time.sleep(5)
        print(
            f"  [S5] Mon stopped. Ceph CLI is now unavailable. "
            f"Using Netdata-only snapshots.",
            flush=True,
        )
        return {"mon_name": mon_name}

    def cleanup(state):
        mn = state.get("mon_name", mon_name)
        print(f"  [S5] Restarting ceph-mon@{mn}...", flush=True)
        try:
            stdin, stdout, stderr = ssh.exec_command(
                f"sudo -S systemctl start ceph-mon@{mn}", timeout=30
            )
            stdin.write(PASSWORD + "\n")
            stdin.flush()
            stdout.read()
        except Exception as e:
            print(f"  [S5] Warning restarting mon: {e}", flush=True)
        print("  [S5] Waiting 15s for quorum to re-establish...", flush=True)
        time.sleep(15)
        # Verify with short timeout
        out, _ = sudo_exec(ssh, "ceph status", timeout=10)
        print(f"  [S5] Cluster status: {out[:120]}", flush=True)

    return setup, cleanup


# ==============================================================================
# Main
# ==============================================================================

def main(trials=3):
    model_path = os.path.join(
        os.path.dirname(os.path.abspath(DB_PATH)), "ceph_semantic_model.pkl"
    )
    if not os.path.exists(model_path):
        print("\n[ERROR] ceph_semantic_model.pkl not found!")
        print("[ERROR] Please run:  python ceph_semantic_baseline.py")
        print("[ERROR] Then retry:  python test_ceph_semantic_scenarios.py")
        sys.exit(1)

    print_header("CEPH SEMANTIC FAULT DETECTION v8 -- INITIALISING")
    print(f"  Trials per scenario : {trials}", flush=True)
    print(f"  Poll window         : {FAULT_WINDOW * 2}s ({FAULT_WINDOW} steps x 2s)", flush=True)
    print(f"  DB                  : {DB_PATH}", flush=True)
    print(f"  Model               : {model_path}", flush=True)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=HOST, port=PORT, username=USER, password=PASSWORD, timeout=10)

    try:
        # -- Pre-flight: cluster status --------------------------------------
        print_header("PRE-FLIGHT CHECKS")
        ceph_status, _ = sudo_exec(ssh, "ceph status")
        print(ceph_status[:500], flush=True)

        osd_info = osd_provisioner.count_live_osds(ssh)
        print(f"\n  OSDs: {osd_info}", flush=True)

        # -- Auto-provision 2nd OSD for Scenario 4 --------------------------
        print("\n[PRE-FLIGHT] Ensuring >=2 OSDs for Scenario 4 (Pool Write Failure)...", flush=True)
        provisioned_osds = osd_provisioner.ensure_osd_count(ssh, required_count=2)

        # Re-query live OSD IDs after provisioning
        live_osd_ids = osd_provisioner.count_live_osds(ssh)["ids"]

        # -- Warmup snapshots ------------------------------------------------
        print("\n[WARMUP] Collecting 3 pre-test snapshots...", flush=True)
        for _ in range(3):
            take_snapshot(ssh, is_baseline=False)
            time.sleep(2)

        results = {}

        # --------------------------------------------------------------------
        # Phase 1 -- Healthy Hold-Out (False Positive Baseline)
        # --------------------------------------------------------------------
        print_header(f"PHASE 1: HEALTHY HOLD-OUT ({trials} trials -- expect 0 anomalies)")
        fps = 0
        for t in range(1, trials + 1):
            time.sleep(2.0)
            take_snapshot(ssh, is_baseline=False)
            res = ceph_semantic_baseline.detect_anomalies()
            if res and res.get("is_anomaly"):
                fps += 1
                print(
                    f"  [Trial {t}] [!] FALSE POSITIVE  "
                    f"triggered={res.get('triggered_models')}",
                    flush=True,
                )
            else:
                score = res.get("decision_score", "?") if res else "?"
                print(f"  [Trial {t}] [OK] Clean (TN)  IF={score}", flush=True)
        results["healthy"] = {"tn": trials - fps, "fp": fps}

        # --------------------------------------------------------------------
        # Scenario 1 -- OSD Administrative Down
        # --------------------------------------------------------------------
        s1_setup, s1_cleanup = make_s1_osd_admin_down(ssh)
        tp1, lat1 = run_scenario(
            ssh, "SCENARIO 1: OSD ADMINISTRATIVE DOWN",
            s1_setup, s1_cleanup, trials,
        )
        results["s1"] = {"label": "OSD Admin Down", "tp": tp1, "fn": trials - tp1, "latencies": lat1}

        # --------------------------------------------------------------------
        # Scenario 2 -- OSD Process Kill
        # --------------------------------------------------------------------
        s2_setup, s2_cleanup = make_s2_osd_process_kill(ssh)
        tp2, lat2 = run_scenario(
            ssh, "SCENARIO 2: OSD PROCESS KILL",
            s2_setup, s2_cleanup, trials,
        )
        results["s2"] = {"label": "OSD Process Kill", "tp": tp2, "fn": trials - tp2, "latencies": lat2}

        # --------------------------------------------------------------------
        # Scenario 3 -- PG-Level Targeted Degradation
        # --------------------------------------------------------------------
        s3_setup, s3_cleanup = make_s3_pg_targeted(ssh)
        tp3, lat3 = run_scenario(
            ssh, "SCENARIO 3: PG-LEVEL TARGETED DEGRADATION",
            s3_setup, s3_cleanup, trials,
        )
        results["s3"] = {"label": "PG Targeted Degradation", "tp": tp3, "fn": trials - tp3, "latencies": lat3}

        # --------------------------------------------------------------------
        # Scenario 4 -- Pool Write Failure (coordinated OSD fault)
        # --------------------------------------------------------------------
        s4_setup, s4_cleanup = make_s4_pool_write_failure(ssh, live_osd_ids)
        tp4, lat4 = run_scenario(
            ssh, "SCENARIO 4: POOL WRITE FAILURE",
            s4_setup, s4_cleanup, trials,
        )
        mode_note = "multi-OSD" if len(live_osd_ids) >= 2 else "single-OSD fallback"
        results["s4"] = {
            "label": f"Pool Write Failure ({mode_note})",
            "tp": tp4, "fn": trials - tp4, "latencies": lat4,
        }

        # --------------------------------------------------------------------
        # Scenario 5 -- Monitor Quorum Loss
        # --------------------------------------------------------------------
        s5_setup, s5_cleanup = make_s5_mon_quorum_loss(ssh)
        tp5, lat5 = run_scenario_mon_loss(ssh, s5_setup, s5_cleanup, trials)
        results["s5"] = {"label": "Mon Quorum Loss", "tp": tp5, "fn": trials - tp5, "latencies": lat5}

    finally:
        # -- Teardown provisioned OSDs ----------------------------------------
        if provisioned_osds:
            print_header("TEARDOWN -- REMOVING PROVISIONED OSDs")
            osd_provisioner.teardown_provisioned_osds(ssh, provisioned_osds)

        ssh.close()
        print("\n[CLEANUP] SSH closed.", flush=True)

    # ------------------------------------------------------------------------
    # Final Confusion Matrix
    # ------------------------------------------------------------------------
    print_header("FINAL CONFUSION MATRIX -- CEPH SEMANTIC DETECTION v8")

    scenario_keys = ["s1", "s2", "s3", "s4", "s5"]
    total_tp    = sum(results[k]["tp"] for k in scenario_keys)
    total_fn    = sum(results[k]["fn"] for k in scenario_keys)
    total_fault = len(scenario_keys) * trials

    all_lats = []
    for k in scenario_keys:
        all_lats.extend(results[k].get("latencies", []))
    med_lat = (
        f"{sorted(all_lats)[len(all_lats) // 2]:.1f}s"
        if all_lats else "N/A"
    )

    # Per-scenario table
    header = f"  {'Scenario':<35} {'TP':>4} {'FN':>4} {'TP%':>6} {'Avg Lat':>9}"
    print(header, flush=True)
    print("  " + "-" * 62, flush=True)
    for k in scenario_keys:
        r    = results[k]
        tp   = r["tp"]
        fn   = r["fn"]
        pct  = f"{100*tp/trials:.0f}%" if trials > 0 else "?"
        lats = r.get("latencies", [])
        avg  = f"{sum(lats)/len(lats):.1f}s" if lats else "N/A"
        print(f"  {r['label']:<35} {tp:>4} {fn:>4} {pct:>6} {avg:>9}", flush=True)

    print("  " + "-" * 62, flush=True)

    # Summary
    fp_rate = f"{100*results['healthy']['fp']/trials:.0f}%" if trials > 0 else "?"
    print(f"\n  Healthy Hold-Out : TN={results['healthy']['tn']}  FP={results['healthy']['fp']}  FP-rate={fp_rate}", flush=True)
    print(f"  Total Fault Trials    : {total_fault}", flush=True)
    print(f"    True Positives  (TP): {total_tp}", flush=True)
    print(f"    False Negatives (FN): {total_fn}", flush=True)
    print(f"  Median Detection Latency: {med_lat}", flush=True)
    print("=" * 80 + "\n", flush=True)

    # Machine-readable JSON summary for CI integration
    summary = {
        "trials_per_scenario": trials,
        "healthy": results["healthy"],
        "scenarios": {k: {"label": results[k]["label"], "tp": results[k]["tp"],
                          "fn": results[k]["fn"]} for k in scenario_keys},
        "totals": {"tp": total_tp, "fn": total_fn, "fault_trials": total_fault},
        "median_latency_s": med_lat,
    }
    print("JSON_SUMMARY:", json.dumps(summary), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ceph Semantic Fault Detection Test Harness v8",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scenarios:
  1. OSD Administrative Down    -- ceph osd down/out (controlled, instant)
  2. OSD Process Kill           -- systemctl stop (realistic crash + heartbeat delay)
  3. PG Targeted Degradation    -- faults the specific OSD backing a given PG
  4. Pool Write Failure         -- coordinated multi-OSD fault (auto-provisions 2nd OSD)
  5. Monitor Quorum Loss        -- ceph-mon stop; Netdata-only snapshots during fault
        """,
    )
    parser.add_argument(
        "--trials", type=int, default=3,
        help="Number of trials per scenario (default: 3)",
    )
    args = parser.parse_args()
    main(trials=args.trials)
