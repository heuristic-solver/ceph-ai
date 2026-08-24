import os
import re
import sqlite3
import json
import time
import requests
import paramiko
from datetime import datetime, timezone
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(ROOT_DIR, ".env")
load_dotenv(dotenv_path=env_path)

HOST = os.getenv("VM_SSH_HOST", "127.0.0.1")
PORT = int(os.getenv("VM_SSH_PORT", "2222"))
USER = os.getenv("VM_SSH_USER", "vboxuser")
PASSWORD = os.getenv("VM_SSH_PASSWORD", "admin")

DB_PATH = os.getenv("DB_PATH", os.path.join(ROOT_DIR, "ceph_monitor.db"))
NETDATA_URL = "http://127.0.0.1:19999/api/v1/allmetrics?format=json"

# High-speed collection interval (2 seconds for real-time visualization)
COLLECT_INTERVAL_SECONDS = 2


def init_db():
    """Initializes the SQLite database schema if not already created."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics_timeseries (
            timestamp TEXT PRIMARY KEY,
            data TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            source TEXT,
            severity TEXT,
            component TEXT,
            message TEXT,
            raw_line TEXT
        )
    """)
    conn.commit()
    conn.close()


def parse_netdata_metrics(json_dict):
    """Parses Netdata REST API response into flat key-value pairs."""
    metrics = {}
    if not isinstance(json_dict, dict):
        return metrics

    for metric_name, info in json_dict.items():
        if not isinstance(info, dict) or "dimensions" not in info:
            continue
        
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', metric_name)
        
        for dim_name, dim_info in info["dimensions"].items():
            if not isinstance(dim_info, dict) or "value" not in dim_info:
                continue
            
            val = dim_info["value"]
            if val is None:
                continue
            
            clean_dim = re.sub(r'[^a-zA-Z0-9_]', '_', dim_name)
            metrics[f"{clean_name}__{clean_dim}"] = float(val)
            
    return metrics


def fetch_netdata_via_ssh(ssh_client):
    """Fallback: fetches Netdata metrics locally on the VM over SSH if port forwarding is down."""
    try:
        cmd = "curl -s http://127.0.0.1:19999/api/v1/allmetrics?format=json"
        stdin, stdout, stderr = ssh_client.exec_command(cmd, timeout=5)
        raw_text = stdout.read().decode("utf-8").strip()
        if raw_text:
            return json.loads(raw_text)
    except Exception:
        pass
    return None


def scrape_host_metrics(ssh_client):
    """Gathers custom system and Ceph CLI performance snapshots via a single atomic SSH command."""
    custom_metrics = {}
    
    try:
        cmd = (
            "sudo -S bash -c '"
            "ceph --connect-timeout 3 status --format json 2>/dev/null || echo \"{}\"; echo \"__SEP__\"; "
            "ceph --connect-timeout 3 osd tree --format json 2>/dev/null || echo \"{}\"; echo \"__SEP__\"; "
            "ceph --connect-timeout 3 pg stat 2>/dev/null || echo \"\"; echo \"__SEP__\"; "
            "ceph --connect-timeout 3 osd perf --format json 2>/dev/null || echo \"{}\"'"
        )
        stdin, stdout, stderr = ssh_client.exec_command(cmd, timeout=8)
        stdin.write(PASSWORD + '\n')
        stdin.flush()
        raw_output = stdout.read().decode('utf-8', errors='ignore').strip()
        
        parts = raw_output.split("__SEP__")
        
        if len(parts) > 0 and parts[0].strip() and parts[0].strip() != "{}":
            try:
                status_json = json.loads(parts[0].strip())
                health_dict = status_json.get("health")
                if not health_dict:
                    custom_metrics["ceph_health_status"] = 2.0
                else:
                    health_str = health_dict.get("status", "HEALTH_OK")
                    checks = health_dict.get("checks", {})
                    critical_checks = [k for k in checks if k not in ("POOL_NO_REDUNDANCY", "MON_DISK_LOW", "MON_DISK_CRIT", "OSDMAP_FLAGS", "DEVICE_HEALTH_TOOMANY", "CEPHADM_FAILED_DAEMON")]
                    if health_str == "HEALTH_ERR" or len(critical_checks) > 0:
                        custom_metrics["ceph_health_status"] = 2.0 if health_str == "HEALTH_ERR" else 1.0
                    else:
                        custom_metrics["ceph_health_status"] = 0.0
                
                osd_map = status_json.get("osdmap", {})
                if osd_map:
                    custom_metrics["ceph_osd_stat_osds"] = float(osd_map.get("num_osds", 1.0))
                    custom_metrics["ceph_osd_stat_osds_up"] = float(osd_map.get("num_up_osds", 1.0))
                    custom_metrics["ceph_osd_stat_osds_in"] = float(osd_map.get("num_in_osds", 1.0))

                pg_map = status_json.get("pgmap", {})
                if pg_map:
                    total_pgs = float(pg_map.get("num_pgs", 97.0))
                    custom_metrics["ceph_pg_total"] = total_pgs
                    clean_cnt = 0.0
                    unhealthy_cnt = 0.0
                    for p in pg_map.get("pgs_by_state", []):
                        sname = p.get("state_name", "")
                        cnt = float(p.get("count", 0))
                        if sname == "active+clean":
                            clean_cnt += cnt
                        else:
                            unhealthy_cnt += cnt
                    custom_metrics["ceph_pg_active_clean"] = clean_cnt
                    custom_metrics["ceph_pg_degraded"] = unhealthy_cnt
            except Exception:
                custom_metrics["ceph_health_status"] = 2.0
        else:
            custom_metrics["ceph_health_status"] = 2.0

        # 2. Parse OSD Tree
        if len(parts) > 1 and parts[1].strip():
            try:
                tree_json = json.loads(parts[1].strip())
                for node in tree_json.get("nodes", []):
                    if node.get("type") == "osd":
                        osd_id = node.get("id")
                        osd_in = 1.0 if node.get("status") == "exists" or node.get("reweight", 0.0) > 0.0 else 0.0
                        osd_up = 1.0 if node.get("status") == "exists" and node.get("exists") == 1 else 0.0
                        custom_metrics[f"ceph_osd_in__ceph_daemon_osd_{osd_id}"] = osd_in
                        custom_metrics[f"ceph_osd_up__ceph_daemon_osd_{osd_id}"] = osd_up
            except Exception:
                pass

        # 3. Parse PG stat
        if len(parts) > 2 and parts[2].strip():
            try:
                pg_output = parts[2].strip()
                match = re.search(r'(\d+)\s+pgs:', pg_output)
                if match:
                    custom_metrics["ceph_pg_total"] = float(match.group(1))
                custom_metrics["ceph_pg_active_clean"] = float(pg_output.count("active+clean"))
                custom_metrics["ceph_pg_degraded"]     = float(pg_output.count("degraded"))
                custom_metrics["ceph_pg_peering"]      = float(pg_output.count("peering"))
            except Exception:
                pass

        # 4. Parse OSD Perf
        if len(parts) > 3 and parts[3].strip():
            try:
                perf_json = json.loads(parts[3].strip())
                max_apply = 0.0
                for osd_info in perf_json.get("osd_perf_infos", []):
                    stats = osd_info.get("perf_stats", {})
                    apply_lat = float(stats.get("apply_latency_ms", 0.0))
                    if apply_lat > max_apply:
                        max_apply = apply_lat
                custom_metrics["osd_apply_latency_ms"] = max_apply
            except Exception:
                pass

    except Exception:
        pass
        
    return custom_metrics


def collect_snapshot(ssh_client):
    """Unified collection point: scrapes Netdata + SSH and returns merged metrics."""
    scraped_data = {}
    netdata_raw = None
    
    # 1. Scrape Netdata REST API directly (HTTP)
    try:
        r = requests.get(NETDATA_URL, timeout=3)
        if r.status_code == 200:
            netdata_raw = r.json()
    except Exception:
        pass
        
    # If HTTP direct failed, fetch Netdata JSON over SSH curl
    if not netdata_raw and ssh_client:
        netdata_raw = fetch_netdata_via_ssh(ssh_client)
        
    # 2. Scrape Host CLI / OS Metrics over SSH if provided
    if ssh_client:
        host_metrics = scrape_host_metrics(ssh_client)
        scraped_data.update(host_metrics)
    
    # 3. Parse Netdata metrics if obtained
    if netdata_raw:
        scraped_data.update(parse_netdata_metrics(netdata_raw))
        
    return scraped_data


def _ensure_ssh_connected(ssh):
    """Maintains a persistent SSH connection, reconnecting only on actual failure."""
    try:
        ssh.exec_command("echo ping", timeout=2)
        return True
    except Exception:
        try:
            ssh.connect(hostname=HOST, port=PORT, username=USER, password=PASSWORD, timeout=3)
            return True
        except Exception:
            return False


def main():
    init_db()
    print(f"Database initialized. High-speed Netdata metrics collector running ({COLLECT_INTERVAL_SECONDS}s interval)...", flush=True)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    last_ssh_check = 0
    cached_host_metrics = {}

    while True:
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        scraped_data = {}
        now = time.time()

        # 1. Scrape Netdata HTTP directly (fast: ~10-20ms)
        netdata_raw = None
        try:
            r = requests.get(NETDATA_URL, timeout=2)
            if r.status_code == 200:
                netdata_raw = r.json()
        except Exception:
            pass

        # 2. Poll Ceph CLI metrics every 6 seconds via persistent SSH connection
        if now - last_ssh_check >= 6:
            if _ensure_ssh_connected(ssh):
                if not netdata_raw:
                    netdata_raw = fetch_netdata_via_ssh(ssh)
                cached_host_metrics = scrape_host_metrics(ssh)
            last_ssh_check = now

        if netdata_raw:
            scraped_data.update(parse_netdata_metrics(netdata_raw))
        scraped_data.update(cached_host_metrics)

        # Save snapshot to SQLite
        if scraped_data:
            try:
                conn = sqlite3.connect(DB_PATH)
                conn.execute(
                    "INSERT INTO metrics_timeseries (timestamp, data, is_baseline) VALUES (?, ?, 0)",
                    (timestamp, json.dumps(scraped_data))
                )
                conn.execute(
                    "DELETE FROM metrics_timeseries WHERE is_baseline=0 AND timestamp < datetime('now', '-7 days')"
                )
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"[{timestamp}] DB Save error: {e}", flush=True)

        time.sleep(COLLECT_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
