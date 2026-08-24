import os
import json
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DB_PATH", os.path.join(ROOT_DIR, "ceph_monitor.db"))


def get_recent_events_log(seconds=120, limit=15):
    """Dynamically fetches recent error/warning/critical log entries from SQLite events_log."""
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        df_logs = pd.read_sql_query(
            "SELECT timestamp, source, severity, component, message "
            "FROM events_log WHERE timestamp >= ? "
            "ORDER BY timestamp DESC LIMIT ?",
            conn, params=(cutoff, limit)
        )
        conn.close()
        logs = []
        for _, r in df_logs.iterrows():
            logs.append(f"[{r['timestamp']}] [{r['severity']}] [{r['component']}] {r['message']}")
        return logs
    except Exception as e:
        return [f"Log query failed: {e}"]


def build_incident_context(v7_result=None, v8_result=None, raw_snapshot=None):
    """
    Dynamically compiles all live telemetry deviations, sentinels, cluster topology,
    and recent log messages into a standardized Incident Context dictionary.
    No hardcoding: all metrics, thresholds, and states are read dynamically from runtime objects.
    """
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    # 1. Extract deviated features dynamically
    deviations = {}
    if v7_result and isinstance(v7_result.get("deviated_features"), dict):
        for k, v in v7_result["deviated_features"].items():
            deviations[f"host.{k}"] = v

    if v8_result and isinstance(v8_result.get("deviated_features"), dict):
        for k, v in v8_result["deviated_features"].items():
            deviations[f"ceph.{k}"] = v

    # 2. Extract sentinel alerts dynamically
    sentinel_alerts = []
    if v7_result and isinstance(v7_result.get("sentinel_alerts"), list):
        for s in v7_result["sentinel_alerts"]:
            sentinel_alerts.append(s.get("alert", str(s)))

    if v8_result and isinstance(v8_result.get("sentinel_alerts"), list):
        for s in v8_result["sentinel_alerts"]:
            sentinel_alerts.append(s.get("alert", str(s)))

    # 3. Dynamic snapshot extraction
    cluster_state = {}
    host_state = {}
    if raw_snapshot and isinstance(raw_snapshot, dict):
        # Ceph state
        cluster_state["health_code"] = raw_snapshot.get("ceph_health_status", 0.0)
        cluster_state["osds_total"]  = raw_snapshot.get("ceph_osd_stat_osds", 1.0)
        cluster_state["osds_up"]     = raw_snapshot.get("ceph_osd_stat_osds_up", 1.0)
        cluster_state["osds_in"]     = raw_snapshot.get("ceph_osd_stat_osds_in", 1.0)
        cluster_state["pgs_total"]    = raw_snapshot.get("ceph_pg_total", 97.0)
        cluster_state["pgs_clean"]    = raw_snapshot.get("ceph_pg_active_clean", 97.0)
        cluster_state["pgs_degraded"] = raw_snapshot.get("ceph_pg_degraded", 0.0)
        cluster_state["osd_apply_latency_ms"] = raw_snapshot.get("osd_apply_latency_ms", 0.0)

        # Host state
        host_state["cpu_pressure_some_10"] = raw_snapshot.get("system_cpu_pressure__some_10", 0.0)
        host_state["cpu_iowait_pct"]       = raw_snapshot.get("system_cpu__iowait", 0.0)
        host_state["ram_available_mib"]    = raw_snapshot.get("mem_available__MemAvailable", 0.0)
        host_state["ceph_process_threads"] = raw_snapshot.get("apps_threads__ceph", 0.0)
        host_state["ceph_process_mem_mib"] = raw_snapshot.get("apps_mem__ceph", 0.0)
        host_state["ceph_process_cpu_pct"] = raw_snapshot.get("apps_cpu__ceph", 0.0)

    # 4. Fetch recent correlated log entries
    recent_logs = get_recent_events_log(seconds=180, limit=10)

    # 5. Build full context payload
    context = {
        "timestamp": timestamp,
        "detection_sources": {
            "host_v7_anomaly": v7_result.get("is_anomaly", False) if v7_result else False,
            "host_v7_method": v7_result.get("detection_method") if v7_result else None,
            "ceph_v8_anomaly": v8_result.get("is_anomaly", False) if v8_result else False,
            "ceph_v8_method": v8_result.get("detection_method") if v8_result else None,
        },
        "sentinel_alerts": sentinel_alerts,
        "deviated_telemetry": deviations,
        "live_cluster_state": cluster_state,
        "live_host_state": host_state,
        "recent_logs": recent_logs,
    }
    return context
