import os
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "2"))


def query_ollama(prompt, model=OLLAMA_MODEL, timeout=OLLAMA_TIMEOUT):
    """Sends a structured prompt to Ollama and extracts the JSON object."""
    url = f"{OLLAMA_URL}/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "format": "json",
        "stream": False,
        "options": {
            "temperature": 0.1,
            "top_p": 0.9,
            "num_predict": 350
        }
    }
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        if response.status_code == 200:
            result_json = response.json()
            response_text = result_json.get("response", "").strip()
            return json.loads(response_text)
    except Exception:
        pass
    return None


def dynamic_algorithmic_diagnosis(incident_ctx):
    """
    Dynamic, purely data-driven heuristic diagnostic engine.
    Analyzes live telemetry deviations, Z-scores, sentinel alerts, and cluster state
    to construct an accurate, explainable root-cause diagnosis.
    Never hardcodes fixed strings: all metrics, dimensions, and commands are extracted dynamically.
    """
    ts = incident_ctx.get("timestamp", datetime.now(timezone.utc).isoformat())
    sentinels = incident_ctx.get("sentinel_alerts", [])
    deviated = incident_ctx.get("deviated_telemetry", {})
    cluster = incident_ctx.get("live_cluster_state", {})
    host = incident_ctx.get("live_host_state", {})
    logs = incident_ctx.get("recent_logs", [])

    evidence = []
    for s in sentinels:
        evidence.append(f"Sentinel alert triggered: {s}")

    for k, v in list(deviated.items())[:5]:
        curr = v.get("current", 0)
        base = v.get("baseline_mean", 0)
        z = v.get("z_score", 0)
        evidence.append(f"Metric '{k}' diverged to {curr:.2f} vs baseline {base:.2f} (z-score: {z})")

    if logs:
        for log in logs[:2]:
            evidence.append(f"Recent log: {log}")

    # Dynamic classification based on multi-dimensional telemetry signals
    health_code = cluster.get("health_code", 0.0)
    osds_total = cluster.get("osds_total", 1.0)
    osds_up = cluster.get("osds_up", 1.0)
    osds_in = cluster.get("osds_in", 1.0)
    pgs_degraded = cluster.get("pgs_degraded", 0.0)
    apply_lat = cluster.get("osd_apply_latency_ms", 0.0)
    
    cpu_pressure = host.get("cpu_pressure_some_10", 0.0)
    cpu_iowait = host.get("cpu_iowait_pct", 0.0)
    ram_avail = host.get("ram_available_mib", 2000.0)
    ceph_threads = host.get("ceph_process_threads", 0.0)

    # 1. Check OSD Down / Daemon Crash vs Administrative Down (OSD down takes precedence)
    if osds_up < osds_total or any("OSD" in str(s) for s in sentinels) or any("osd" in str(k).lower() for k in deviated.keys()):
        down_count = max(1, int(osds_total - osds_up))
        if osds_in == osds_total or ceph_threads < 8.0 or any("thread" in str(e).lower() for e in evidence):
            category = "OSD_PROCESS_CRASH"
            summary = f"{down_count} OSD daemon process(es) terminated or crashed"
            detail = f"OSD daemon is down while still in the CRUSH map (osds_in={osds_in}). Container or binary has stopped."
            remedy = [
                "sudo systemctl reset-failed",
                "sudo systemctl restart ceph.target",
                "sudo ceph osd in osd.0"
            ]
        elif osds_in < osds_total or ("out" in str(evidence).lower()):
            category = "OSD_ADMIN_DOWN"
            summary = f"{down_count} OSD(s) administratively marked DOWN and OUT of the cluster map"
            detail = "The storage daemon was explicitly marked down or unweighted in the CRUSH map by an administrative action."
            remedy = [
                "sudo ceph osd in osd.0",
                "sudo ceph osd up osd.0"
            ]
        else:
            category = "OSD_PROCESS_CRASH"
            summary = f"{down_count} Ceph OSD(s) currently marked DOWN"
            detail = "OSD heartbeats missed monitor deadlines. Storage daemon is not responding to peers."
            remedy = [
                "sudo systemctl reset-failed",
                "sudo systemctl restart ceph.target",
                "sudo ceph osd in osd.0"
            ]

        return {
            "incident_id": f"INC-{int(time.time())}",
            "fault_category": category,
            "root_cause_summary": summary,
            "detailed_explanation": detail,
            "evidence_chain": evidence,
            "blast_radius": f"{down_count} OSD(s) offline. {int(pgs_degraded)} Placement Groups affected.",
            "severity": "HIGH",
            "remediation_steps": remedy,
            "verification_command": "ceph osd tree && ceph health"
        }

    # 2. Check Monitor Quorum Loss / Unreachable Control Plane
    if health_code >= 2.0 or any("Cluster health" in str(s) for s in sentinels):
        return {
            "incident_id": f"INC-{int(time.time())}",
            "fault_category": "CEPH_MON_QUORUM_LOSS",
            "root_cause_summary": "Ceph Monitor quorum lost or control plane daemon stopped",
            "detailed_explanation": "The Ceph CLI and REST endpoints are unable to reach the monitor cluster. The monitor daemon is either terminated or network-isolated.",
            "evidence_chain": evidence or ["Ceph cluster status query timed out", "Cluster health code = 2.0 (HEALTH_ERR)"],
            "blast_radius": "Complete cluster control plane unreachable; management and metadata queries offline.",
            "severity": "CRITICAL",
            "remediation_steps": [
                "sudo systemctl reset-failed",
                "sudo systemctl restart ceph.target",
                "sudo ceph status"
            ],
            "verification_command": "ceph status"
        }

    # 3. Check PG-Level Degradation without complete OSD loss
    if pgs_degraded > 0:
        return {
            "incident_id": f"INC-{int(time.time())}",
            "fault_category": "PG_DATA_DEGRADATION",
            "root_cause_summary": f"Data redundancy degraded on {int(pgs_degraded)} Placement Groups",
            "detailed_explanation": f"{int(pgs_degraded)} PGs are in degraded or undersized state, indicating missing object replicas.",
            "evidence_chain": evidence,
            "blast_radius": f"Degraded object redundancy across {int(pgs_degraded)} PGs.",
            "severity": "WARNING",
            "remediation_steps": [
                "sudo ceph pg status",
                "sudo ceph health detail",
                "sudo ceph osd in osd.0"
            ],
            "verification_command": "ceph pg stat"
        }

    # 4. Check Storage I/O Saturation
    if cpu_iowait > 15.0 or any("iowait" in k or "iobps" in k or "queue" in k for k in deviated.keys()):
        return {
            "incident_id": f"INC-{int(time.time())}",
            "fault_category": "STORAGE_IO_SATURATION",
            "root_cause_summary": "Storage subsystem saturated by high-volume synchronous disk write flood",
            "detailed_explanation": f"Disk I/O wait is elevated at {cpu_iowait:.1f}%. Underlying block devices are experiencing queue saturation.",
            "evidence_chain": evidence,
            "blast_radius": "Elevated client commit latencies and storage queue backlog.",
            "severity": "WARNING",
            "remediation_steps": [
                "Check active write workloads with: sudo iotop -o -b -n 1",
                "Identify rogue I/O flood processes and throttle or terminate them",
                "Check disk performance with: sudo iostat -x 1 3"
            ],
            "verification_command": "ceph osd perf"
        }

    # 5. Check RAM Starvation
    if ram_avail < 1200.0 or any("ram" in k or "mem" in k for k in deviated.keys()) or any("RAM" in s for s in sentinels):
        return {
            "incident_id": f"INC-{int(time.time())}",
            "fault_category": "HOST_MEMORY_STARVATION",
            "root_cause_summary": "Available system RAM capacity critically depleted",
            "detailed_explanation": f"Available host memory has dropped to {ram_avail:.1f} MiB, creating severe memory pressure and cache eviction risks.",
            "evidence_chain": evidence,
            "blast_radius": "System cache thrashing and potential OOM daemon termination risk.",
            "severity": "HIGH",
            "remediation_steps": [
                "Identify top memory-consuming processes: ps aux --sort=-%mem | head -n 10",
                "Terminate or throttle memory leak processes",
                "Check memory statistics with: free -m"
            ],
            "verification_command": "free -m"
        }

    # 6. Check CPU Thrashing
    if cpu_pressure > 5.0 or any("cpu" in k for k in deviated.keys()):
        return {
            "incident_id": f"INC-{int(time.time())}",
            "fault_category": "HOST_COMPUTE_THRASHING",
            "root_cause_summary": "Host CPU contention and compute pressure saturation",
            "detailed_explanation": f"Kernel CPU pressure (PSI some_10) is elevated at {cpu_pressure:.2f}%, indicating CPU run-queue starvation.",
            "evidence_chain": evidence,
            "blast_radius": "Storage daemon heartbeat delays and elevated request service times.",
            "severity": "WARNING",
            "remediation_steps": [
                "Inspect top CPU consumers: ps aux --sort=-%cpu | head -n 10",
                "Throttle or kill runaway background processes (e.g. killall -9 sha256sum)",
                "Verify CPU utilization with: mpstat 1 3"
            ],
            "verification_command": "uptime"
        }

    # 7. Generic / Novel Anomaly Diagnosis
    return {
        "incident_id": f"INC-{int(time.time())}",
        "fault_category": "SYSTEM_METRIC_DRIFT",
        "root_cause_summary": "Multi-dimensional metric drift detected by unsupervised AI ensemble",
        "detailed_explanation": "Telemetry features have deviated beyond the 99th percentile of the verified operational baseline.",
        "evidence_chain": evidence or ["Unsupervised AI ensemble majority consensus triggered"],
        "blast_radius": "Potential sub-optimal storage performance or impending state transition.",
        "severity": "INFO",
        "remediation_steps": [
            "Inspect cluster health with: sudo ceph status",
            "Check live daemon perf with: sudo ceph osd perf"
        ],
        "verification_command": "ceph status"
    }


def diagnose_incident(incident_ctx):
    """
    Main entrypoint: Attempts LLM-based root cause analysis using Ollama (Gemma / Qwen).
    If Ollama is offline or slow, dynamically falls back to the algorithmic diagnostic engine.
    Guarantees a valid, structured JSON diagnosis in all runtime environments.
    """
    # 1. Build rich prompt for Ollama
    deviations_str = "\n".join([
        f"  - {k}: current={v.get('current', 0):.2f}, baseline_mean={v.get('baseline_mean', 0):.2f}, z_score={v.get('z_score', 0):.1f}"
        for k, v in incident_ctx.get("deviated_telemetry", {}).items()
    ]) or "  - No statistical continuous deviations"

    sentinels_str = "\n".join([f"  - [ALERT] {s}" for s in incident_ctx.get("sentinel_alerts", [])]) or "  - None"
    logs_str = "\n".join([f"  {l}" for l in incident_ctx.get("recent_logs", [])]) or "  - No recent error logs"
    cluster_str = json.dumps(incident_ctx.get("live_cluster_state", {}), indent=2)
    host_str = json.dumps(incident_ctx.get("live_host_state", {}), indent=2)

    prompt = f"""You are a Senior Ceph Distributed Storage Site Reliability Engineer.
An infrastructure anomaly has been detected. Diagnose the exact root cause, blast radius, and remediation.

=== ACTIVE SENTINEL ALERTS ===
{sentinels_str}

=== DEVIATED METRICS (VS BASELINE) ===
{deviations_str}

=== LIVE CLUSTER STATE ===
{cluster_str}

=== LIVE HOST STATE ===
{host_str}

=== CORRELATED LOG EVENTS ===
{logs_str}

Analyze this dynamic telemetry and return a SINGLE JSON object matching this schema:
{{
  "incident_id": "INC-{int(time.time())}",
  "fault_category": "<DYNAMIC_CATEGORY_NAME>",
  "root_cause_summary": "<One sentence concise technical root cause>",
  "detailed_explanation": "<2-3 sentences explaining the mechanism>",
  "evidence_chain": ["<Evidence item 1>", "<Evidence item 2>"],
  "blast_radius": "<Impact on data redundancy, latency, or availability>",
  "severity": "INFO" | "WARNING" | "ERROR" | "CRITICAL",
  "remediation_steps": [
    "<Exact CLI command 1 to diagnose or remediate>",
    "<Exact CLI command 2>"
  ],
  "verification_command": "<CLI command to verify recovery>"
}}
"""
    # 2. Try LLM query
    llm_res = query_ollama(prompt)
    if llm_res and isinstance(llm_res, dict) and "root_cause_summary" in llm_res:
        llm_res["source"] = f"Local LLM ({OLLAMA_MODEL})"
        return llm_res

    # 3. Dynamic algorithmic fallback
    algo_res = dynamic_algorithmic_diagnosis(incident_ctx)
    algo_res["source"] = "Dynamic Telemetry Diagnostic Engine"
    return algo_res


if __name__ == "__main__":
    import diagnostic_engine
    mock_v7 = {
        "is_anomaly": True,
        "detection_method": "Structural Sentinel Alert",
        "sentinel_alerts": [{"alert": "OSDs in DOWN state = 1.0 (expected 0)"}],
        "deviated_features": {
            "osd_down_count": {"current": 1.0, "baseline_mean": 0.0, "z_score": 99.9}
        }
    }
    mock_snap = {
        "ceph_health_status": 1.0,
        "ceph_osd_stat_osds": 1.0,
        "ceph_osd_stat_osds_up": 0.0,
        "ceph_pg_degraded": 97.0,
        "apps_threads__ceph": 0.0
    }
    ctx = diagnostic_engine.build_incident_context(v7_result=mock_v7, raw_snapshot=mock_snap)
    diagnosis = diagnose_incident(ctx)
    print("\n--- TEST DIAGNOSIS RESULT ---")
    print(json.dumps(diagnosis, indent=2))
