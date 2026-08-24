import os
import sys
import time
import sqlite3
import threading
from datetime import datetime, timedelta
import pandas as pd

import metrics_collector
import host_log_streamer
import ml_anomaly_detector
import llm_analyst
import alert_engine

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DB_PATH", os.path.join(ROOT_DIR, "ceph_monitor.db"))

# Prevent identical ML alerts from flooding the terminal
_last_anomaly_alert_ts = None
ANOMALY_COOLDOWN_SEC   = 120  # seconds between repeated ML alerts


def start_thread(target_func, name):
    t = threading.Thread(target=target_func, name=name, daemon=True)
    t.start()
    return t


def get_recent_logs(seconds=60):
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn   = sqlite3.connect(DB_PATH)
        cutoff = (datetime.utcnow() - timedelta(seconds=seconds)).isoformat() + "Z"
        df_logs = pd.read_sql_query(
            "SELECT timestamp, source, severity, component, message "
            "FROM events_log WHERE timestamp >= ? ORDER BY timestamp ASC",
            conn, params=(cutoff,)
        )
        conn.close()
        return [
            f"[{r['timestamp']}] {r['source']} | {r['severity']} | {r['component']} | {r['message']}"
            for _, r in df_logs.iterrows()
        ]
    except Exception as e:
        print(f"[orchestrator] Log query error: {e}", file=sys.stderr)
        return []


def handle_ml_anomaly(result):
    global _last_anomaly_alert_ts
    deviated = result.get("deviated_features", {})
    if not deviated:
        return

    now = datetime.now()
    if _last_anomaly_alert_ts:
        if (now - _last_anomaly_alert_ts).total_seconds() < ANOMALY_COOLDOWN_SEC:
            return
    _last_anomaly_alert_ts = now

    score  = result.get("decision_score", 0)
    method = result.get("detection_method", "ML")
    print(
        f"\n[!] ML anomaly detected via [{method}] (score={score:.4f}). "
        f"Deviations: {list(deviated.keys())}. Querying Gemma...",
        flush=True
    )

    llm_explanation = llm_analyst.explain_anomaly(deviated)

    if llm_explanation:
        alert_engine.print_alert(
            alert_type=f"ML Anomaly Detector [{method}] + Gemma",
            title=llm_explanation.get("title", "Metric Anomaly Detected"),
            explanation=llm_explanation.get(
                "explanation", "Metrics deviated significantly from the locked baseline."
            ),
            recommended_action=llm_explanation.get(
                "recommended_action", "Inspect Ceph cluster with 'ceph status'."
            ),
            severity=llm_explanation.get("severity", "WARNING"),
            timestamp=result.get("timestamp"),
        )
    else:
        # Ollama offline fallback
        alert_engine.print_alert(
            alert_type="ML Anomaly Detector",
            title="Anomalous Metrics Detected",
            explanation=(
                f"The following metrics deviated from the locked baseline: "
                f"{', '.join(deviated.keys())}. Anomaly score: {score:.4f}."
            ),
            recommended_action="Run 'ceph status' to inspect the cluster. "
                               "Start Ollama for AI-powered explanation.",
            severity="WARNING",
            timestamp=result.get("timestamp"),
        )


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

    print("=" * 72, flush=True)
    print("      CEPH AI FAULT MONITORING SYSTEM  —  ML + LLM EDITION           ", flush=True)
    print("=" * 72, flush=True)

    metrics_collector.init_db()

    print("Starting Metrics Collector thread...", flush=True)
    start_thread(metrics_collector.main, "metrics_collector")

    print("Starting Log Streamer thread...", flush=True)
    start_thread(host_log_streamer.main, "log_streamer")

    target = ml_anomaly_detector.MIN_BASELINE_SAMPLES
    print(
        f"\nSystem running.  ML baseline requires {target} samples "
        f"({target * 15 // 60}m {target * 15 % 60}s).",
        flush=True,
    )

    time.sleep(20)  # let the first metric scrape land

    last_log_check   = datetime.now()
    log_check_interval = 60

    try:
        while True:
            # ── ML Anomaly Detection (every 15 s) ──────────────────────────
            result = ml_anomaly_detector.detect_anomalies()

            if result:
                status  = result.get("status")
                samples = result.get("samples_collected", 0)

                if status == "collecting_data":
                    pct = int((samples / target) * 30)
                    bar = "█" * pct + "░" * (30 - pct)
                    print(
                        f"\r  Baseline [{bar}] {samples}/{target}", end="", flush=True
                    )
                elif result.get("is_anomaly"):
                    handle_ml_anomaly(result)

            # ── LLM Log Trend Analysis (every 60 s) ────────────────────────
            if (datetime.now() - last_log_check).total_seconds() >= log_check_interval:
                last_log_check = datetime.now()
                recent_logs    = get_recent_logs(seconds=log_check_interval)
                if recent_logs:
                    log_analysis = llm_analyst.analyze_log_window(recent_logs)
                    if log_analysis and log_analysis.get("health_issue_detected"):
                        alert_engine.print_alert(
                            alert_type="Gemma Log Sequence Analyst",
                            title=log_analysis.get("title", "Health Pattern in Logs"),
                            explanation=log_analysis.get("explanation", ""),
                            recommended_action=log_analysis.get("recommended_action", ""),
                            severity=log_analysis.get("severity", "INFO"),
                        )

            time.sleep(15)

    except KeyboardInterrupt:
        print("\nStopping. Goodbye!", flush=True)


if __name__ == "__main__":
    main()
