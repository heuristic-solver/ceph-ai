import os
import json
import time
from datetime import datetime, timezone
import diagnostic_engine
import llm_analyst
import alert_engine

def run_rca_unit_tests():
    print("\n" + "=" * 80)
    print("      CEPH AI ROOT CAUSE ANALYSIS (RCA) DYNAMIC ENGINE VALIDATION")
    print("=" * 80)

    # Define 7 realistic synthetic telemetry snapshots reflecting our edge cases
    test_cases = [
        {
            "name": "1. Host CPU Thrashing",
            "v7_result": {
                "is_anomaly": True,
                "detection_method": "AI Majority Consensus (Scaled-IsolationForest, OneClassSVM)",
                "sentinel_alerts": [],
                "deviated_features": {
                    "kernel_cpu_pressure": {"current": 42.8, "baseline_mean": 0.1, "z_score": 14.5},
                    "ceph_cpu_pct": {"current": 88.5, "baseline_mean": 2.1, "z_score": 8.2}
                }
            },
            "snapshot": {
                "ceph_health_status": 0.0,
                "system_cpu_pressure__some_10": 42.8,
                "system_cpu__iowait": 0.5,
                "mem_available__MemAvailable": 3200.0,
                "ceph_osd_stat_osds": 1.0,
                "ceph_osd_stat_osds_up": 1.0,
                "ceph_pg_degraded": 0.0,
                "apps_threads__ceph": 9.0
            },
            "expected_category": "HOST_COMPUTE_THRASHING"
        },
        {
            "name": "2. Host RAM Starvation",
            "v7_result": {
                "is_anomaly": True,
                "detection_method": "Structural Sentinel Alert",
                "sentinel_alerts": [{"alert": "Available system RAM capacity critically depleted: 520.0 (thresh: 1200.0)"}],
                "deviated_features": {
                    "sys_ram_available_mib": {"current": 520.0, "baseline_mean": 3400.0, "z_score": 11.2}
                }
            },
            "snapshot": {
                "ceph_health_status": 0.0,
                "system_cpu_pressure__some_10": 0.0,
                "system_cpu__iowait": 1.2,
                "mem_available__MemAvailable": 520.0,
                "ceph_osd_stat_osds": 1.0,
                "ceph_osd_stat_osds_up": 1.0,
                "ceph_pg_degraded": 0.0,
                "apps_threads__ceph": 9.0
            },
            "expected_category": "HOST_MEMORY_STARVATION"
        },
        {
            "name": "3. Storage I/O Saturation",
            "v7_result": {
                "is_anomaly": True,
                "detection_method": "AI Majority Consensus (Scaled-IsolationForest, PCA-Reconstruction)",
                "sentinel_alerts": [],
                "deviated_features": {
                    "cpu_iowait_pct": {"current": 38.5, "baseline_mean": 0.2, "z_score": 18.0},
                    "storage_total_iobps": {"current": 184500000.0, "baseline_mean": 120000.0, "z_score": 12.4},
                    "disk_queue_depth": {"current": 44.0, "baseline_mean": 0.8, "z_score": 9.5}
                }
            },
            "snapshot": {
                "ceph_health_status": 0.0,
                "system_cpu_pressure__some_10": 1.5,
                "system_cpu__iowait": 38.5,
                "mem_available__MemAvailable": 2800.0,
                "ceph_osd_stat_osds": 1.0,
                "ceph_osd_stat_osds_up": 1.0,
                "ceph_pg_degraded": 0.0,
                "apps_threads__ceph": 9.0
            },
            "expected_category": "STORAGE_IO_SATURATION"
        },
        {
            "name": "4. OSD Administrative Down & Out",
            "v7_result": {
                "is_anomaly": True,
                "detection_method": "Structural Sentinel Alert",
                "sentinel_alerts": [{"alert": "OSDs in DOWN state = 1.0 (expected 0)"}],
                "deviated_features": {
                    "osd_down_count": {"current": 1.0, "baseline_mean": 0.0, "z_score": 99.9}
                }
            },
            "snapshot": {
                "ceph_health_status": 1.0,
                "system_cpu_pressure__some_10": 0.0,
                "system_cpu__iowait": 0.0,
                "mem_available__MemAvailable": 3100.0,
                "ceph_osd_stat_osds": 1.0,
                "ceph_osd_stat_osds_up": 0.0,
                "ceph_osd_stat_osds_in": 0.0,
                "ceph_pg_degraded": 97.0,
                "apps_threads__ceph": 9.0
            },
            "expected_category": "OSD_ADMIN_DOWN"
        },
        {
            "name": "5. OSD Daemon Process Crash",
            "v7_result": {
                "is_anomaly": True,
                "detection_method": "Structural Sentinel Alert",
                "sentinel_alerts": [
                    {"alert": "Ceph daemon worker thread count collapsed below floor: 0.0 (min: 1.0)"},
                    {"alert": "OSDs in DOWN state = 1.0 (expected 0)"}
                ],
                "deviated_features": {
                    "ceph_threads": {"current": 0.0, "baseline_mean": 9.0, "z_score": 25.0},
                    "osd_down_count": {"current": 1.0, "baseline_mean": 0.0, "z_score": 99.9}
                }
            },
            "snapshot": {
                "ceph_health_status": 2.0,
                "system_cpu_pressure__some_10": 0.0,
                "system_cpu__iowait": 0.0,
                "mem_available__MemAvailable": 3200.0,
                "ceph_osd_stat_osds": 1.0,
                "ceph_osd_stat_osds_up": 0.0,
                "ceph_osd_stat_osds_in": 1.0,
                "ceph_pg_degraded": 97.0,
                "apps_threads__ceph": 0.0
            },
            "expected_category": "OSD_PROCESS_CRASH"
        },
        {
            "name": "6. PG-Level Data Degradation",
            "v7_result": {
                "is_anomaly": True,
                "detection_method": "Structural Sentinel Alert",
                "sentinel_alerts": [{"alert": "Degraded Placement Groups = 97.0 (expected 0)"}],
                "deviated_features": {
                    "pg_degraded_count": {"current": 97.0, "baseline_mean": 0.0, "z_score": 99.9}
                }
            },
            "snapshot": {
                "ceph_health_status": 1.0,
                "system_cpu_pressure__some_10": 0.0,
                "system_cpu__iowait": 0.0,
                "mem_available__MemAvailable": 3000.0,
                "ceph_osd_stat_osds": 1.0,
                "ceph_osd_stat_osds_up": 1.0,
                "ceph_osd_stat_osds_in": 1.0,
                "ceph_pg_degraded": 97.0,
                "apps_threads__ceph": 9.0
            },
            "expected_category": "PG_DATA_DEGRADATION"
        },
        {
            "name": "7. Ceph Monitor Quorum Loss",
            "v7_result": {
                "is_anomaly": True,
                "detection_method": "Structural Sentinel Alert",
                "sentinel_alerts": [{"alert": "Cluster health (0=OK, 1=WARN, 2=ERR) = 2.0 (expected 0)"}],
                "deviated_features": {
                    "cluster_health_code": {"current": 2.0, "baseline_mean": 0.0, "z_score": 99.9}
                }
            },
            "snapshot": {
                "ceph_health_status": 2.0,
                "system_cpu_pressure__some_10": 0.0,
                "system_cpu__iowait": 0.0,
                "mem_available__MemAvailable": 3100.0,
                "ceph_osd_stat_osds": 0.0,
                "ceph_osd_stat_osds_up": 0.0,
                "ceph_osd_stat_osds_in": 0.0,
                "ceph_pg_degraded": 0.0,
                "apps_threads__ceph": 0.0
            },
            "expected_category": "CEPH_MON_QUORUM_LOSS"
        }
    ]

    results = []
    print(f"\nEvaluating {len(test_cases)} edge cases across dynamic diagnostic pipeline...\n")

    for tc in test_cases:
        ctx = diagnostic_engine.build_incident_context(
            v7_result=tc["v7_result"],
            raw_snapshot=tc["snapshot"]
        )
        diag = llm_analyst.diagnose_incident(ctx)
        actual_cat = diag.get("fault_category")
        matched = (actual_cat == tc["expected_category"])
        
        has_remediation = len(diag.get("remediation_steps", [])) > 0
        has_evidence = len(diag.get("evidence_chain", [])) > 0
        has_verification = bool(diag.get("verification_command"))
        
        pass_verdict = matched and has_remediation and has_evidence and has_verification

        results.append({
            "name": tc["name"],
            "expected": tc["expected_category"],
            "actual": actual_cat,
            "passed": pass_verdict,
            "diag": diag
        })

    # Render summary table
    print("=" * 80)
    print(f"{'#':<3} {'Scenario Name':<32} {'Expected':<24} {'Actual':<24} {'Verdict':<8}")
    print("-" * 80)
    for idx, r in enumerate(results, 1):
        v_str = "[PASS]" if r["passed"] else "[FAIL]"
        print(f"{idx:<3} {r['name']:<32} {r['expected']:<24} {r['actual']:<24} {v_str:<8}")
    print("=" * 80)

    # Render full RCA sample report for OSD Process Crash
    print("\n\n" + "=" * 80)
    print("     DETAILED RCA INCIDENT CARD SAMPLE (Scenario 5: OSD Process Crash)")
    print("=" * 80)
    alert_engine.print_rca_report(results[4]["diag"])

    all_passed = all(r["passed"] for r in results)
    print(f"\nFinal RCA Engine Verdict: {'ALL 7 TESTS PASSED (100% ACCURACY)' if all_passed else 'SOME TESTS FAILED'}\n")
    return all_passed


if __name__ == "__main__":
    run_rca_unit_tests()
