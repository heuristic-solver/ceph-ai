"""
Ceph-Semantic Baseline Trainer (v8) -- Phase 1 + 2
===================================================
Reads rows already saved in metrics_timeseries by the existing
metrics_collector.py, filters them down to Ceph-specific Netdata /
SSH-scraped keys, and trains a new unsupervised ensemble saved as
ceph_semantic_model.pkl.

The existing ml_model_state.pkl and ml_anomaly_detector.py are NEVER touched.

Typical workflow:
    python baseline_injector.py         # (existing) runs cluster workout, fills DB
    python ceph_semantic_baseline.py    # (new) trains ceph_semantic_model.pkl

    # Optional pre-training audit:
    python ceph_semantic_baseline.py --audit

    # Optional single detection pass:
    python ceph_semantic_baseline.py --detect
"""

import os
import re
import sys
import json
import pickle
import sqlite3
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from sklearn.preprocessing import RobustScaler
from sklearn.feature_selection import VarianceThreshold
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.decomposition import PCA
from dotenv import load_dotenv
import sklearn

load_dotenv()

ROOT_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.getenv("DB_PATH", os.path.join(ROOT_DIR, "ceph_monitor.db"))
MODEL_PATH = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "ceph_semantic_model.pkl")
MODEL_VERSION = f"ceph-semantic-sklearn-{sklearn.__version__}-v1"

# Minimum clean baseline samples required before ensemble trains and locks
MIN_BASELINE_SAMPLES = 30

# Rolling evaluation window for detect_anomalies()
EVAL_WINDOW_MINUTES = 15

# -- Ceph-semantic key patterns ------------------------------------------------
# These regexes are matched against the flat metric keys stored in
# metrics_timeseries.data JSON blobs.
#
# Convention used by metrics_collector.py:
#   Netdata charts  -> "{chart_name}__{dimension_name}"  (special chars -> _)
#   Ceph CLI (SSH)  -> custom keys like ceph_health_status, ceph_pg_degraded, ...
#
CEPH_KEY_PATTERNS = [
    # -- Netdata Ceph plugin charts --
    r"^ceph_osd",                   # ceph_osd_latency__*, ceph_osd_status__*, ceph_osd_io__*
    r"^ceph_pg",                    # ceph_pg_status__*, ceph_pg_total, ceph_pg_degraded
    r"^ceph_cluster",               # ceph_cluster_disk__*, ceph_cluster_read_bytes__*
    r"^ceph_net",                   # ceph_net_* health check counters
    r"^ceph_check",                 # ceph_check_* health-check specific keys
    # -- SSH-scraped Ceph CLI keys --
    r"^ceph_health",                # ceph_health_status  (0=OK, 1=WARN, 2=ERR)
    r"^ceph_osd_stat",              # ceph_osd_stat_osds, ceph_osd_stat_osds_up, _in
    r"^ceph_osd_in__",              # per-OSD in flags
    r"^ceph_osd_up__",              # per-OSD up flags
    r"^ceph_osd_apply_latency__",   # per-OSD apply latency from SSH
    r"^osd_perf",                   # osd_perf_commit_ms_*, osd_perf_apply_ms_*
    r"^pg_",                        # pg_degraded_count (from SSH scraper)
    r"^disk_io_queue",              # disk_io_queue_depth (from SSH /proc/diskstats)
    # -- Netdata Ceph process charts (process-level activity) --
    r"^apps_lreads__ceph",
    r"^apps_lwrites__ceph",
    r"^apps_pwrites__ceph",
    r"^apps_mem__ceph",
    r"^apps_cpu__ceph",
    r"^apps_threads__ceph",
    r"^apps_minor_faults__ceph",
]

# -- In-memory model state (lazy-loaded on first detect call) -----------------
_model_cache = {
    "ensemble":     None,
    "feature_cols": None,
    "baseline_X":   None,
}
_consecutive_anomalies = 0   # temporal persistence filter (mirrors v7 design)


# -- Utilities -----------------------------------------------------------------

def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_is_baseline_col(db_path):
    """
    Safely adds the `is_baseline` column to metrics_timeseries if it is
    missing.  The existing init_db() does not declare it, but the column must
    exist for our baseline queries to work.  This is idempotent -- if the
    column already exists SQLite raises OperationalError which we swallow.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "ALTER TABLE metrics_timeseries ADD COLUMN is_baseline INTEGER DEFAULT 0"
        )
        conn.commit()
        conn.close()
        print("[SEMANTIC] Added missing `is_baseline` column to metrics_timeseries.", flush=True)
    except Exception:
        # Column already exists -- this is the expected happy path on a live DB
        pass


def _key_is_ceph(key):
    """Returns True if `key` matches any CEPH_KEY_PATTERNS regex."""
    return any(re.match(pat, key) for pat in CEPH_KEY_PATTERNS)


# -- Feature discovery ---------------------------------------------------------

def discover_ceph_features(db_path=DB_PATH, n_rows=500):
    """
    Scans the most recent `n_rows` rows in metrics_timeseries and returns
    a sorted list of Ceph-semantic keys that have non-negligible variance.
    Also prints a summary so the user can verify Netdata coverage.
    """
    _ensure_is_baseline_col(db_path)
    try:
        conn = sqlite3.connect(db_path)
        df_raw = pd.read_sql_query(
            f"SELECT data FROM metrics_timeseries ORDER BY timestamp DESC LIMIT {n_rows}",
            conn,
        )
        conn.close()
    except Exception as e:
        print(f"[SEMANTIC] DB read error during feature discovery: {e}")
        return []

    rows = []
    for _, row in df_raw.iterrows():
        try:
            rows.append(json.loads(row["data"]))
        except Exception:
            continue

    if not rows:
        print("[SEMANTIC] No rows found in DB for feature discovery.")
        return []

    df = pd.DataFrame(rows).fillna(0.0)

    # Filter to Ceph-semantic keys present in the data
    ceph_cols = sorted(c for c in df.columns if _key_is_ceph(c))

    print(f"\n[SEMANTIC] Feature audit ({len(rows)} rows sampled):", flush=True)
    print(f"  Total metric keys in DB  : {len(df.columns)}", flush=True)
    print(f"  Ceph-semantic key matches: {len(ceph_cols)}", flush=True)

    if not ceph_cols:
        print(
            "[SEMANTIC] WARNING: Zero Ceph-semantic keys found in recent metrics!\n"
            "[SEMANTIC] Ensure either:\n"
            "[SEMANTIC]   (a) Netdata's Ceph plugin is enabled on the VM, OR\n"
            "[SEMANTIC]   (b) metrics_collector.py SSH-Ceph scraping is working.",
            flush=True,
        )
        return []

    # Remove near-zero-variance features (constants in this sample window)
    df_ceph = df[ceph_cols].astype(float).fillna(0.0)
    selector = VarianceThreshold(threshold=1e-6)
    try:
        selector.fit(df_ceph)
        active_mask = selector.get_support()
        active_cols = sorted(df_ceph.columns[active_mask].tolist())
        flat_cols   = sorted(df_ceph.columns[~active_mask].tolist())
        if flat_cols:
            print(f"  Removed {len(flat_cols)} constant keys: {flat_cols[:5]}{'...' if len(flat_cols)>5 else ''}", flush=True)
    except Exception:
        active_cols = ceph_cols

    print(f"  Keys with variance        : {len(active_cols)}", flush=True)
    return active_cols


# -- Data loading & feature extraction ----------------------------------------

def _load_rows_from_db(db_path, where_clause="1=1", limit=None):
    """
    Generic helper -- returns (timestamps, rows_as_dicts) or ([], []) on error.
    """
    limit_clause = f"LIMIT {limit}" if limit else ""
    try:
        conn = sqlite3.connect(db_path)
        df_raw = pd.read_sql_query(
            f"SELECT timestamp, data FROM metrics_timeseries "
            f"WHERE {where_clause} ORDER BY timestamp ASC {limit_clause}",
            conn,
        )
        conn.close()
    except Exception as e:
        print(f"[SEMANTIC] DB read error: {e}")
        return [], []

    timestamps, rows = [], []
    for _, row in df_raw.iterrows():
        try:
            rows.append(json.loads(row["data"]))
            timestamps.append(row["timestamp"])
        except Exception:
            continue
    return timestamps, rows


def extract_semantic_features(df, feature_cols):
    """
    Projects a DataFrame of raw metric rows onto `feature_cols`.
    Missing keys are filled with 0.0 (mirrors behaviour during mon outage
    where Ceph CLI keys are absent but model still needs a vector).
    """
    out = pd.DataFrame(index=df.index)
    for col in feature_cols:
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            out[col] = 0.0
    return out


# -- Model persistence ---------------------------------------------------------

def _save_model(ensemble, feature_cols, baseline_X):
    """Persist the trained semantic ensemble to ceph_semantic_model.pkl."""
    try:
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(
                {
                    "version":      MODEL_VERSION,
                    "ensemble":     ensemble,
                    "feature_cols": feature_cols,
                    "baseline_X":   baseline_X,
                },
                f,
            )
        print(f"[SEMANTIC] [OK] Model saved -> {MODEL_PATH}", flush=True)
    except Exception as e:
        print(f"[SEMANTIC] Warning: Could not save model: {e}")


def _load_model():
    """
    Lazy-loads the persisted model into _model_cache.
    Returns True if a valid model is now in cache, False otherwise.
    """
    if _model_cache["ensemble"] is not None:
        return True
    if not os.path.exists(MODEL_PATH):
        return False
    try:
        with open(MODEL_PATH, "rb") as f:
            saved = pickle.load(f)
        if saved.get("version") != MODEL_VERSION:
            print(
                f"[SEMANTIC] Model version mismatch "
                f"({saved.get('version')} vs {MODEL_VERSION}). Retraining needed.",
                flush=True,
            )
            return False
        _model_cache["ensemble"]     = saved["ensemble"]
        _model_cache["feature_cols"] = saved["feature_cols"]
        _model_cache["baseline_X"]   = saved["baseline_X"]
        print(
            f"[SEMANTIC] Loaded model from disk. "
            f"Features: {len(_model_cache['feature_cols'])}",
            flush=True,
        )
        return True
    except Exception as e:
        print(f"[SEMANTIC] Failed to load model: {e}")
        return False


# -- Training pipeline ---------------------------------------------------------

def train_semantic_model(db_path=DB_PATH):
    """
    Full training pipeline:
      1.  Discover Ceph features dynamically from recent DB rows
      2.  Load all is_baseline=1 rows
      3.  Extract + clean feature matrix
      4.  Train: RobustScaler -> IsolationForest + OneClassSVM + PCA
      5.  Persist to ceph_semantic_model.pkl
    Returns the ensemble dict, or None on failure.
    """
    global _model_cache

    _ensure_is_baseline_col(db_path)

    print("\n" + "=" * 72, flush=True)
    print("  CEPH SEMANTIC BASELINE TRAINER (v8)", flush=True)
    print("=" * 72, flush=True)

    # -- Step 1: Feature discovery --------------------------------------------
    feature_cols = discover_ceph_features(db_path)
    if not feature_cols:
        print(
            "[SEMANTIC] ERROR: No usable Ceph-semantic features found.\n"
            "[SEMANTIC] Run metrics_collector.py for >=2 minutes, then retry.",
        )
        return None

    # -- Step 2: Load baseline rows --------------------------------------------
    _, base_rows = _load_rows_from_db(db_path, where_clause="is_baseline = 1")
    if not base_rows:
        print(
            "[SEMANTIC] ERROR: No baseline rows found (is_baseline=1).\n"
            "[SEMANTIC] Run baseline_injector.py first, then retry.",
        )
        return None

    df_base = pd.DataFrame(base_rows)
    print(f"\n[SEMANTIC] Baseline rows loaded: {len(df_base)}", flush=True)

    # -- Step 3: Feature extraction + integrity filter -------------------------
    X_base = extract_semantic_features(df_base, feature_cols)

    # Drop rows where ALL Ceph features are zero (scrape failures)
    all_zero_mask = (X_base == 0.0).all(axis=1)
    X_base = X_base[~all_zero_mask].copy()
    print(
        f"[SEMANTIC] Clean baseline rows (non-zero): {len(X_base)} "
        f"(dropped {all_zero_mask.sum()} all-zero rows)",
        flush=True,
    )

    if len(X_base) < MIN_BASELINE_SAMPLES:
        print(
            f"[SEMANTIC] ERROR: Need >={MIN_BASELINE_SAMPLES} clean baseline rows. "
            f"Have {len(X_base)}.\n"
            f"[SEMANTIC] Run baseline_injector.py and ensure Ceph metrics are flowing.",
        )
        return None

    # -- Step 4: Second-pass variance filter on actual baseline ----------------
    selector = VarianceThreshold(threshold=0.01)
    try:
        selector.fit(X_base)
        active_mask = selector.get_support()
        active_cols = X_base.columns[active_mask].tolist()
        removed     = X_base.columns[~active_mask].tolist()
        if removed:
            print(
                f"[SEMANTIC] Removed {len(removed)} near-constant features "
                f"(variance<0.01): {removed}",
                flush=True,
            )
    except Exception:
        active_cols = list(X_base.columns)

    if not active_cols:
        print(
            "[SEMANTIC] ERROR: All Ceph features are constant in the baseline.\n"
            "[SEMANTIC] Ensure baseline_injector.py ran workloads (write/read bursts).",
        )
        return None

    X_active = X_base[active_cols].copy()
    print(f"[SEMANTIC] Active ML features ({len(active_cols)}): {active_cols}", flush=True)

    # -- Step 5: Scale ---------------------------------------------------------
    scaler = RobustScaler(quantile_range=(10.0, 90.0))
    X_scaled = scaler.fit_transform(X_active)

    # Apply operational allowance floor (mirrors v7 design -- prevents drift FP)
    min_scales = np.maximum(0.25 * np.abs(scaler.center_), 1.0)
    for idx, col in enumerate(active_cols):
        if 'minor_faults' in col:
            min_scales[idx] = max(min_scales[idx], 50.0)
        elif 'cpu' in col:
            min_scales[idx] = max(min_scales[idx], 10.0)
        elif 'mem' in col or 'ram' in col:
            min_scales[idx] = max(min_scales[idx], 100.0)
        elif 'threads' in col:
            min_scales[idx] = max(min_scales[idx], 100.0)
        elif 'reads' in col or 'writes' in col:
            min_scales[idx] = max(min_scales[idx], 100.0)
        elif 'pressure' in col or 'iowait' in col:
            min_scales[idx] = max(min_scales[idx], 2.0)
    scaler.scale_ = np.maximum(scaler.scale_, min_scales)
    X_scaled = scaler.transform(X_active)   # re-transform with floored scale

    # -- Step 6: Isolation Forest ----------------------------------------------
    iso = IsolationForest(contamination=0.01, n_estimators=150, random_state=42)
    iso.fit(X_scaled)
    print("[SEMANTIC] [OK] Isolation Forest trained.", flush=True)

    # -- Step 7: One-Class SVM (with PCA pre-reduction if needed) -------------
    pca_pre_svm = None
    svm_input   = X_scaled
    n_samples, n_feats = X_scaled.shape
    if n_feats > 0 and n_samples < 10 * n_feats:
        target_dim  = max(1, n_samples // 10)
        pca_pre_svm = PCA(n_components=target_dim, random_state=42)
        svm_input   = pca_pre_svm.fit_transform(X_scaled)
        print(
            f"[SEMANTIC] SVM PCA pre-reduction: {n_feats}->{target_dim} components.",
            flush=True,
        )
    svm = OneClassSVM(kernel="rbf", gamma="scale", nu=0.01)
    svm.fit(svm_input)
    print("[SEMANTIC] [OK] One-Class SVM trained.", flush=True)

    # -- Step 8: PCA Reconstruction (moving-block bootstrap P95 threshold) -----
    n_components = 0.90 if n_feats > 1 else 1
    pca = PCA(n_components=n_components, random_state=42)
    pca.fit(X_scaled)
    recon      = pca.inverse_transform(pca.transform(X_scaled))
    base_errs  = np.mean((X_scaled - recon) ** 2, axis=1)

    block_size     = max(5, n_samples // 10)
    bootstrap_p95s = []
    for _ in range(100):
        starts   = np.random.randint(0, max(1, n_samples - block_size + 1),
                                     size=max(1, n_samples // block_size + 1))
        resampled = np.concatenate([base_errs[s: s + block_size] for s in starts])
        bootstrap_p95s.append(np.percentile(resampled[:n_samples], 95))
    pca_threshold = max(float(np.median(bootstrap_p95s)) * 10.0, 5.0)
    print(
        f"[SEMANTIC] [OK] PCA trained. "
        f"Explained variance: {pca.explained_variance_ratio_.sum():.2%}  "
        f"Threshold: {pca_threshold:.4f}",
        flush=True,
    )

    # -- Step 9: Assemble and persist -----------------------------------------
    ensemble = {
        "scaler":       scaler,
        "iso_forest":   iso,
        "one_class_svm": svm,
        "pca":          pca,
        "pca_threshold": pca_threshold,
        "pca_pre_svm":  pca_pre_svm,
    }

    _model_cache["ensemble"]     = ensemble
    _model_cache["feature_cols"] = active_cols
    _model_cache["baseline_X"]   = X_active

    _save_model(ensemble, active_cols, X_active)

    print(
        f"\n[SEMANTIC] == Ensemble locked. "
        f"{len(active_cols)} active features, "
        f"{len(X_active)} baseline samples. ==",
        flush=True,
    )
    return ensemble


# -- Detection -----------------------------------------------------------------

def detect_anomalies(db_path=DB_PATH):
    """
    Main detection function -- mirrors the interface of
    ml_anomaly_detector.detect_anomalies() for drop-in use in test harnesses.

    Returns a dict with at minimum:
      is_anomaly       bool
      decision_score   float   (Isolation Forest score)
      detection_method str | None
      deviated_features dict
      timestamp        str
      status           str
    """
    global _consecutive_anomalies

    if not os.path.exists(db_path):
        return None

    if not _load_model():
        return {
            "status":     "no_model",
            "is_anomaly": False,
            "message": (
                "ceph_semantic_model.pkl not found. "
                "Run: python ceph_semantic_baseline.py"
            ),
            "timestamp": _now_iso(),
        }

    ensemble     = _model_cache["ensemble"]
    feature_cols = _model_cache["feature_cols"]
    baseline_X   = _model_cache["baseline_X"]

    # -- Fetch most recent monitoring row -------------------------------------
    cutoff = (
        datetime.now(timezone.utc) - timedelta(minutes=EVAL_WINDOW_MINUTES)
    ).isoformat().replace("+00:00", "Z")

    try:
        conn = sqlite3.connect(db_path)
        df_eval = pd.read_sql_query(
            "SELECT timestamp, data FROM metrics_timeseries "
            "WHERE is_baseline = 0 AND timestamp >= ? "
            "ORDER BY timestamp DESC LIMIT 1",
            conn,
            params=(cutoff,),
        )
        if df_eval.empty:
            df_eval = pd.read_sql_query(
                "SELECT timestamp, data FROM metrics_timeseries "
                "WHERE is_baseline = 0 ORDER BY timestamp DESC LIMIT 1",
                conn,
            )
        conn.close()
    except Exception as e:
        print(f"[SEMANTIC] DB read error: {e}")
        return None

    if df_eval.empty:
        return {"status": "no_eval_data", "is_anomaly": False, "timestamp": _now_iso()}

    try:
        eval_flat = json.loads(df_eval.iloc[0]["data"])
        eval_ts   = df_eval.iloc[0]["timestamp"]
    except Exception:
        return None

    df_point = pd.DataFrame([eval_flat])
    X_eval   = extract_semantic_features(df_point, feature_cols)
    x_scaled = ensemble["scaler"].transform(X_eval)

    # -- Isolation Forest ------------------------------------------------------
    if_score = float(ensemble["iso_forest"].decision_function(x_scaled)[0])
    if_anom  = bool(ensemble["iso_forest"].predict(x_scaled)[0] == -1)

    # -- One-Class SVM ---------------------------------------------------------
    svm_input = x_scaled
    if ensemble.get("pca_pre_svm") is not None:
        svm_input = ensemble["pca_pre_svm"].transform(x_scaled)
    svm_anom = bool(ensemble["one_class_svm"].predict(svm_input)[0] == -1)

    # -- PCA Reconstruction ----------------------------------------------------
    recon   = ensemble["pca"].inverse_transform(ensemble["pca"].transform(x_scaled))
    pca_err = float(np.mean((x_scaled - recon) ** 2))
    pca_anom = bool(pca_err > ensemble["pca_threshold"])

    # -- Majority vote ---------------------------------------------------------
    triggered = []
    if if_anom:  triggered.append("Scaled-IsolationForest")
    if svm_anom: triggered.append("OneClassSVM")
    if pca_anom: triggered.append("PCA-Reconstruction")

    raw_consensus = len(triggered) >= 2

    # Temporal persistence filter: require 2 consecutive consensus windows
    if raw_consensus:
        _consecutive_anomalies += 1
    else:
        _consecutive_anomalies = 0

    is_anomaly = raw_consensus and _consecutive_anomalies >= 2

    # -- Feature attribution (Z-score, for explanation only) -------------------
    deviated = {}
    if is_anomaly:
        for col in feature_cols:
            if col not in baseline_X.columns:
                continue
            mean = baseline_X[col].mean()
            std  = baseline_X[col].std() + 1e-9
            curr = float(X_eval[col].iloc[0])
            z    = abs(curr - mean) / std
            if z >= 3.0:
                deviated[col] = {
                    "current":       round(curr, 4),
                    "baseline_mean": round(float(mean), 4),
                    "z_score":       round(float(z), 2),
                }

        # If no 3sigma hit but anomaly, surface top-3 most-deviated for context
        if not deviated:
            z_scores = []
            for col in feature_cols:
                if col not in baseline_X.columns:
                    continue
                mean = baseline_X[col].mean()
                std  = baseline_X[col].std() + 1e-9
                curr = float(X_eval[col].iloc[0])
                z_scores.append((col, abs(curr - mean) / std))
            z_scores.sort(key=lambda x: x[1], reverse=True)
            for col, z in z_scores[:3]:
                deviated[col] = {
                    "current":       round(float(X_eval[col].iloc[0]), 4),
                    "baseline_mean": round(float(baseline_X[col].mean()), 4),
                    "z_score":       round(float(z), 2),
                }

    return {
        "is_anomaly":          is_anomaly,
        "decision_score":      round(if_score, 4),
        "pca_reconstruct_err": round(pca_err, 4),
        "pca_threshold":       round(ensemble["pca_threshold"], 4),
        "detection_method": (
            f"Ceph-Semantic Majority ({', '.join(triggered)})"
            if is_anomaly else None
        ),
        "triggered_models":  triggered,
        "consecutive_votes": _consecutive_anomalies,
        "deviated_features": deviated,
        "timestamp":         eval_ts,
        "active_features":   len(feature_cols),
        "status":            "ready",
    }


def reset_model():
    """Remove the persisted model and clear in-memory cache."""
    global _model_cache, _consecutive_anomalies
    _model_cache = {"ensemble": None, "feature_cols": None, "baseline_X": None}
    _consecutive_anomalies = 0
    if os.path.exists(MODEL_PATH):
        os.remove(MODEL_PATH)
        print(f"[SEMANTIC] Model removed: {MODEL_PATH}", flush=True)


# -- CLI entry point -----------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ceph Semantic Baseline Trainer v8",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ceph_semantic_baseline.py              # train and save model
  python ceph_semantic_baseline.py --audit      # inspect available features only
  python ceph_semantic_baseline.py --detect     # run one detection pass
  python ceph_semantic_baseline.py --reset      # delete model, start fresh
        """,
    )
    parser.add_argument("--audit",  action="store_true", help="Audit available Ceph features (no training)")
    parser.add_argument("--detect", action="store_true", help="Run one detection pass against current DB")
    parser.add_argument("--reset",  action="store_true", help="Delete ceph_semantic_model.pkl")
    args = parser.parse_args()

    if args.reset:
        reset_model()
        sys.exit(0)

    if args.audit:
        print("=== CEPH SEMANTIC FEATURE AUDIT ===")
        cols = discover_ceph_features(DB_PATH, n_rows=300)
        print(f"\nAvailable Ceph-semantic features with variance ({len(cols)}):")
        for c in sorted(cols):
            print(f"  {c}")
        sys.exit(0)

    if args.detect:
        res = detect_anomalies()
        print(json.dumps(res, indent=2, default=str))
        sys.exit(0)

    # Default: full training run
    ensemble = train_semantic_model()
    if ensemble:
        print("\n=== VERIFICATION: Running one detection pass ===")
        res = detect_anomalies()
        print(json.dumps(res, indent=2, default=str))
