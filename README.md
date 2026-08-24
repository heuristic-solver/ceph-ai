# Ceph AI Autonomous Monitoring & Root Cause Analysis (RCA) Engine

An AI-driven telemetry monitoring, anomaly detection, and automated root cause analysis system for Ceph distributed storage clusters.

---

## Key Features

1. **Dual-Layer Anomaly Detection**:
   * **Host Layer (v7)**: Monitors OS-level CPU, memory pressure (PSI), and storage I/O using an unsupervised 3-model ensemble (Isolation Forest + One-Class SVM + PCA Reconstruction) with temporal persistence filtering.
   * **Ceph Semantic Layer (v8)**: Deep storage daemon telemetry (OSD commit/apply latencies, PG peering states, daemon memory/threads).
2. **Deterministic Structural Sentinels**:
   * Instant alerts for discrete cluster events (OSD down, PG degraded, Monitor quorum loss, RAM starvation) with zero false alarms.
3. **Dynamic AI Root Cause Analysis (RCA)**:
   * 100% data-driven incident diagnosis engine. Identifies *what* failed, *why* it occurred (evidence chain), *operational blast radius*, and provides exact *CLI remediation runbooks*.
   * Uses local LLM (Ollama / Gemma / Qwen) with dynamic mathematical fallback.

---

## Prerequisites

### 1. Host Machine (Laptop / PC)
* **Python 3.10+**
* (Optional) **Ollama** running locally on port `11434` with model `qwen2.5:3b` or `gemma` (if Ollama is offline, the built-in dynamic heuristic engine takes over automatically).

### 2. Ceph Storage VM (VirtualBox / KVM / Cloud)
* Any Linux VM with Ceph running.
* **VirtualBox NAT Port Forwarding Rules**:
  * Host Port `2222` -> Guest Port `22` (SSH)
  * Host Port `19999` -> Guest Port `19999` (Netdata Telemetry)
* **Netdata on the VM**:
  If Netdata is not already installed on the VM, install it by running either command inside the VM:
  ```bash
  # Option A (Ubuntu/Debian):
  sudo apt-get update && sudo apt-get install -y netdata

  # Option B (Universal Kickstart Script):
  wget -O /tmp/netdata-kickstart.sh https://get.netdata.cloud/kickstart.sh && sudo sh /tmp/netdata-kickstart.sh --non-interactive
  ```

---

## Setup & Installation

### 1. Install Python Dependencies
```bash
# Optional: Create and activate virtual environment
python -m venv venv
# On Windows:
venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and set your VM SSH username and password:
```bash
cp .env.example .env
```
Default `.env` settings:
```ini
VM_SSH_HOST=127.0.0.1
VM_SSH_PORT=2222
VM_SSH_USER=vboxuser
VM_SSH_PASSWORD=admin

OLLAMA_URL=http://localhost:11434
OLLAMA_MODEL=qwen2.5:3b
OLLAMA_TIMEOUT=2
```

---

## How to Run the Live Interactive Demo

Open **two terminal windows** side-by-side:

### Terminal 1: Live Monitoring & Root Cause AI Dashboard
```bash
python demo_live.py
```
* **Normal State**: Displays `[OK] SYSTEM NORMAL` with live cluster metrics.
* **Fault Injected**: Instantly displays `[!] ANOMALY DETECTED` followed by the **AI ROOT CAUSE INCIDENT REPORT** card with the exact cause, evidence, and remediation steps.
* **Fault Stopped**: Automatically returns to `[OK] SYSTEM NORMAL`.

### Terminal 2: Interactive Fault Injector
```bash
python demo_inject.py
```
Presents a menu of all 7 supported failure scenarios:
1. `CPU Thrashing` (8x sha256sum runaway compute)
2. `RAM Starvation` (Host memory capacity drain)
3. `Storage I/O Saturation` (Synchronous direct I/O write flood)
4. `OSD Administrative Down` (ceph osd down/out)
5. `OSD Process Kill` (OSD daemon container termination)
6. `PG-Level Targeted Degradation` (Placement group redundancy loss)
7. `Monitor Quorum Loss` (Monitor container stopped / control plane offline)

*Select any scenario (1-7), observe Terminal 1 diagnose it, then press **ENTER** in Terminal 2 to stop the fault and restore the cluster.*

---

## Running Automated Live Validation Suites

### 1. Live End-to-End Cluster RCA Validation (7/7 Live Scenarios)
Runs all 7 faults consecutively on the live VM, validates real-time anomaly detection, verifies RCA diagnostic classification, and confirms clean cluster recovery:
```bash
python run_live_rca_validation.py
```

### 2. Live Anomaly Detection Regression Suite
Executes the full suite of live fault injections against the VM to validate host-layer and Ceph-layer model responses:
```bash
python run_scenario_tests.py
```

---

## Handy Utility Scripts

* **Cluster Recovery Helper**:
  If the Ceph VM ever gets stuck in a degraded state after manual testing, run:
  ```bash
  python restore_cluster.py
  ```
