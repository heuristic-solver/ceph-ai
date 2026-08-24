import sys
from datetime import datetime

# Reconfigure stdout to use UTF-8 to prevent any UnicodeEncodeError in Windows terminals
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# ANSI escape codes for formatting
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
GRAY = "\033[90m"
CYAN = "\033[96m"
WHITE = "\033[97m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
RED = "\033[91m"
MAGENTA = "\033[95m"

SEVERITY_COLORS = {
    "INFO": GREEN,
    "WARNING": YELLOW,
    "HIGH": RED,
    "ERROR": RED,
    "CRITICAL": "\033[1;91m" # Bold Red
}


def print_alert(alert_type, title, explanation, recommended_action, severity="INFO", timestamp=None):
    """Prints a standard terminal alert box."""
    if not timestamp:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    color = SEVERITY_COLORS.get(severity.upper(), RESET)
    width = 76
    
    print(f"\n{GRAY}{'=' * width}{RESET}")
    print(f"{color}{BOLD}[!] [{severity.upper()}] {title}{RESET}")
    print(f"{GRAY}    Time:    {timestamp}{RESET}")
    print(f"{GRAY}    Source:  {alert_type}{RESET}")
    print(f"{GRAY}{'-' * width}{RESET}")
    
    words = explanation.split()
    lines = []
    current_line = []
    for word in words:
        if len(" ".join(current_line + [word])) <= width - 8:
            current_line.append(word)
        else:
            lines.append(" ".join(current_line))
            current_line = [word]
    if current_line:
        lines.append(" ".join(current_line))
        
    for line in lines:
        print(f"    {line}")
        
    print(f"{GRAY}{'-' * width}{RESET}")
    print(f"{CYAN}{BOLD}    >>> Recommended Action:{RESET}")
    print(f"    {recommended_action}")
    print(f"{GRAY}{'=' * width}{RESET}\n")


def print_rca_report(diagnosis):
    """
    Renders a comprehensive, beautifully styled Root Cause Analysis (RCA) Incident Card.
    Dynamically displays the incident ID, fault category, root cause, evidence chain,
    blast radius, and exact remediation steps.
    """
    if not isinstance(diagnosis, dict):
        return

    inc_id = diagnosis.get("incident_id", "INC-UNKNOWN")
    category = diagnosis.get("fault_category", "UNKNOWN_ANOMALY")
    summary = diagnosis.get("root_cause_summary", "Anomaly detected")
    detail = diagnosis.get("detailed_explanation", "")
    evidence = diagnosis.get("evidence_chain", [])
    radius = diagnosis.get("blast_radius", "Under investigation")
    severity = diagnosis.get("severity", "WARNING").upper()
    remedy = diagnosis.get("remediation_steps", [])
    verify = diagnosis.get("verification_command", "ceph status")
    source = diagnosis.get("source", "AI Diagnosis Engine")

    color = SEVERITY_COLORS.get(severity, YELLOW)
    width = 78

    print(f"\n{color}{BOLD}{'#' * width}{RESET}")
    print(f"{color}{BOLD}  [AI ROOT CAUSE INCIDENT REPORT] :: {inc_id}{RESET}")
    print(f"{GRAY}  Category: {category}  |  Severity: {color}{BOLD}{severity}{RESET}{GRAY}  |  Source: {source}{RESET}")
    print(f"{GRAY}{'=' * width}{RESET}")

    # Root Cause Summary
    print(f"{WHITE}{BOLD}  [ROOT CAUSE SUMMARY]{RESET}")
    print(f"  {YELLOW}{BOLD}>> {summary}{RESET}")
    if detail:
        print(f"  {DIM}{detail}{RESET}")

    # Blast Radius
    print(f"\n{WHITE}{BOLD}  [BLAST RADIUS & IMPACT]{RESET}")
    print(f"  {MAGENTA}{radius}{RESET}")

    # Evidence Chain
    if evidence:
        print(f"\n{WHITE}{BOLD}  [TELEMETRY EVIDENCE CHAIN]{RESET}")
        for ev in evidence:
            print(f"  {CYAN}* {ev}{RESET}")

    # Remediation Steps
    if remedy:
        print(f"\n{WHITE}{BOLD}  [STEP-BY-STEP REMEDIATION RUNBOOK]{RESET}")
        for idx, step in enumerate(remedy, 1):
            if step.startswith("sudo ") or step.startswith("ceph ") or step.startswith("ps ") or step.startswith("free "):
                print(f"  {GREEN}{BOLD}  {idx}. $ {step}{RESET}")
            else:
                print(f"  {GREEN}  {idx}. {step}{RESET}")

    # Verification
    if verify:
        print(f"\n{WHITE}{BOLD}  [VERIFICATION COMMAND]{RESET}")
        print(f"  {CYAN}{BOLD}  $ {verify}{RESET}")

    print(f"{color}{BOLD}{'#' * width}{RESET}\n")


if __name__ == "__main__":
    sample = {
        "incident_id": "INC-20260819-01",
        "fault_category": "OSD_PROCESS_CRASH",
        "root_cause_summary": "1 OSD daemon process(es) terminated or crashed unexpectedly",
        "detailed_explanation": "OSD daemon worker threads collapsed to 0.0. The container or binary has exited.",
        "evidence_chain": [
            "Sentinel alert: OSDs in DOWN state = 1.0 (expected 0)",
            "Metric 'host.osd_down_count' diverged to 1.00 vs baseline 0.00 (z-score: 99.9)"
        ],
        "blast_radius": "1 OSD offline. 97 Placement Groups degraded. Data redundancy compromised.",
        "severity": "HIGH",
        "remediation_steps": [
            "sudo systemctl reset-failed",
            "sudo systemctl restart ceph.target",
            "sudo ceph osd in osd.0"
        ],
        "verification_command": "ceph osd tree && ceph health",
        "source": "Local LLM (Gemma)"
    }
    print_rca_report(sample)
