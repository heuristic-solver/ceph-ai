"""
ceph_cluster_info.py  --  Dynamic Ceph Cluster Discovery Helper
================================================================
Discovers the Ceph cluster FSID and daemon identifiers dynamically
from the live VM at runtime. This makes every script portable across
any Ceph VM regardless of the cluster FSID or hostname.

Usage (called at startup in any script needing systemctl service names):
    from ceph_cluster_info import get_cluster_info
    info = get_cluster_info(ssh)
    osd_svc  = info["osd_service"]    # e.g. ceph-<fsid>@osd.0.service
    mon_svc  = info["mon_service"]    # e.g. ceph-<fsid>@mon.<hostname>.service
"""

import re


_cache = {}  # Cache per SSH session to avoid repeated queries


def get_cluster_info(ssh):
    """
    Dynamically discovers the Ceph cluster FSID and primary service names
    by querying the live VM via SSH. Results are cached for the lifetime
    of the process.

    Returns a dict:
        {
            "fsid":        "<cluster-fsid>",
            "hostname":    "<mon-hostname>",
            "osd_service": "ceph-<fsid>@osd.0.service",
            "mon_service": "ceph-<fsid>@mon.<hostname>.service",
        }
    """
    global _cache
    if _cache:
        return _cache

    fsid = ""
    hostname = ""

    # Method 1: ceph fsid (most reliable)
    try:
        stdin, stdout, stderr = ssh.exec_command("sudo -S ceph fsid 2>/dev/null", timeout=8)
        stdin.write("admin\n"); stdin.flush()
        fsid = stdout.read().decode("utf-8", errors="ignore").strip()
    except Exception:
        pass

    # Method 2: Parse from systemctl unit names if ceph fsid fails (e.g. mon is down)
    if not fsid or len(fsid) < 30:
        try:
            stdin, stdout, stderr = ssh.exec_command(
                "systemctl list-units 'ceph-*@osd*' --no-legend --no-pager 2>/dev/null | head -1",
                timeout=8
            )
            line = stdout.read().decode("utf-8", errors="ignore").strip()
            # e.g. "ceph-b4651b96-6d4c-11f1-acdf-9704d31d1067@osd.0.service  loaded active running ..."
            m = re.search(r"ceph-([0-9a-f-]{36})@osd", line)
            if m:
                fsid = m.group(1)
        except Exception:
            pass

    # Method 3: Last fallback - scan /etc/ceph/ceph.conf
    if not fsid or len(fsid) < 30:
        try:
            stdin, stdout, stderr = ssh.exec_command(
                "grep -i fsid /etc/ceph/ceph.conf 2>/dev/null | head -1",
                timeout=8
            )
            line = stdout.read().decode("utf-8", errors="ignore").strip()
            m = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", line)
            if m:
                fsid = m.group(1)
        except Exception:
            pass

    # Discover MON hostname dynamically
    try:
        stdin, stdout, stderr = ssh.exec_command(
            "systemctl list-units 'ceph-*@mon*' --no-legend --no-pager 2>/dev/null | head -1",
            timeout=8
        )
        line = stdout.read().decode("utf-8", errors="ignore").strip()
        m = re.search(r"@mon\.(\S+?)\.service", line)
        if m:
            hostname = m.group(1)
    except Exception:
        pass

    # Final fallback for hostname
    if not hostname:
        try:
            stdin, stdout, stderr = ssh.exec_command("hostname", timeout=5)
            hostname = stdout.read().decode("utf-8", errors="ignore").strip()
        except Exception:
            hostname = "unknown"

    osd_service = f"ceph-{fsid}@osd.0.service" if fsid else "ceph.target"
    mon_service = f"ceph-{fsid}@mon.{hostname}.service" if fsid and hostname else "ceph.target"

    _cache = {
        "fsid":        fsid,
        "hostname":    hostname,
        "osd_service": osd_service,
        "mon_service": mon_service,
    }
    print(f"[CLUSTER INFO] FSID={fsid} | MON hostname={hostname}")
    print(f"[CLUSTER INFO] OSD service: {osd_service}")
    print(f"[CLUSTER INFO] MON service: {mon_service}")
    return _cache


def clear_cache():
    """Call this to force re-discovery (e.g. after a MON restart)."""
    global _cache
    _cache = {}
