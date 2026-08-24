"""
demo_inject.py  --  Interactive Fault Controller for Ceph AI Demo
==================================================================
Run this in a second terminal while demo_live.py is running in the first.

You pick which fault to inject from a menu.
The fault stays ACTIVE until YOU press Enter to stop it.
You have full control over timing.

Usage:
    python demo_inject.py
"""

import os, sys, time, json
import paramiko
from datetime import datetime
from dotenv import load_dotenv
from ceph_cluster_info import get_cluster_info

ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(ROOT, ".env"))

HOST     = os.getenv("VM_SSH_HOST", "127.0.0.1")
PORT     = int(os.getenv("VM_SSH_PORT", "2222"))
USER     = os.getenv("VM_SSH_USER", "vboxuser")
PASSWORD = os.getenv("VM_SSH_PASSWORD", "admin")

# ANSI
R="\033[0m"; B="\033[1m"; D="\033[2m"
RED="\033[91m"; GRN="\033[92m"; YLW="\033[93m"
CYN="\033[96m"; WHT="\033[97m"; MGT="\033[95m"
BG_RED="\033[41m"; BG_GRN="\033[42m"; BG_YLW="\033[43m"

def c(text, *codes): return "".join(codes)+str(text)+R
def ts(): return datetime.now().strftime("%H:%M:%S")

def hdr(title):
    print()
    print(c("  +"+("="*66)+"+", CYN, B))
    print(c("  | "+title.ljust(65)+"|", CYN, B))
    print(c("  +"+("="*66)+"+", CYN, B))

def step(msg, col=WHT):
    print(f"  {c('['+ts()+']', D)}  {c(msg, col)}")

def active_banner(fault_name):
    print()
    print(c("  "+("!"*68), BG_RED, WHT, B))
    print(c("  !! FAULT ACTIVE: "+fault_name.ljust(50)+"!!", BG_RED, WHT, B))
    print(c("  "+("!"*68), BG_RED, WHT, B))
    print()
    print(c("  >>> Switch to demo_live.py window to see the AI detect it <<<", YLW, B))
    print()
    print(c("  Press ENTER when you are ready to STOP the fault and clean up...", WHT, B))

def clean_banner(fault_name):
    print()
    print(c("  "+("="*68), BG_GRN, B))
    print(c("  [CLEAN] "+fault_name+" fault stopped and cleaned up.".ljust(57), BG_GRN, B))
    print(c("  "+("="*68), BG_GRN, B))
    print()

# SSH helpers
def connect():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=HOST, port=PORT, username=USER, password=PASSWORD, timeout=8)
    # Discover cluster info immediately so all scripts can use dynamic service names
    try:
        get_cluster_info(ssh)
    except Exception:
        pass
    return ssh

def sudo(ssh, cmd, timeout=30):
    stdin, stdout, stderr = ssh.exec_command("sudo -S bash -c \""+cmd+"\"", timeout=timeout)
    stdin.write(PASSWORD + "\n"); stdin.flush()
    out = stdout.read().decode("utf-8", errors="ignore").strip()
    err = stderr.read().decode("utf-8", errors="ignore").strip()
    return out, err

def run_bg(ssh, cmd):
    ssh.exec_command("nohup bash -c '"+cmd+"' > /dev/null 2>&1 &")

def wait_for_clean(ssh, timeout=60):
    step("Waiting for cluster to recover...", YLW)
    deadline = time.time() + timeout
    while time.time() < deadline:
        out, _ = sudo(ssh, "ceph status --format json 2>/dev/null || echo ''", timeout=10)
        try:
            j = json.loads(out)
            h = j.get("health", {}).get("status", "")
            checks = j.get("health", {}).get("checks", {})
            crit = [k for k in checks if k not in ("POOL_NO_REDUNDANCY", "MON_DISK_LOW", "MON_DISK_CRIT", "OSDMAP_FLAGS", "DEVICE_HEALTH_TOOMANY", "CEPHADM_FAILED_DAEMON")]
            up = j.get("osdmap", {}).get("num_up_osds", 0)
            tot = j.get("osdmap", {}).get("num_osds", 1)
            if (h == "HEALTH_OK" or len(crit) == 0) and up == tot and up > 0:
                step("Cluster clean and healthy: " + h, GRN)
                return True
        except Exception:
            pass
        time.sleep(2)
    step("Warning: cluster still not clean after timeout.", YLW)
    return False

def emergency_cleanup(ssh):
    step("Running emergency cleanup sweep...", YLW)
    sudo(ssh, "killall -9 sha256sum dd 2>/dev/null; pkill -9 -f io_demo_test 2>/dev/null; pkill -9 -f ram_drain_demo 2>/dev/null; rm -f /tmp/io_demo_active /tmp/io_demo_test /tmp/ram_drain_demo.py 2>/dev/null; systemctl restart ceph.target 2>/dev/null; ceph osd in osd.0 2>/dev/null; true", timeout=30)

# ============================================================
# FAULT INJECTORS
# Each starts a fault, returns a cleanup function.
# The caller blocks on input(), then calls cleanup.
# ============================================================

def inject_cpu(ssh):
    hdr("FAULT 1: CPU THRASHING")
    step("Spawning 8 sha256sum processes to saturate all CPU cores...")
    run_bg(ssh, "for i in $(seq 1 8); do sha256sum /dev/zero > /dev/null 2>&1 & done")
    time.sleep(1)
    out, _ = sudo(ssh, "pgrep -c sha256sum 2>/dev/null || echo 0")
    step("sha256sum processes on VM: "+out.strip(), YLW)
    active_banner("CPU THRASHING")
    input()

    def cleanup():
        step("Killing sha256sum processes...")
        sudo(ssh, "killall -9 sha256sum 2>/dev/null; true")
        step("CPU fault cleaned.", GRN)
    return cleanup

def inject_ram(ssh):
    hdr("FAULT 2: RAM STARVATION")
    free_out, _ = sudo(ssh, "free -m | awk 'NR==2{print $7}'")
    try:
        free_mib = int(free_out.strip())
    except Exception:
        free_mib = 1500
    target = max(600, free_mib - 250)
    step(f"Free RAM on VM: {free_mib} MiB. Will allocate {target} MiB...")
    ram_py = "import time; chunks = [bytearray(1024*1024) for _ in range("+str(target)+")]; time.sleep(3600)"
    sudo(ssh, "echo '"+ram_py+"' > /tmp/ram_drain_demo.py")
    run_bg(ssh, "python3 /tmp/ram_drain_demo.py")
    time.sleep(1)
    out, _ = sudo(ssh, "pgrep -c -f ram_drain_demo 2>/dev/null || echo 0")
    step("RAM drain process count: "+out.strip(), YLW)
    active_banner("RAM STARVATION")
    input()

    def cleanup():
        step("Killing RAM drain process...")
        sudo(ssh, "pkill -9 -f ram_drain_demo 2>/dev/null; rm -f /tmp/ram_drain_demo.py; true")
        step("RAM fault cleaned.", GRN)
    return cleanup

def inject_io(ssh):
    hdr("FAULT 3: STORAGE I/O SATURATION")
    step("Starting dd write flood (10MB blocks, sync mode) on VM...")
    sudo(ssh, "touch /tmp/io_demo_active")
    run_bg(ssh, "while [ -f /tmp/io_demo_active ]; do dd if=/dev/zero of=/tmp/io_demo_test bs=10M count=50 oflag=dsync conv=notrunc 2>/dev/null; done")
    time.sleep(1)
    out, _ = sudo(ssh, "pgrep -x -c dd 2>/dev/null || echo 0")
    step("dd processes running: "+out.strip(), YLW)
    active_banner("STORAGE I/O SATURATION")
    input()

    def cleanup():
        step("Killing dd write processes...")
        sudo(ssh, "rm -f /tmp/io_demo_active; killall -9 dd 2>/dev/null; rm -f /tmp/io_demo_test; true")
        step("I/O fault cleaned.", GRN)
    return cleanup

def inject_osd_down(ssh):
    hdr("FAULT 4: OSD ADMINISTRATIVE DOWN")
    step("Issuing: ceph osd down osd.0 + ceph osd out osd.0")
    out, _ = sudo(ssh, "ceph osd down osd.0 && ceph osd out osd.0 && systemctl stop 'ceph*@osd.0*'")
    step(out[:100], RED)
    out2, _ = sudo(ssh, "ceph osd stat 2>/dev/null || echo ''")
    step("OSD stat:  "+out2, RED)
    out3, _ = sudo(ssh, "ceph pg stat 2>/dev/null || echo ''")
    step("PG  stat:  "+out3, RED)
    active_banner("OSD ADMINISTRATIVE DOWN")
    input()

    def cleanup():
        step("Restoring osd.0...")
        sudo(ssh, "systemctl restart ceph.target; ceph osd in osd.0")
        wait_for_clean(ssh)
        step("OSD fault cleaned.", GRN)
    return cleanup

def inject_osd_kill(ssh):
    hdr("FAULT 5: OSD PROCESS KILL (Simulated Crash)")
    step("Stopping ceph-osd daemon via systemctl...")
    sudo(ssh, "systemctl stop 'ceph*@osd.0*'", timeout=20)
    time.sleep(3)
    out, _ = sudo(ssh, "ceph osd stat 2>/dev/null || echo ''")
    step("OSD stat: "+out, RED)
    active_banner("OSD PROCESS KILL")
    input()

    def cleanup():
        step("Restarting ceph-osd...")
        sudo(ssh, "systemctl restart ceph.target; ceph osd in osd.0", timeout=30)
        wait_for_clean(ssh)
        step("OSD kill fault cleaned.", GRN)
    return cleanup

def inject_pg_target(ssh):
    hdr("FAULT 6: PG-LEVEL TARGETED DEGRADATION")
    step("Querying ceph pg dump to identify a specific PG and its acting OSD...")
    out, _ = sudo(ssh, "ceph pg dump --format json", timeout=20)
    target_osd = 0
    pgid = "?"
    try:
        pg_dump = json.loads(out)
        pg_stats = pg_dump.get("pg_map", {}).get("pg_stats", pg_dump.get("pg_stats", []))
        if pg_stats:
            first_pg = pg_stats[0]
            acting = first_pg.get("acting", [0])
            pgid = first_pg.get("pgid", "?")
            target_osd = acting[0] if acting else 0
    except Exception as e:
        step("PG dump parse error: "+str(e)+". Defaulting to osd.0", YLW)
    step(f"Targeting PG {pgid} -> faulting osd.{target_osd}", RED)
    sudo(ssh, f"ceph osd down osd.{target_osd} && ceph osd out osd.{target_osd} && systemctl stop 'ceph*@osd.{target_osd}*'")
    out2, _ = sudo(ssh, "ceph pg stat 2>/dev/null || echo ''")
    step("PG stat: "+out2, RED)
    active_banner(f"PG-TARGETED (osd.{target_osd} down)")
    input()

    def cleanup():
        step(f"Restoring osd.{target_osd}...")
        sudo(ssh, "systemctl restart ceph.target; ceph osd in osd.0")
        wait_for_clean(ssh)
        step("PG targeted fault cleaned.", GRN)
    return cleanup

def inject_mon_loss(ssh):
    hdr("FAULT 7: MONITOR QUORUM LOSS")
    step("Stopping ceph-mon daemon via systemctl...")
    info = get_cluster_info(ssh)
    mon_svc = info["mon_service"]
    step(f"Stopping: {mon_svc}", YLW)
    sudo(ssh, f"systemctl stop '{mon_svc}'", timeout=15)
    active_banner("MONITOR QUORUM LOSS")
    input()

    def cleanup():
        step("Restarting ceph-mon daemon...")
        sudo(ssh, f"systemctl reset-failed; systemctl start '{mon_svc}'; ceph osd in osd.0", timeout=20)
        wait_for_clean(ssh)
        step("Monitor restored.", GRN)
    return cleanup

# ============================================================
# MENU
# ============================================================
FAULTS = [
    ("CPU Thrashing            (8x sha256sum loops saturate CPU cores)",          inject_cpu),
    ("RAM Starvation           (dynamic allocation, triggers memory pressure)",   inject_ram),
    ("Storage I/O Saturation   (dd write flood, destroys disk queue depth)",      inject_io),
    ("OSD Administrative Down  (ceph osd down/out -- instant PG degradation)",    inject_osd_down),
    ("OSD Process Kill         (systemctl stop -- simulates daemon crash)",        inject_osd_kill),
    ("PG-Level Targeted Fault  (faults the acting OSD of a specific PG)",         inject_pg_target),
    ("Monitor Quorum Loss      (ceph-mon stopped -- feature dropout detection)",   inject_mon_loss),
]

def print_menu(ssh_ok):
    os.system("cls" if os.name == "nt" else "clear")
    print()
    print(c("  +================================================================+", CYN, B))
    print(c("  |      CEPH AI DEMO  --  FAULT INJECTION CONTROLLER              |", CYN, B))
    print(c("  |      Keep demo_live.py running in the other terminal           |", CYN, B))
    print(c("  +================================================================+", CYN, B))
    print()
    vm_str = c("CONNECTED", GRN, B) if ssh_ok else c("OFFLINE", RED, B)
    print(f"  VM SSH: {vm_str}")
    print()
    print(c("  SELECT A FAULT TO INJECT:", WHT, B))
    print()
    for i, (label, _) in enumerate(FAULTS, 1):
        print(f"   {c('['+str(i)+']', YLW, B)}  {label}")
    print()
    print(f"   {c('[E]', RED, B)}  Emergency cleanup all (kills all stress, restores OSDs)")
    print(f"   {c('[Q]', D)}  Quit")
    print()
    print(c("  ---------------------------------------------------------------", D))
    print(c("  NOTE: Fault stays ACTIVE until you press ENTER to stop it.", YLW, B))
    print(c("  ---------------------------------------------------------------", D))
    print()

def main():
    print(c("\n  Connecting to VM...", D))
    try:
        ssh = connect()
        ssh_ok = True
        print(c("  Connected.", GRN))
    except Exception as e:
        print(c("  Could not connect to VM: "+str(e), RED))
        print(c("  Some faults may not work without SSH.", YLW))
        ssh_ok = False
        ssh = None

    while True:
        print_menu(ssh_ok)
        choice = input("  Your choice: ").strip().upper()

        if choice == "Q":
            print(c("\n  Goodbye.\n", D))
            break

        elif choice == "E":
            hdr("EMERGENCY CLEANUP")
            if ssh:
                emergency_cleanup(ssh)
            else:
                step("No SSH -- nothing to clean.", YLW)
            input("\n  Press ENTER to return to menu...")

        elif choice.isdigit() and 1 <= int(choice) <= len(FAULTS):
            idx = int(choice) - 1
            _, inject_fn = FAULTS[idx]
            cleanup_fn = None
            try:
                cleanup_fn = inject_fn(ssh)
            except KeyboardInterrupt:
                print(c("\n  Ctrl+C received during fault injection.", YLW))
            except Exception as e:
                print(c("\n  Error during fault injection: "+str(e), RED))

            # Always run cleanup
            if cleanup_fn:
                try:
                    cleanup_fn()
                except Exception as e:
                    print(c("  Cleanup error: "+str(e), YLW))
                    emergency_cleanup(ssh)

            clean_banner(FAULTS[idx][0].split("(")[0].strip())
            input("  Press ENTER to return to menu...")

        else:
            print(c("  Invalid choice. Press ENTER to try again.", YLW))
            input()

    if ssh:
        ssh.close()

if __name__ == "__main__":
    main()
