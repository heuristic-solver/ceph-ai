import paramiko, os, time
from dotenv import load_dotenv
from ceph_cluster_info import get_cluster_info

ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(ROOT, '.env'))

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(
    os.getenv('VM_SSH_HOST', '127.0.0.1'),
    int(os.getenv('VM_SSH_PORT', '2222')),
    os.getenv('VM_SSH_USER', 'vboxuser'),
    os.getenv('VM_SSH_PASSWORD', 'admin'),
    timeout=5
)

def exec_cmd(cmd):
    stdin, stdout, stderr = ssh.exec_command(f'sudo -S bash -c "{cmd}"')
    stdin.write(os.getenv('VM_SSH_PASSWORD', 'admin') + '\n')
    stdin.flush()
    return stdout.read().decode() + stderr.read().decode()

# Dynamically discover service names
info = get_cluster_info(ssh)
OSD_SVC = info["osd_service"]
MON_SVC = info["mon_service"]

print(f"[INFO] OSD service: {OSD_SVC}")
print(f"[INFO] MON service: {MON_SVC}")

print("[1] Reset-failed and start services...")
exec_cmd(f"systemctl reset-failed; systemctl start '{OSD_SVC}'; systemctl start '{MON_SVC}'; systemctl start ceph.target")
time.sleep(6)

print("[2] Marking OSD in and archiving crashes...")
exec_cmd("ceph osd in osd.0 2>/dev/null; ceph crash archive-all 2>/dev/null; true")
time.sleep(3)

print("[3] Checking ceph status...")
status = exec_cmd("ceph status; echo '---'; ceph health detail")
print(status)

ssh.close()
