import os
import sys
import re
import threading
import json
import time
import sqlite3
from datetime import datetime
import paramiko
from dotenv import load_dotenv

# Load configuration from .env file
load_dotenv()

HOST = os.getenv("VM_SSH_HOST", "127.0.0.1")
PORT = int(os.getenv("VM_SSH_PORT", "2222"))
USER = os.getenv("VM_SSH_USER", "vboxuser")
PASSWORD = os.getenv("VM_SSH_PASSWORD", "admin")

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DB_PATH", os.path.join(ROOT_DIR, "ceph_monitor.db"))

# Print lock to prevent interleaved stdout writes from multiple threads
print_lock = threading.Lock()

# Regex patterns for log parsing
CEPH_EVENT_PATTERN = re.compile(
    r'^(\S+)\s+(\S+)\s+(?:\(\S+\)\s+)?\d+\s+:\s+\S+\s+\[([A-Z]+)\]\s+(.*)$'
)
JOURNAL_PATTERN = re.compile(
    r'^([A-Z][a-z]{2}\s+\d+\s+\d+:\d+:\d+)\s+(\S+)\s+([^:\[]+)(?:\[(\d+)\])?:\s+(.*)$'
)

def format_output(source, timestamp, severity, component, message, raw_line):
    """Prints a unified JSON line representation of the log and saves it to SQLite."""
    ts = timestamp or datetime.utcnow().isoformat() + "Z"
    sev = severity or "INFO"
    comp = component or "unknown"
    msg = message or raw_line.strip()
    
    log_data = {
        "timestamp": ts,
        "source": source,
        "severity": sev,
        "component": comp,
        "message": msg
    }
    with print_lock:
        print(json.dumps(log_data), flush=True)
        
    # Write to database
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO events_log (timestamp, source, severity, component, message, raw_line) VALUES (?, ?, ?, ?, ?, ?)",
            (ts, source, sev, comp, msg, raw_line.strip())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Database write error in streamer: {e}", file=sys.stderr)

def handle_ssh_sudo_command(client, command, password, line_handler):
    """Executes a command via SSH, feeds sudo password, and streams output lines."""
    try:
        # Use exec_command to run command
        stdin, stdout, stderr = client.exec_command(command)
        
        # Sudo -S reads password from stdin. Send it immediately.
        stdin.write(password + '\n')
        stdin.flush()
        
        # Read stdout line by line
        for line in stdout:
            # Skip the sudo password prompt if it shows up in stdout
            if "[sudo] password for" in line:
                continue
            line_handler(line)
            
        # If the command finishes, check for errors
        err_content = stderr.read().decode('utf-8', errors='ignore')
        if err_content and "[sudo] password for" not in err_content:
            with print_lock:
                print(f"Error in command '{command}': {err_content.strip()}", file=sys.stderr)
                
    except Exception as e:
        with print_lock:
            print(f"Exception running '{command}': {e}", file=sys.stderr)

def parse_ceph_event(line):
    """Parses lines from 'ceph -w'."""
    raw = line.strip()
    if not raw:
        return
        
    match = CEPH_EVENT_PATTERN.match(raw)
    if match:
        timestamp, component, severity, msg = match.groups()
        format_output(
            source="ceph-cluster",
            timestamp=timestamp,
            severity=severity,
            component=component,
            message=msg,
            raw_line=raw
        )
    else:
        # Fallback if log format is slightly different
        format_output(
            source="ceph-cluster",
            timestamp=None,
            severity="INFO",
            component="cluster-event",
            message=raw,
            raw_line=raw
        )

def parse_journal_event(line):
    """Parses lines from 'journalctl -f'."""
    raw = line.strip()
    if not raw:
        return
        
    match = JOURNAL_PATTERN.match(raw)
    if match:
        time_str, hostname, service, pid, msg = match.groups()
        
        # Convert syslog style date (e.g. 'Jul 19 12:00:00') to ISO format for AI parser consistency
        try:
            parsed_time = datetime.strptime(time_str, "%b %d %H:%M:%S")
            # Assume current year since journalctl logs don't output year by default
            current_year = datetime.now().year
            parsed_time = parsed_time.replace(year=current_year)
            iso_time = parsed_time.isoformat() + "Z"
        except ValueError:
            iso_time = None
            
        # Determine severity based on common daemon log keywords
        severity = "INFO"
        lower_msg = msg.lower()
        if "err" in lower_msg or "fail" in lower_msg or "crit" in lower_msg:
            severity = "ERROR"
        elif "warn" in lower_msg:
            severity = "WARNING"
            
        format_output(
            source="ceph-daemon",
            timestamp=iso_time,
            severity=severity,
            component=service,
            message=msg,
            raw_line=raw
        )
    else:
        # Fallback
        format_output(
            source="ceph-daemon",
            timestamp=None,
            severity="INFO",
            component="systemd-journal",
            message=raw,
            raw_line=raw
        )

def main():
    print(f"Connecting to Ceph VM at {HOST}:{PORT} as user '{USER}'...", file=sys.stderr)
    
    # Establish base SSH connection
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        client.connect(hostname=HOST, port=PORT, username=USER, password=PASSWORD, timeout=10)
        print("Connected successfully! Starting log streaming threads...", file=sys.stderr)
        
        # We need two separate clients or channels because SSH executes commands on separate channels/sessions
        client_event = paramiko.SSHClient()
        client_event.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client_event.connect(hostname=HOST, port=PORT, username=USER, password=PASSWORD, timeout=10)
        
        client_journal = paramiko.SSHClient()
        client_journal.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client_journal.connect(hostname=HOST, port=PORT, username=USER, password=PASSWORD, timeout=10)
        
        # Define commands (using sudo -S to feed password via stdin)
        cmd_ceph_event = "sudo -S ceph -w"
        cmd_journal = "sudo -S journalctl -f -u 'ceph-*'"
        
        # Spawn daemon threads for log streams
        t1 = threading.Thread(
            target=handle_ssh_sudo_command, 
            args=(client_event, cmd_ceph_event, PASSWORD, parse_ceph_event),
            daemon=True
        )
        t2 = threading.Thread(
            target=handle_ssh_sudo_command, 
            args=(client_journal, cmd_journal, PASSWORD, parse_journal_event),
            daemon=True
        )
        
        t1.start()
        t2.start()
        
        # Main thread loop
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping log streaming client...", file=sys.stderr)
    except Exception as e:
        print(f"SSH Connection failed: {e}", file=sys.stderr)
    finally:
        client.close()
        print("SSH Connection closed.", file=sys.stderr)

if __name__ == "__main__":
    main()
