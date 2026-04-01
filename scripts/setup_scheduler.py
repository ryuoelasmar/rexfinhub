"""
Set up all Windows Task Scheduler jobs for automated pipeline.

Creates 5 scheduled tasks:
  1. ETP_Watcher         -- every 30 min, polls EDGAR for new filings/trusts
  2. ETP_RapidSync       -- every 2 hours (8am-10pm), scrapes + uploads to Render
  3. ETP_TrustSync       -- 3:45 PM weekdays, trust universe sync from SEC
  4. ETP_DailySync       -- 5:00 PM weekdays, full pipeline (finishes ~5:30, emails at 5:30)
  5. ETP_BloombergWatch  -- 4:00-5:00 PM weekdays, detect fresh Bloomberg + trigger sync

Run as Administrator:
    python scripts/setup_scheduler.py

To remove all tasks:
    python scripts/setup_scheduler.py --remove
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable

TASKS = [
    {
        "name": "ETP_Watcher",
        "desc": "Poll EDGAR for new filings and trusts every 30 minutes",
        "command": f'"{PYTHON}" "{PROJECT_ROOT / "scripts" / "run_watcher.py"}"',
        "schedule": "/sc minute /mo 30",
        "start_time": None,
    },
    {
        "name": "ETP_RapidSync",
        "desc": "Rapid filing detection + Render upload every 2 hours",
        "command": f'"{PYTHON}" "{PROJECT_ROOT / "scripts" / "run_rapid_sync.py"}"',
        "schedule": "/sc daily /mo 1 /ri 120 /du 16:00",  # repeat every 120 min for 16 hours
        "start_time": "08:00",
    },
    {
        "name": "ETP_TrustSync",
        "desc": "Trust universe sync from SEC submissions.zip (3:45 PM daily)",
        "command": f'"{PYTHON}" "{PROJECT_ROOT / "scripts" / "sync_trust_universe.py"}"',
        "schedule": "/sc weekly /d MON,TUE,WED,THU,FRI",
        "start_time": "15:45",
    },
    {
        "name": "ETP_DailySync",
        "desc": "Full pipeline at 5:00 PM -- finishes by ~5:25, sends emails at 5:30",
        "command": f'"{PYTHON}" "{PROJECT_ROOT / "scripts" / "run_daily.py"}"',
        "schedule": "/sc weekly /d MON,TUE,WED,THU,FRI",
        "start_time": "17:00",
    },
    {
        "name": "ETP_BloombergWatch",
        "desc": "Watch for fresh Bloomberg file and trigger market sync (4:00-5:00 PM)",
        "command": f'"{PYTHON}" "{PROJECT_ROOT / "scripts" / "watch_bloomberg.py"}"',
        "schedule": "/sc daily /mo 1 /ri 5 /du 01:00",  # repeat every 5 min for 1 hour
        "start_time": "16:00",
    },
]


def create_tasks():
    print("=== Setting up ETP scheduled tasks ===\n")
    for task in TASKS:
        cmd = [
            "schtasks", "/create",
            "/tn", task["name"],
            "/tr", task["command"],
            *task["schedule"].split(),
            "/f",  # force overwrite
        ]
        if task["start_time"]:
            cmd.extend(["/st", task["start_time"]])

        print(f"Creating: {task['name']}")
        print(f"  {task['desc']}")
        print(f"  Command: {task['command']}")
        print(f"  Schedule: {task['schedule']}", end="")
        if task["start_time"]:
            print(f" at {task['start_time']}", end="")
        print()

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  OK\n")
        else:
            print(f"  FAILED: {result.stderr.strip()}")
            print(f"  (Run as Administrator if access denied)\n")


def remove_tasks():
    print("=== Removing ETP scheduled tasks ===\n")
    for task in TASKS:
        cmd = ["schtasks", "/delete", "/tn", task["name"], "/f"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  Removed: {task['name']}")
        else:
            print(f"  Not found: {task['name']}")


def show_tasks():
    print("=== Current ETP scheduled tasks ===\n")
    for task in TASKS:
        cmd = ["schtasks", "/query", "/tn", task["name"], "/fo", "LIST"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            # Extract key fields
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if any(line.startswith(k) for k in ("TaskName:", "Status:", "Next Run Time:", "Last Run Time:")):
                    print(f"  {line}")
            print()
        else:
            print(f"  {task['name']}: NOT SCHEDULED\n")


if __name__ == "__main__":
    if "--remove" in sys.argv:
        remove_tasks()
    elif "--status" in sys.argv:
        show_tasks()
    else:
        create_tasks()
        print("=" * 50)
        show_tasks()
