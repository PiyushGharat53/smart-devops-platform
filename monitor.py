# monitor.py
"""
Self-Healing Monitor
---------------------
Continuously polls the health endpoint of the FastAPI app.
If the health check fails, it automatically restarts the
associated Docker container to recover the service.
"""

import subprocess
import time

import requests

# --- Configuration ---
HEALTH_CHECK_URL = "http://localhost:8000/health"
CONTAINER_NAME = "my-patient"
CHECK_INTERVAL_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 3


def check_health() -> bool:
    """Return True if the app responds healthy, False otherwise."""
    try:
        response = requests.get(HEALTH_CHECK_URL, timeout=REQUEST_TIMEOUT_SECONDS)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def restart_container() -> None:
    """Restart the target Docker container using subprocess."""
    try:
        subprocess.run(
            ["docker", "restart", CONTAINER_NAME],
            check=True,
            capture_output=True,
            text=True,
        )
        print("[🛠️] Container restarted successfully.")
    except subprocess.CalledProcessError as e:
        print(f"[❌] Failed to restart container: {e.stderr.strip()}")


def main() -> None:
    print(f"[🔎] Starting Self-Healing Monitor for '{CONTAINER_NAME}'...")
    print(f"[🔎] Polling {HEALTH_CHECK_URL} every {CHECK_INTERVAL_SECONDS}s\n")

    while True:
        if check_health():
            print("[✅] System Healthy")
        else:
            print("[🚨] CRASH DETECTED! Initiating Self-Healing...")
            restart_container()

        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[🛑] Monitor stopped by user.")