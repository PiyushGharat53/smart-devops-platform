# predictor.py
"""
AIOps Anomaly Predictor
------------------------
Periodically queries Prometheus for the current HTTP request
rate, flags potential overload conditions before they cause a
crash, automatically files an incident report when an anomaly
is first detected, and toggles the app's Active Defense mode.
"""

import os
import time
from datetime import datetime

import requests

# --- Configuration ---
PROMETHEUS_URL = "http://localhost:9090/api/v1/query"
PROMQL_QUERY = "rate(http_requests_total[1m])"
REQUEST_RATE_THRESHOLD = 5.0  # requests per second
CHECK_INTERVAL_SECONDS = 10
REQUEST_TIMEOUT_SECONDS = 5
INCIDENTS_DIR = "incidents"

APP_BASE_URL = "http://localhost:8000"
DEFENSE_ENABLE_URL = f"{APP_BASE_URL}/defense/enable"
DEFENSE_DISABLE_URL = f"{APP_BASE_URL}/defense/disable"

# Cooldown state: tracks whether we're already "inside" an active incident
incident_active = False


def query_prometheus(promql: str):
    """Query Prometheus and return the list of result vectors."""
    try:
        response = requests.get(
            PROMETHEUS_URL,
            params={"query": promql},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "success":
            return data["data"]["result"]
        print(f"[❌] Prometheus query failed: {data.get('error', 'unknown error')}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"[❌] Could not reach Prometheus: {e}")
        return []


def get_max_rate(results) -> float:
    """Extract the highest current rate value across all series."""
    max_rate = 0.0
    for series in results:
        try:
            value = float(series["value"][1])
            max_rate = max(max_rate, value)
        except (KeyError, IndexError, ValueError):
            continue
    return max_rate


def file_incident_report(rate: float) -> None:
    """Create a timestamped incident report in the incidents/ directory."""
    os.makedirs(INCIDENTS_DIR, exist_ok=True)

    now = datetime.now()
    timestamp_for_filename = now.strftime("%Y%m%d_%H%M%S")
    filename = f"incident_report_{timestamp_for_filename}.log"
    filepath = os.path.join(INCIDENTS_DIR, filename)

    report = f"""INCIDENT REPORT
================
Timestamp:          {now.isoformat()}
Detected Rate:       {rate:.2f} requests/sec
Threshold:           {REQUEST_RATE_THRESHOLD:.2f} requests/sec
Severity:            HIGH

Description:
An anomalous spike in HTTP traffic was detected, exceeding the
configured safe threshold. This may indicate a viral traffic
event, a misbehaving client, or a potential DDoS attack.

Recommended Action:
Investigate potential DDoS attack or viral traffic spike.
Review upstream load balancer logs and source IP distribution.
Consider enabling rate limiting or auto-scaling if not already active.
"""

    with open(filepath, "w") as f:
        f.write(report)

    print(f"[📄] Incident report filed: {filepath}")


def set_defense_mode(enable: bool) -> None:
    """Call the app's defense toggle endpoint."""
    url = DEFENSE_ENABLE_URL if enable else DEFENSE_DISABLE_URL
    try:
        requests.post(url, timeout=REQUEST_TIMEOUT_SECONDS)
        if enable:
            print("[🛡️ DEFENSE] AI Activated App Shields!")
        else:
            print("[🛡️ DEFENSE] AI Deactivated App Shields.")
    except requests.exceptions.RequestException as e:
        print(f"[❌] Failed to {'enable' if enable else 'disable'} defense mode: {e}")


def evaluate_rate(rate: float) -> None:
    """Print a status message and manage incident lifecycle based on rate."""
    global incident_active

    if rate > REQUEST_RATE_THRESHOLD:
        print(
            f"[⚠️ AI ALERT] Anomalous traffic spike detected! "
            f"Potential crash imminent. (rate={rate:.2f} req/s)"
        )
        if not incident_active:
            # New incident -> file exactly one report, then enter cooldown
            file_incident_report(rate)
            set_defense_mode(enable=True)
            incident_active = True
        else:
            print("[⏳] Incident already active — skipping duplicate ticket.")
    else:
        if incident_active:
            print("[✅] Traffic has returned to normal. Incident resolved.")
            set_defense_mode(enable=False)
        else:
            print(f"[🧠 AI] Traffic patterns normal. (rate={rate:.2f} req/s)")
        incident_active = False


def main() -> None:
    print("[🔎] Starting AIOps Anomaly Predictor...")
    print(f"[🔎] Querying {PROMETHEUS_URL} every {CHECK_INTERVAL_SECONDS}s\n")

    while True:
        results = query_prometheus(PROMQL_QUERY)
        rate = get_max_rate(results)
        evaluate_rate(rate)
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[🛑] Predictor stopped by user.")