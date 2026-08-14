# predictor.py
"""
AIOps Anomaly Predictor
------------------------
Periodically queries Prometheus for the current HTTP request
rate and flags potential overload conditions before they cause
a crash.
"""

import time

import requests

# --- Configuration ---
PROMETHEUS_URL = "http://localhost:9090/api/v1/query"
PROMQL_QUERY = "rate(http_requests_total[1m])"
REQUEST_RATE_THRESHOLD = 5.0  # requests per second
CHECK_INTERVAL_SECONDS = 10
REQUEST_TIMEOUT_SECONDS = 5


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


def evaluate_rate(rate: float) -> None:
    """Print a status message based on the observed rate."""
    if rate > REQUEST_RATE_THRESHOLD:
        print(
            f"[⚠️ AI ALERT] Anomalous traffic spike detected! "
            f"Potential crash imminent. (rate={rate:.2f} req/s)"
        )
    else:
        print(f"[🧠 AI] Traffic patterns normal. (rate={rate:.2f} req/s)")


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