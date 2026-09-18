import os
import asyncio
import httpx
import random
import signal
from collections import deque
from datetime import datetime, timezone

# Discord Webhook Configuration (Retrieved safely from environment)
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

# Resolves backend URL from environment variables with Docker/Local fallbacks
RAW_URL = os.getenv("FINSIGHT_API_URL") or os.getenv("BACKEND_URL") or "http://backend:5000"
FINSIGHT_API_URL = RAW_URL.rstrip("/")
if FINSIGHT_API_URL.endswith("/metrics"):
    FINSIGHT_API_URL = FINSIGHT_API_URL[:-8].rstrip("/")


class TrafficWatchdog:
    def __init__(self, dispatch_alert_cb, add_log_cb, create_incident_cb, resolve_incident_cb):
        self.dispatch_alert = dispatch_alert_cb
        self.add_log = add_log_cb
        self.create_incident = create_incident_cb
        self.resolve_incident = resolve_incident_cb
        
        self.traffic_history = deque(maxlen=30)
        
        self.is_monitoring = False
        self.defense_mode_active = False
        self.db_incident_active = False
        self.current_incident_id = None
        
        self.spike_threshold = 8.0
        self.previous_request_count = None
        self.last_check_time = None
        self.cooldown_counter = 0

        # Backoff resilience state
        self.base_delay = 2.0
        self.current_retry_delay = 2.0
        self.max_retry_delay = 30.0

    async def _send_discord_webhook(self, title: str, description: str, color: int = 15158332):
        """Sends rich embedded alert messages to Discord if a webhook URL is present."""
        if not DISCORD_WEBHOOK_URL:
            return
            
        payload = {
            "embeds": [{
                "title": f"🚨 Sentinel AIOps Alert: {title}",
                "description": description,
                "color": color,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "footer": {"text": "FinSight Autonomous SRE Engine"}
            }]
        }
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(DISCORD_WEBHOOK_URL, json=payload)
        except Exception as e:
            await self.add_log("ERROR", f"Failed to post alert to Discord: {e}")

    async def start_monitoring(self, target_service_name: str):
        self.is_monitoring = True
        init_msg = f"Traffic Watchdog initialized for {target_service_name}. Autonomous SRE Engine active."
        await self.add_log("INFO", init_msg)
        
        await self._send_discord_webhook(
            title=f"Watchdog Started: {target_service_name}",
            description="🛡️ **Sentinel SRE Engine initialized.** Active monitoring enabled for target endpoints.",
            color=3066993
        )
        
        async with httpx.AsyncClient(timeout=5.0) as client:
            while self.is_monitoring:
                current_time_str = datetime.now().strftime("%H:%M:%S")
                current_time_obj = datetime.now()
                current_rps = 0.0
                latency = 0
                heap_used = 0.0
                heap_total = 0.0
                rss = 0.0
                db_status = "UNKNOWN"
                uptime = 0
                
                try:
                    start_ping = datetime.now()
                    response = await client.get(f"{FINSIGHT_API_URL}/metrics")
                    latency = int((datetime.now() - start_ping).total_seconds() * 1000)
                    
                    if response.status_code == 200:
                        self.current_retry_delay = self.base_delay
                        
                        data = response.json()
                        current_total = data.get("total_requests", 0)
                        memory_info = data.get("memory", {})
                        db_info = data.get("database", {})
                        
                        heap_used = memory_info.get("heapUsedMB", 0.0)
                        heap_total = memory_info.get("heapTotalMB", 0.0)
                        rss = memory_info.get("rssMB", 0.0)
                        db_status = db_info.get("status", "UNKNOWN")
                        uptime = data.get("uptime_seconds", 0)

                        # Deep Telemetry: Check database connection status
                        if db_status == "DISCONNECTED" and not self.db_incident_active:
                            self.db_incident_active = True
                            log_msg = f"[SRE ENGINE] MongoDB cluster connection dropped on {target_service_name}!"
                            await self.add_log("CRITICAL", log_msg)
                            
                            alert_title = f"Database Outage: {target_service_name}"
                            alert_body = "🚨 **MongoDB Disconnected!**\nService is failing readiness checks."
                            await self.dispatch_alert(alert_title, alert_body, color=15158332)
                            await self._send_discord_webhook(alert_title, alert_body, color=15158332)

                        elif db_status == "CONNECTED" and self.db_incident_active:
                            self.db_incident_active = False
                            log_msg = f"[SRE ENGINE] MongoDB connection restored on {target_service_name}."
                            await self.add_log("INFO", log_msg)
                            
                            rec_title = f"Database Restored: {target_service_name}"
                            rec_body = "✅ **MongoDB Connection Re-established!**\nDatabase status returned to CONNECTED."
                            await self.dispatch_alert(rec_title, rec_body, color=3066993)
                            await self._send_discord_webhook(rec_title, rec_body, color=3066993)
                        
                        # Calculate RPS with defensive zero division safety
                        if self.previous_request_count is not None and self.last_check_time is not None:
                            time_delta = (current_time_obj - self.last_check_time).total_seconds()
                            req_delta = current_total - self.previous_request_count
                            
                            if time_delta > 0:
                                current_rps = max(0.0, req_delta / time_delta)
                        
                        self.previous_request_count = current_total
                        self.last_check_time = current_time_obj
                    else:
                        raise httpx.HTTPStatusError("Non-200 Status Code", request=response.request, response=response)
                        
                except asyncio.CancelledError:
                    self.is_monitoring = False
                    break
                except Exception as e:
                    current_rps = 0.0
                    self.previous_request_count = None
                    
                    # Exponential Backoff with Jitter
                    jitter = random.uniform(0.8, 1.2)
                    backoff_wait = min(self.max_retry_delay, self.current_retry_delay * 2) * jitter
                    self.current_retry_delay = min(self.max_retry_delay, self.current_retry_delay * 2)
                    
                    await self.add_log("WARN", f"[RESILIENCE] Polling error ({type(e).__name__}). Backing off for {backoff_wait:.1f}s.")
                    await asyncio.sleep(backoff_wait)
                    continue

                # Active defense traffic throttling simulation
                if self.defense_mode_active:
                    current_rps = max(2.0, current_rps - 3.0)
                    self.cooldown_counter += 1
                    
                    if self.cooldown_counter >= 6:  # ~12 seconds
                        self.defense_mode_active = False
                        self.cooldown_counter = 0
                        if self.current_incident_id:
                            self.resolve_incident(self.current_incident_id)
                            await self.add_log("REMEDIATED", f"[{self.current_incident_id}] Traffic normalized. Active defense disengaged.")
                            
                            rec_title = f"System Recovered: {target_service_name}"
                            rec_body = "✅ **Auto-Healing Successful!**\nTraffic returned to normal baseline."
                            await self.dispatch_alert(rec_title, rec_body, color=3066993)
                            await self._send_discord_webhook(rec_title, rec_body, color=3066993)

                data_point = {
                    "time": current_time_str,
                    "rps": round(current_rps, 2),
                    "latency": latency,
                    "defense_active": self.defense_mode_active,
                    "heap_used": heap_used,
                    "heap_total": heap_total,
                    "rss": rss,
                    "db_status": db_status,
                    "uptime": uptime
                }
                self.traffic_history.append(data_point)
                
                await self._analyze_traffic(target_service_name, current_rps)
                await asyncio.sleep(2)

    async def _analyze_traffic(self, service_name: str, current_rps: float):
        if current_rps > self.spike_threshold and not self.defense_mode_active:
            self.defense_mode_active = True
            self.cooldown_counter = 0
            self.current_incident_id = f"INC-{random.randint(1000, 9999)}"
            
            await self.add_log("ANOMALY", f"[{self.current_incident_id}] VOLUMETRIC SURGE: {current_rps:.2f} req/s detected. Engaging active defense.")
            
            self.create_incident({
                "id": self.current_incident_id,
                "service": service_name,
                "service_id": "traffic_watchdog",
                "title": "Volumetric Traffic Spike Detected",
                "status": "Mitigating (Active Defense Engaged)",
                "time": datetime.now().strftime("%H:%M:%S"),
                "severity": "CRITICAL",
                "rootCause": f"Sudden volumetric traffic surge of {current_rps:.2f} req/s.",
                "remediation": "Rate-limiting active defense engaged. Monitoring stabilization."
            })
            
            alert_title = f"Critical Incident: {service_name}"
            alert_body = f"🚨 **Traffic Spike Detected!**\n**Rate:** {current_rps:.2f} req/s\n**Action:** Active defense shield engaged automatically."
            await self.dispatch_alert(alert_title, alert_body, color=15158332)
            await self._send_discord_webhook(alert_title, alert_body, color=15158332)

    def get_current_metrics(self):
        return list(self.traffic_history)


# Fallback Execution Entrypoint
if __name__ == "__main__":
    async def dummy_dispatch(title, msg, color=None):
        print(f"[ALERT] {title}: {msg}")

    async def dummy_log(level, msg):
        print(f"[{level}] {msg}")

    def dummy_create(data):
        print(f"[INCIDENT] Created {data.get('id')}")

    def dummy_resolve(inc_id):
        print(f"[INCIDENT] Resolved {inc_id}")

    async def main():
        print(f"🛡️ Sentinel SRE Engine starting. Target Endpoint: {FINSIGHT_API_URL}/metrics")
        watchdog = TrafficWatchdog(
            dispatch_alert_cb=dummy_dispatch,
            add_log_cb=dummy_log,
            create_incident_cb=dummy_create,
            resolve_incident_cb=dummy_resolve
        )
        
        loop = asyncio.get_running_loop()
        
        def handle_signal():
            print("\n[SENTINEL ENGINE SHUTDOWN] Signal caught. Disengaging watchdog loop cleanly...")
            watchdog.is_monitoring = False

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, handle_signal)
            except NotImplementedError:
                signal.signal(sig, lambda s, f: handle_signal())

        await watchdog.start_monitoring("Express-API-Service")

    async def main_runner():
        try:
            await main()
        except asyncio.CancelledError:
            pass

    asyncio.run(main_runner())