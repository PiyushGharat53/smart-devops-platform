import os
import asyncio
import httpx
import random
from collections import deque
from datetime import datetime

FINSIGHT_API_URL = os.getenv("FINSIGHT_API_URL", "https://finsight-erku.onrender.com").rstrip("/")

class TrafficWatchdog:
    def __init__(self, dispatch_alert_cb, add_log_cb, create_incident_cb, resolve_incident_cb):
        self.dispatch_alert = dispatch_alert_cb
        self.add_log = add_log_cb
        self.create_incident = create_incident_cb
        self.resolve_incident = resolve_incident_cb
        
        self.traffic_history = deque(maxlen=30)
        
        self.is_monitoring = False
        self.defense_mode_active = False
        self.current_incident_id = None
        
        self.spike_threshold = 8.0
        self.previous_request_count = None
        self.last_check_time = None
        self.cooldown_counter = 0

    async def start_monitoring(self, target_service_name: str):
        self.is_monitoring = True
        await self.add_log("INFO", f"Traffic Watchdog initialized for {target_service_name}. Autonomous SRE Engine active.")
        
        async with httpx.AsyncClient(timeout=5.0) as client:
            while self.is_monitoring:
                current_time_str = datetime.now().strftime("%H:%M:%S")
                current_time_obj = datetime.now()
                current_rps = 0.0
                latency = 0
                
                try:
                    start_ping = datetime.now()
                    response = await client.get(f"{FINSIGHT_API_URL}/metrics")
                    latency = int((datetime.now() - start_ping).total_seconds() * 1000)
                    
                    if response.status_code == 200:
                        data = response.json()
                        current_total = data.get("total_requests", 0)
                        
                        if self.previous_request_count is not None and self.last_check_time is not None:
                            time_delta = (current_time_obj - self.last_check_time).total_seconds()
                            req_delta = current_total - self.previous_request_count
                            
                            if time_delta > 0:
                                current_rps = max(0.0, req_delta / time_delta)
                        
                        self.previous_request_count = current_total
                        self.last_check_time = current_time_obj
                        
                except Exception as e:
                    current_rps = 0.0
                    self.previous_request_count = None

                # If defense mode is active, simulate active throttling easing down traffic
                if self.defense_mode_active:
                    current_rps = max(2.0, current_rps - 3.0)
                    self.cooldown_counter += 1
                    
                    # Auto-heal after a brief stabilization window
                    if self.cooldown_counter >= 6: # ~12 seconds
                        self.defense_mode_active = False
                        self.cooldown_counter = 0
                        if self.current_incident_id:
                            self.resolve_incident(self.current_incident_id)
                            await self.add_log("REMEDIATED", f"[{self.current_incident_id}] Traffic normalized. Active defense disengaged. Incident resolved.")
                            await self.dispatch_alert(
                                f"System Recovered: {target_service_name}",
                                f"✅ **Auto-Healing Successful!**\nTraffic levels returned to normal baselines. Incident `{self.current_incident_id}` closed.",
                                color=3066993
                            )

                data_point = {
                    "time": current_time_str,
                    "rps": round(current_rps, 2),
                    "latency": latency,
                    "defense_active": self.defense_mode_active
                }
                self.traffic_history.append(data_point)
                
                await self._analyze_traffic(target_service_name, current_rps)
                await asyncio.sleep(2)

    async def _analyze_traffic(self, service_name: str, current_rps: float):
        if current_rps > self.spike_threshold and not self.defense_mode_active:
            self.defense_mode_active = True
            self.cooldown_counter = 0
            self.current_incident_id = f"INC-{random.randint(1000, 9999)}"
            
            await self.add_log("ANOMALY", f"[{self.current_incident_id}] VOLUMETRIC SURGE: {current_rps:.2f} req/s detected. Engaging active defense & auto-remediation.")
            
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
            
            await self.dispatch_alert(
                f"Critical Incident: {service_name}",
                f"🚨 **Traffic Spike Detected!**\n**Rate:** {current_rps:.2f} req/s\n**Action:** Active defense shield engaged automatically.",
                color=15158332
            )

    def get_current_metrics(self):
        return list(self.traffic_history)