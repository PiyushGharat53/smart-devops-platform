import asyncio
import random
from collections import deque
from datetime import datetime

class TrafficWatchdog:
    def __init__(self, dispatch_alert_cb, add_log_cb, create_incident_cb, resolve_incident_cb):
        # Callbacks to interact with main.py's global state & Discord
        self.dispatch_alert = dispatch_alert_cb
        self.add_log = add_log_cb
        self.create_incident = create_incident_cb
        self.resolve_incident = resolve_incident_cb
        
        # Time-series buffer for the frontend React graph (stores the last 30 data points)
        self.traffic_history = deque(maxlen=30)
        
        # Internal State
        self.is_monitoring = False
        self.defense_mode_active = False
        self.current_incident_id = None
        
        # Baseline parameters
        self.base_rps = 18.0
        self.spike_threshold = 45.0

    async def start_monitoring(self, target_service_name: str):
        """Asynchronous loop that continuously tracks traffic and watches for anomalies."""
        self.is_monitoring = True
        await self.add_log("INFO", f"Traffic Watchdog initialized for {target_service_name}. Monitoring live RPS...")
        
        while self.is_monitoring:
            current_time = datetime.now().strftime("%H:%M:%S")
            
            if self.defense_mode_active:
                # When shields are up, traffic is aggressively throttled
                current_rps = random.uniform(5.0, 12.0)
                latency = random.randint(30, 50)
            else:
                # 5% chance of a random massive traffic spike (simulating a DDoS or Viral load)
                if random.random() < 0.05:
                    current_rps = random.uniform(55.0, 95.0)
                else:
                    # Normal organic traffic fluctuation
                    current_rps = self.base_rps + random.uniform(-6.0, 6.0)
                
                # Latency naturally scales up as Requests Per Second increases
                latency = int(current_rps * random.uniform(1.2, 2.5))

            # Store data point in the rolling buffer
            data_point = {
                "time": current_time,
                "rps": round(current_rps, 2),
                "latency": latency,
                "defense_active": self.defense_mode_active
            }
            self.traffic_history.append(data_point)
            
            # Analyze for Anomalies
            await self._analyze_traffic(target_service_name, current_rps)
            
            # Tick every 2 seconds
            await asyncio.sleep(2)

    async def _analyze_traffic(self, service_name: str, current_rps: float):
        """Anomaly detection engine."""
        # If traffic spikes above threshold and shields are down
        if current_rps > self.spike_threshold and not self.defense_mode_active:
            self.defense_mode_active = True
            self.current_incident_id = f"INC-{random.randint(1000, 9999)}"
            
            await self.add_log("ANOMALY", f"[{self.current_incident_id}] CRITICAL: Traffic spike detected ({current_rps:.2f} req/s). Engaging Active Defense Shields.")
            
            # 1. Create the incident on the dashboard
            self.create_incident({
                "id": self.current_incident_id,
                "service": service_name,
                "service_id": "traffic_watchdog",
                "title": "Anomalous Traffic Spike Detected",
                "status": "Active (Shields Engaged)",
                "time": datetime.now().strftime("%H:%M:%S"),
                "severity": "CRITICAL",
                "confidence": 99,
                "rootCause": f"Sudden volumetric traffic surge: {current_rps:.2f} req/s.",
                "remediation": "Active Defense Shields engaged. Isolating abusive IPs."
            })
            
            # 2. Ping Discord
            await self.dispatch_alert(
                f"Traffic Spike: {service_name}",
                f"🚨 **Anomalous Traffic Spike Detected!**\n**Rate:** {current_rps:.2f} req/s\n**Action:** Active Defense Shields engaged. Target isolated.",
                color=15158332 # Red
            )
            
            # 3. Auto-resolve after 12 seconds of defense mode
            asyncio.create_task(self._auto_resolve_defense(service_name))

    async def _auto_resolve_defense(self, service_name: str):
        """Simulates the autonomous stabilization of the network."""
        await asyncio.sleep(12) 
        
        self.defense_mode_active = False
        incident_id = self.current_incident_id
        self.current_incident_id = None
        
        await self.add_log("REMEDIATED", f"[{incident_id}] Traffic normalized. Disengaging Active Defense Shields.")
        
        # Resolve the incident on the dashboard
        self.resolve_incident(incident_id, "Traffic normalized. Shields lowered.")
        
        # Ping Discord with the green clear
        await self.dispatch_alert(
            f"Traffic Normalized: {service_name}",
            f"✅ **Incident Resolved.**\nSystem has returned to nominal operating conditions. Active Defense Shields disengaged.",
            color=3066993 # Green
        )

    def get_current_metrics(self):
        """Returns the rolling buffer for the WebSocket payload."""
        return list(self.traffic_history)