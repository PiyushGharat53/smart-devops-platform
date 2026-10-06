# 🛡️ Sentinel SmartOps — Autonomous SRE & Active Defense Platform

Sentinel SmartOps is an autonomous Site Reliability Engineering (SRE) and active defense system designed to monitor, protect, and self-heal cloud-hosted web applications. 

We built Sentinel as an independent operational layer around **FinSight** (our production MERN finance tracking application). Instead of treating monitoring and security as an afterthought or relying on slow manual interventions, Sentinel continuously tracks incoming telemetry, detects traffic anomalies, auto-quarantines abusive IPs via active defense middleware, dispatches real-time incident reports to SecOps, and enables autonomous self-healing recovery.

---

## 🌐 Live Production Deployments

| Component | Platform | Live URL |
| :--- | :--- | :--- |
| **Sentinel SRE Command Center** | Vercel (Edge) | [smart-devops-platform.vercel.app](https://smart-devops-platform.vercel.app) |
| **Sentinel AIOps Engine & Watchdog** | Render Cloud | [sentinel-aiops-engine.onrender.com](https://sentinel-aiops-engine.onrender.com) |
| **FinSight Target Application** | Render Cloud | [finsight-erku.onrender.com/#/](https://finsight-erku.onrender.com/#/) |

---

## 💡 Why We Built This

When deploying full-stack web applications to public cloud providers, services face constant production threats:
1. **Unchecked Volumetric Surges & DoS Attempts**: Spikes in rapid requests can exhaust server CPU, crash Node.js event loops, and exceed cloud quota limits.
2. **Database Resource Starvation**: Unrestricted spam queries saturate MongoDB Atlas connection pools, degrading performance for legitimate users.
3. **Slow Manual SRE Response**: Human engineers often notice outages minutes or hours after they happen, leading to extended downtime.
4. **Cloud Egress Restrictions**: Free-tier cloud environments (like Render) block raw outbound SMTP ports (25, 465, 587), causing standard email notification scripts to fail silently with network errors.

Sentinel SmartOps solves these problems by providing an external, automated guard dog that sits alongside the application to observe, protect, and heal without human delays.

---

## 🏗️ System Architecture

```text
       [ Legitimate Users ]                [ Attacking Rogue Clients ]
                │                                      │
                ▼                                      ▼
      ┌─────────────────────────────────────────────────────────────┐
      │          FinSight Gateway (Node.js / Express API)           │
      │  - Rolling 10s In-Memory Request Window                     │
      │  - Active Defense Shield Middleware                         │
      │  - Normal Traffic Allowed -> Business Logic & Mongo Atlas   │
      │  - Volumetric Surge (>20 req/10s) -> Intercepted & Reported │
      └──────────────┬───────────────────────────────┬──────────────┘
                     │                               │
        Reports Threat / Checks IP                   │ Redirects to Challenge
                     │                               ▼
                     │              ┌─────────────────────────────────┐
                     │              │ HTTP 429 Security Challenge     │
                     │              │ - Live incident telemetry       │
                     │              │ - Auto-polls for SRE release    │
                     │              │ - Instant redirect on restore   │
                     │              └─────────────────────────────────┘
                     ▼
      ┌─────────────────────────────────────────────────────────────┐
      │              Sentinel AIOps Engine (FastAPI)                │
      │  - Background Telemetry Watchdog (polls /metrics every 2s)  │
      │  - Dynamic IP Quarantine Jail & Duration Controller         │
      │  - Cloud SecOps Email Dispatch via Resend (HTTPS 443)       │
      │  - Discord Operations Webhook Alerts                        │
      │  - MongoDB Atlas Audit Trail with 30-Day TTL Auto-Purge     │
      └──────────────────────────────┬──────────────────────────────┘
                                     │
                     WebSocket Telemetry & REST API
                                     │
                                     ▼
      ┌─────────────────────────────────────────────────────────────┐
      │          Sentinel SRE Command Center (React + Vite)         │
      │  - Real-time RPS & Latency Radar Area Charts (Recharts)     │
      │  - Active Defense IP Quarantine Jail & Manual IP Blocker    │
      │  - SRE Duration Overrides (5m, 1h, Permanent Ban, Release)  │
      │  - Interactive Pre-Flight CI/CD Pipeline Simulator          │
      │  - Live SecOps SMTP & Cloud API Configuration Modal         │
      └─────────────────────────────────────────────────────────────┘
```

---

## ⚡ Key Features & Engineering Highlights

### 1. Active Defense Shield & Volumetric Surge Detection
- **Rolling Window Rate Limiting**: FinSight's Express gateway monitors incoming requests per IP across a rolling 10-second window.
- **Autonomous Blast Shield**: When an IP exceeds 20 requests within 10 seconds, FinSight immediately flags the client as a threat.
- **Automated SRE Handshake**: The gateway auto-reports the offending IP to Sentinel (`POST /api/security/report-threat`), adding it to the quarantine jail instantly without requiring manual SRE entry.

### 2. HTTP 429 Active Defense Challenge Screen & Zero-Click Recovery
- **Direct Client Isolation**: Quarantined clients are intercepted with an HTTP 429 status and redirected to `/challenge?ip=<client_ip>`.
- **Transparent Telemetry**: The challenge screen displays why the request was blocked (Incident ID, IST timestamp, Quarantined IP, Protection Layer).
- **Zero-Click Real-Time Recovery**: The challenge screen polls Sentinel every 1 second in the background. The moment an SRE operator clicks "Release" on the dashboard (or auto-cooldown expires), the page displays **"✅ Access Restored by SRE!"** and automatically returns the user to FinSight within ~800ms.
- **Instant F5 Refresh Handling**: If an unbanned user refreshes `/challenge`, the server verifies their status on the fly and immediately redirects them back to FinSight.

### 3. SRE Quarantine Duration & Threat Matrix Controls
- From the Sentinel dashboard, operators have granular control over jailed IPs:
  - **5m**: Temporary cooldown block for mild bursts.
  - **1h**: Extended quarantine for persistent offenders.
  - **Permanent Ban**: Full block requiring manual SRE revocation.
  - **Release**: Instant pardon that unlocks the client immediately.
- **Manual IP Block**: Allows SecOps engineers to proactively jail any suspicious IP address with custom reasoning.
- **Threat Matrix**: Documented multi-tier defense policy from Tier 1 (Normal 0–8 req/s) up to Tier 4 (Autonomous Remediation).

### 4. Cloud SecOps Alerting (Render Cloud Egress Safe)
- **The Problem**: Render's free tier firewall blocks outbound SMTP traffic on ports 25, 465, and 587 (`[Errno 101] Network is unreachable`), causing traditional Python `smtplib` scripts to crash.
- **The Fix**: Sentinel incorporates a dedicated HTTPS API driver using **Resend Cloud API** over standard Port 443. 
- **Instant Incident Dispatch**: Security alerts and manual test dispatches are delivered directly to the SecOps inbox (`gharatpiyush63@gmail.com`) with full incident details and styled HTML formatting.
- **Fallback Support**: Also supports Brevo API and standard Gmail SMTP when running in environments with open outbound ports.

### 5. Live Telemetry Watchdog & Real-Time Radar
- **Asynchronous Watchdog**: The Python engine continuously queries FinSight's internal `/metrics` endpoint every 2 seconds without blocking the event loop.
- **WebSocket Streaming**: Telemetry updates (RPS, memory usage, active connections) are pushed in real time to the React dashboard over `/ws/telemetry`.
- **Dynamic Visuals**: Rendered as responsive area charts with peak surge indicators and baseline comparison lines.
- **Discord Operations Webhook**: Sends rich embeds to a team Discord channel whenever critical incidents or autonomous self-healing events occur.

### 6. MongoDB Atlas 30-Day TTL Auto-Purge
- Telemetry events and security audit logs are persisted to a cloud MongoDB Atlas cluster.
- Uses a MongoDB native **TTL (Time-To-Live) index** on timestamp fields:
  ```javascript
  db.audit_logs.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 2592000 })
  ```
- Stale incident records and telemetry snapshots older than 30 days are automatically pruned by MongoDB background threads, eliminating disk bloat without manual maintenance.

### 7. Interactive Pre-Flight CI/CD Pipeline Simulator
- Powered by our custom `.sentinel-config.yml` blueprint.
- Evaluates code safety across 5 distinct validation gates before deployment:
  1. **Gate 1**: Dependency Resolution (`npm install`)
  2. **Gate 2**: Security Vulnerability Audit (`npm audit --audit-level=high`)
  3. **Gate 3**: AST Syntax Compilation Integrity (`node --check server.js`)
  4. **Gate 4**: Test Suite Execution (`npm test`)
  5. **Gate 5**: Production Health Probe & Deployment Verification
- SRE engineers can trigger and inspect the pre-flight pipeline directly from the dashboard UI.

### 8. Chaos Resilience & Graceful Shutdown
- Target services implement graceful termination handlers for `SIGTERM` and `SIGINT`.
- During deployment cycles or pod restarts, open HTTP Keep-Alive connections and MongoDB connection pools are cleanly drained within a 10-second safety window, preventing corrupted database writes and dropped user requests.

---

## 🗂️ Project Structure

```text
smart-devops-platform/
│
├── core-engine/                       # Sentinel SRE & AIOps Backend
│   ├── main.py                        # FastAPI Server, REST APIs, Security Endpoints & Challenge
│   ├── traffic_watchdog.py            # Asynchronous background telemetry collector & Discord alerter
│   ├── requirements.txt               # Python dependencies (FastAPI, Uvicorn, Motor, Requests, etc.)
│   └── Dockerfile                     # Container definition for cloud deployment
│
├── dashboard-ui/                      # Sentinel Command Center Frontend
│   ├── src/
│   │   ├── App.jsx                    # Core Dashboard UI (Radar, Jail, Modals, Logs, CI/CD)
│   │   ├── App.css                    # Custom styles, animations, and dark mode tokens
│   │   ├── main.jsx                   # React root entry point
│   │   └── components/
│   │       └── LiveTrafficChart.jsx   # Real-time Recharts telemetry visualization
│   ├── package.json                   # UI dependencies (React, Lucide icons, Recharts, Vite)
│   ├── vite.config.js                 # Vite build & proxy configuration
│   └── .env.production                # Production API endpoint targets
│
├── .sentinel-config.yml               # CI/CD Pre-Flight Quality Gates definition
└── README.md                          # Project documentation
```

---

## 💻 Local Development Setup

### Prerequisites
- Node.js (v18+) & npm
- Python (v3.10+)
- MongoDB (Local instance or free MongoDB Atlas URI)
---
