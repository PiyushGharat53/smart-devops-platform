# 🛡️ FinSight x Sentinel SmartOps: Autonomous Finance & Self-Healing SRE Command Center

Welcome to the unified repository for **FinSight**, a MERN-stack personal finance tracker, and **Sentinel SmartOps**, our custom-built, enterprise-grade Site Reliability Engineering (SRE) and AIOps platform.

---

## 📖 Our Journey: From Application to Enterprise Infrastructure

This project began as **FinSight**, a robust financial tracking application designed to help users monitor income, expenses, and savings goals. However, as we prepared for cloud deployment, we realized that building a functional application was only half the engineering challenge. The real challenge was **keeping it alive in production**.

Instead of manually monitoring the app, we built **Sentinel SmartOps** around it. We evolved our architecture from a simple client-server model into a resilient, autonomous ecosystem. We implemented real-time telemetry, active defense middleware to prevent DDoS attacks, closed-loop auto-remediation, and a strict CI/CD pipeline. 

This repository demonstrates not just full-stack development, but modern DevOps, infrastructure automation, and Site Reliability Engineering.

---

## 🚀 Core Architectural Features

### 1. 📊 Live Network Traffic Watchdog (Telemetry)
* **Real-Time Observability:** A custom Python SRE engine asynchronously polls the Node.js backend (`/metrics`) every 2 seconds.
* **Dynamic Visualization:** Calculates live Requests Per Second (RPS) and streams it to the React dashboard, rendering smooth, real-time area charts using Recharts.

### 2. 🛡️ Active Defense Shield & Rate Limiting
* **In-Memory IP Tracking:** Custom Express.js middleware tracks request frequencies per client IP within a rolling 10-second window.
* **Automated Blast Shield:** If a single IP exceeds 20 rapid requests, the middleware instantly isolates the user and returns an HTTP `429 Too Many Requests` error, protecting the MongoDB database and server CPU from crashing.

### 3. 🤖 Automated Incident Intelligence & Alerting
* **Heuristic Anomaly Detection:** When traffic surges past the safe baseline (8.0 RPS), the Sentinel engine flags an anomaly and generates a unique Incident Ticket (e.g., `INC-2227`).
* **Discord Webhook Integration:** Critical incident data (RPS rate, incident ID, and mitigation status) is immediately dispatched as a rich-text embed to our team's Discord operations channel.

### 4. 🔄 Autonomous Closed-Loop Self-Healing
* **Zero-Touch Remediation:** Sentinel does not just alert humans; it manages the crisis. Once defense mode engages, the engine monitors the traffic for stabilization. 
* **Auto-Recovery:** After a 12-second normal baseline cooldown, Sentinel automatically disengages the defense shields, logs a `[REMEDIATED]` status, and sends a green recovery confirmation to Discord.

---

## 🚦 Continuous Integration & Delivery (CI/CD)

Our deployment lifecycle is strictly governed by a custom `.sentinel-config.yml` policy blueprint. Before any code is allowed to reach our Render production servers, the automated pipeline enforces strict pre-flight quality gates:

1. **Dependency Resolution:** Automates `npm install` for a clean build environment.
2. **Security Vulnerability Scanning:** Executes `npm audit --audit-level=high` to detect and block vulnerable packages.
3. **Syntax Integrity Checks:** Runs `node --check server.js` to compile the execution tree and catch fatal syntax errors before runtime.
4. **Code Linting:** Triggers `eslint` to validate coding standards and syntax health.

*If any of these pre-flight gates fail, the deployment is hard-blocked, ensuring zero broken code enters production.*

---

## 🛠️ Technology Stack

**Frontend (Client-Side)**
* **React.js & Vite:** Core UI framework.
* **Recharts:** Real-time dynamic SVG charting.
* **Vercel:** Edge-network cloud hosting.

**Backend (Server-Side)**
* **Node.js & Express.js:** REST API and Active Defense Middleware.
* **Render:** Cloud application hosting.

**Database**
* **MongoDB Atlas:** Fully managed cloud NoSQL database.

**SRE & AIOps Engine (Sentinel)**
* **Python (Asyncio, HTTPX):** High-performance, non-blocking telemetry polling.
* **Discord API:** Real-time webhook operations alerts.

---

## 📂 Repository Architecture (Monorepo)

```text
finsight-sentinel-monorepo/
│
├── core-engine/         # Python AIOps Backend & Watchdog
│   ├── traffic_watchdog.py
│   └── requirements.txt
│
├── backend/             # Node.js Express API & Active Defense
│   ├── server.js        # Core API & Rate-Limiting Middleware
│   ├── .sentinel-config.yml # CI/CD Pre-flight Policy
│   └── package.json
│
└── dashboard-ui/        # React & Tailwind Command Center
    ├── src/
    │   └── components/
    │       └── LiveTrafficChart.jsx
    ├── package.json
    └── vite.config.js