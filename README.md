# 🛡️ Sentinel SmartOps: Autonomous DevOps & Self-Healing Command Center

Sentinel is an enterprise-grade AIOps platform designed to automate pre-flight security audits, real-time cloud telemetry monitoring, and autonomous service remediation without human intervention.

---

## 🚀 Key Architectural Features

### 1. 🔒 The Secret Shield (Vault)
* **Regex-Based Threat Detection:** Actively scans incoming code commits for exposed API keys (Stripe, OpenAI, GitHub Tokens) and database connection strings (`mongodb+srv://`).
* **Breach Prevention:** Instantly blocks deployment pipelines if credentials are hardcoded, protecting production clusters from leaks.

### 2. 🔍 Dynamic Pre-Flight Testing & Dependency Audits
* **Multi-Stack Auto-Detection:** Automatically identifies whether an ecosystem is Node.js or Python.
* **Vulnerability Scanning:** Runs automated syntax checks (`node --check`, `py_compile`) and high-severity dependency audits (`npm audit`) before authorizing cloud builds.

### 3. 🤖 The Autonomous Medic (Self-Healing Watchdog)
* **Real-Time Telemetry:** Connects directly to cloud microservices and MongoDB Atlas clusters, tracking round-trip latency and HTTP health states over WebSockets.
* **Zero-Touch Remediation:** When a service crashes (throwing 500-level errors), the AI watchdog creates an incident ticket and fires emergency webhooks to cloud deployment hooks (Render API) to spin up fresh containers.
* **Rate-Limit Protection:** Built-in 60-second cooldown timers and HTTP 429 error handlers prevent deployment loops and API spamming during recovery sequences.

---

## 📂 Repository Architecture (Monorepo)

```text
smart-devops-platform/
│
├── core-engine/         # Python FastAPI AIOps Backend & Watchdog
│   ├── main.py
│   └── requirements.txt
│
└── dashboard-ui/        # Real-time React & Tailwind Command Center
    ├── src/
    ├── package.json
    └── vite.config.js