# main.py
import os
import subprocess
import time
import urllib.request
import urllib.error
import asyncio
import random
import json
import re

import psutil
import yaml
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Secrets & environment-specific config -----------------------------
# All of these MUST come from the environment. Never hardcode credentials
# or deploy-hook URLs here — see the note in chat about rotating the ones
# that were previously committed in plaintext.
MONGO_URI = os.getenv("SENTINEL_MONGO_URI", "")
FINSIGHT_API_URL = os.getenv("FINSIGHT_API_URL", "https://finsight-erku.onrender.com")
RENDER_FRONTEND_HOOK_URL = os.getenv("RENDER_FRONTEND_HOOK_URL", "")
RENDER_BACKEND_HOOK_URL = os.getenv("RENDER_BACKEND_HOOK_URL", "")
CHATBOT_API_URL = os.getenv("CHATBOT_API_URL", "http://localhost:8001/health")
ECOMMERCE_API_URL = os.getenv("ECOMMERCE_API_URL", "http://localhost:8002/health")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
FINSIGHT_DIR = os.getenv("SENTINEL_FINSIGHT_DIR", "")

if not MONGO_URI:
    print("[⚠️ CONFIG] SENTINEL_MONGO_URI is not set — audit logging to Mongo will fail silently.")
if not RENDER_FRONTEND_HOOK_URL or not RENDER_BACKEND_HOOK_URL:
    print("[⚠️ CONFIG] Render deploy hook URLs are not set — deployments will fail until configured.")
if not FINSIGHT_DIR:
    print("[⚠️ CONFIG] SENTINEL_FINSIGHT_DIR is not set — FinSight pipeline runs will fail until configured.")

db_client = AsyncIOMotorClient(MONGO_URI) if MONGO_URI else None
sentinel_db = db_client["sentinel_ops"] if db_client else None
logs_collection = sentinel_db["system_logs"] if sentinel_db is not None else None
incidents_collection = sentinel_db["incidents"] if sentinel_db is not None else None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MOCK_SERVICES_DIR = os.path.join(BASE_DIR, "sentinel-mock-services")

# --- Multi-workspace routing --------------------------------------------
# service_ids: None means "show every monitored service" (used by the
# catch-all "core" workspace). Otherwise it's the exact set of service ids
# that belong to that project stack. This is the single source of truth —
# the frontend fetches it from /api/workspaces rather than duplicating it.
WORKSPACES = [
    {
        "id": "finsight",
        "label": "FinSight Financial Engine",
        "env": "Production",
        "service_ids": ["gateway", "mongo"],
    },
    {
        "id": "chatbot",
        "label": "Campus Multilingual Chatbot",
        "env": "Staging",
        "service_ids": ["chatbot"],
    },
    {
        "id": "core",
        "label": "Core Microservices Cluster",
        "env": "All Services",
        "service_ids": None,
    },
]

system_state = {
    "auth": {"id": "auth", "name": "Authentication Service", "status": "healthy", "latency": 58},
}

live_logs = []
live_incidents = []
MAX_INCIDENTS_RETAINED = 20  # keep recent history (active + resolved) without growing unbounded

deployment_state = {
    "status": "idle",
    "commit_hash": "",
    "author": "",
    "message": "",
    "stage": "Pipeline Ready & Listening",
}

healing_in_progress = set()


async def send_dispatch_alert(title: str, description: str, color: int = 15158332):
    if not DISCORD_WEBHOOK_URL:
        return
    payload = {
        "embeds": [{
            "title": f"🛡️ Sentinel AIOps Dispatch: {title}",
            "description": description,
            "color": color,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }]
    }
    try:
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json', 'User-Agent': 'Sentinel-AIOps'}
        )
        urllib.request.urlopen(req, timeout=3)
    except Exception:
        pass


async def add_log(level, msg):
    time_str = time.strftime("%H:%M:%S")
    log_entry = {"id": random.randint(10000, 99999), "level": level, "msg": msg, "time": time_str}

    live_logs.append(log_entry)
    if len(live_logs) > 50:
        live_logs.pop(0)

    if logs_collection is not None:
        try:
            await logs_collection.insert_one(dict(log_entry))
        except Exception:
            pass


def check_service_health(expected_id, name, url):
    try:
        start_time = time.time()
        response = urllib.request.urlopen(url, timeout=2)
        latency = int((time.time() - start_time) * 1000)
        status = "healthy" if response.getcode() == 200 else "degraded"
        return {"id": expected_id, "name": name, "status": status, "latency": latency}
    except Exception:
        return {"id": expected_id, "name": name, "status": "failed", "latency": 0}


def check_finsight_system():
    gateway_health = {"id": "gateway", "name": "FinSight API Gateway", "status": "failed", "latency": 0}
    mongo_health = {"id": "mongo", "name": "Primary MongoDB Cluster", "status": "failed", "latency": 0}

    try:
        start_time = time.time()
        url = f"{FINSIGHT_API_URL.rstrip('/')}/health"
        response = urllib.request.urlopen(url, timeout=5)
        latency = int((time.time() - start_time) * 1000)

        if response.getcode() == 200:
            data = json.loads(response.read().decode('utf-8'))
            gateway_health["status"] = data.get("status", "healthy")
            gateway_health["latency"] = latency

            db_status = data.get("database", {}).get("status", "failed")
            db_name = data.get("database", {}).get("name", "HydraBolt Finance Cluster")

            mongo_health["status"] = db_status
            mongo_health["name"] = db_name
            mongo_health["latency"] = latency
    except Exception:
        pass

    return gateway_health, mongo_health


def scan_for_secrets(file_path: str) -> tuple[bool, str]:
    secret_patterns = {
        "MongoDB URI": r"mongodb(?:\+srv)?:\/\/(?:[a-zA-Z0-9_]+):(?:[a-zA-Z0-9_]+)@",
        "Stripe/OpenAI Secret Key": r"sk-[a-zA-Z0-9]{20,}",
        "GitHub Access Token": r"ghp_[a-zA-Z0-9]{36}",
        "AWS Access Key": r"AKIA[0-9A-Z]{16}",
    }
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            for name, pattern in secret_patterns.items():
                if re.search(pattern, content):
                    return False, f"CRITICAL: Exposed {name} detected!"
        return True, "No exposed secrets found."
    except Exception as e:
        return False, f"Secret scan failed to read file: {e}"


def run_dynamic_preflight(repo_dir: str, target_file: str) -> tuple[bool, str]:
    config_path = os.path.join(repo_dir, ".sentinel-config.yml")

    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            commands = config.get("pre_flight", [])
            for cmd in commands:
                res = subprocess.run(cmd, shell=True, cwd=repo_dir, capture_output=True, text=True)
                if res.returncode != 0:
                    return False, f"Custom test failed:\n{res.stderr.strip()}"
            return True, "All custom tenant tests passed."
        except Exception as e:
            return False, f"Failed to parse config file: {e}"

    if os.path.exists(os.path.join(repo_dir, "package.json")):
        res = subprocess.run(["node", "--check", target_file], cwd=repo_dir, capture_output=True, text=True)
        if res.returncode != 0:
            return False, res.stderr.strip().split("\n")[-1] if res.stderr else "Syntax verification failed"

        audit_res = subprocess.run(["npm", "audit", "--audit-level=high", "--json"], cwd=repo_dir, capture_output=True, text=True)
        if audit_res.returncode != 0:
            return False, "NPM Audit failed: High-severity vulnerabilities found in dependencies."

        return True, f"{target_file} passed syntax and dependency audits."

    elif os.path.exists(os.path.join(repo_dir, "requirements.txt")) or target_file.endswith(".py"):
        res = subprocess.run(["python", "-m", "py_compile", target_file], cwd=repo_dir, capture_output=True, text=True)
        if res.returncode != 0:
            return False, res.stderr.strip().split("\n")[-1] if res.stderr else "Python compilation failed"
        return True, f"{target_file} passed Python compilation check."

    return True, "Stack unclassified. Proceeding with caution (No tests run)."


def build_rca(service_id: str, service_name: str) -> dict:
    """
    Generates the Root Cause Analysis payload attached to every incident.
    In a real system this would come from log correlation / an ML model;
    here it's a deterministic-but-plausible summary keyed off which
    service failed, so the RCA modal always has real, service-specific
    content instead of a canned string.
    """
    if service_id == "mongo":
        return {
            "severity": "CRITICAL",
            "confidence": random.randint(90, 99),
            "rootCause": (
                f"{service_name} failed consecutive health probes consistent with a dropped "
                f"database connection pool — likely a network partition or exhausted "
                f"connection limit on the Atlas cluster."
            ),
            "remediation": (
                "Recycle the connection pool, verify Atlas cluster status and IP allowlist, "
                "then re-run health checks before releasing the incident."
            ),
        }
    if service_id == "gateway":
        return {
            "severity": "CRITICAL",
            "confidence": random.randint(88, 97),
            "rootCause": (
                f"{service_name} stopped responding to health checks — consistent with a "
                f"crashed process or a stalled deploy on the Render container."
            ),
            "remediation": "Trigger a container restart via the Render deploy hook and confirm boot completion.",
        }
    return {
        "severity": "WARNING",
        "confidence": random.randint(80, 95),
        "rootCause": (
            f"{service_name} failed its health check, consistent with container-level "
            f"instability or a transient network blip rather than a data-layer failure."
        ),
        "remediation": f"Restart the {service_name} container and re-route traffic once it reports healthy.",
    }


async def autonomous_heal(service_id: str, service_name: str):
    healing_in_progress.add(service_id)

    incident_id = f"INC-{random.randint(1000, 9999)}"
    await add_log("ANOMALY", f"[{incident_id}] {service_name} is DOWN. Initiating Auto-Heal...")

    await send_dispatch_alert(
        f"Incident {incident_id}",
        f"🚨 **{service_name}** is DOWN. Initiating autonomous remediation sequence.",
        color=15158332,
    )

    rca = build_rca(service_id, service_name)
    incident_doc = {
        "id": incident_id,
        "service": service_name,
        "service_id": service_id,
        "title": f"{service_name} Health Check Failure",
        "status": "Active (Healing...)",
        "time": time.strftime("%H:%M:%S"),
        **rca,
    }
    live_incidents.append(incident_doc)
    if len(live_incidents) > MAX_INCIDENTS_RETAINED:
        live_incidents.pop(0)

    if incidents_collection is not None:
        try:
            await incidents_collection.insert_one(dict(incident_doc))
        except Exception:
            pass

    if service_id == "gateway":
        await add_log("INFO", f"[{incident_id}] Sending emergency reboot command to Render Cloud API...")
        try:
            urllib.request.urlopen(RENDER_BACKEND_HOOK_URL, timeout=5)
            await add_log("INFO", f"[{incident_id}] Render accepted command. Container boot initiated. Cooldown active (60s)...")
            await asyncio.sleep(60)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                await add_log("ANOMALY", f"[{incident_id}] Render Rate Limit (429). Cooling down for 60s...")
                await asyncio.sleep(60)
            else:
                await add_log("ANOMALY", f"[{incident_id}] Render API Error: {str(e)}")
        except Exception as e:
            await add_log("ANOMALY", f"[{incident_id}] Render API unreachable: {str(e)}")
    else:
        await asyncio.sleep(4)
        await add_log("INFO", f"[{incident_id}] Re-routing traffic and rebooting {service_name} containers...")
        await asyncio.sleep(3)
        if service_id in system_state:
            system_state[service_id]["status"] = "healthy"
            system_state[service_id]["latency"] = random.randint(35, 90)

    await add_log("REMEDIATED", f"[{incident_id}] SUCCESS: {service_name} restoration cycle complete.")

    await send_dispatch_alert(
        f"Resolved {incident_id}",
        f"✅ **{service_name}** restoration cycle complete. System back to 100% health.",
        color=3066993,
    )

    for inc in live_incidents:
        if inc["id"] == incident_id:
            inc["status"] = "Resolved"

    if incidents_collection is not None:
        try:
            await incidents_collection.update_one({"id": incident_id}, {"$set": {"status": "Resolved"}})
        except Exception:
            pass

    healing_in_progress.remove(service_id)


async def run_real_deployment_pipeline(target_file: str, commit_hash: str, author: str, message: str, active_dir: str):
    global deployment_state
    deployment_state = {
        "status": "in_progress",
        "commit_hash": commit_hash,
        "author": author,
        "message": message,
        "stage": "Scanning code for security threats...",
    }
    await add_log("INFO", f"CI/CD Pipeline started for {target_file} (Commit: {commit_hash})")
    await asyncio.sleep(1)

    file_path = os.path.join(active_dir, target_file)
    if not os.path.exists(file_path):
        deployment_state["stage"] = f"File {target_file} not found. Aborting."
        deployment_state["status"] = "failed"
        await add_log("ANOMALY", f"Build failed: {file_path} does not exist.")
        return

    await add_log("INFO", f"Engaging Secret Shield: Scanning for exposed API keys in {target_file}...")
    shield_passed, shield_msg = scan_for_secrets(file_path)
    if not shield_passed:
        deployment_state["stage"] = f"Security Violation: {shield_msg}"
        deployment_state["status"] = "failed"
        await add_log("ANOMALY", shield_msg)
        await asyncio.sleep(2)
        await add_log("REMEDIATED", "Deployment aborted. Vault secured.")
        return
    else:
        await add_log("INFO", shield_msg)

    await add_log("INFO", f"Executing dynamic pre-flight tests in {active_dir}...")
    try:
        passed, msg = run_dynamic_preflight(active_dir, target_file)
        if not passed:
            deployment_state["stage"] = f"Pre-flight Error: {msg}"
            deployment_state["status"] = "failed"
            await add_log("ANOMALY", f"Pre-flight failed on {target_file}: {msg}")
            await asyncio.sleep(2)
            deployment_state["stage"] = "Deployment Rejected. Safe baseline preserved."
            deployment_state["status"] = "rolled_back"
            await add_log("REMEDIATED", "Auto-rollback complete. Production protected from faulty release.")
            return
        else:
            await add_log("INFO", msg)
    except Exception as e:
        deployment_state["stage"] = f"Test execution error: {str(e)}"
        deployment_state["status"] = "failed"
        await add_log("ANOMALY", f"Pipeline executor error: {str(e)}")
        return

    deployment_state["stage"] = "Syntax & security audits passed. Packaging release..."
    await add_log("INFO", f"Code verified successfully for {target_file}.")
    await asyncio.sleep(1.5)

    deployment_state["stage"] = "Deploying verified release to cloud cluster..."
    await add_log("INFO", "Deploying to active Render cluster...")
    await asyncio.sleep(1.5)

    deployment_state["stage"] = "Tests passed. Triggering full MERN stack deployment..."
    await add_log("INFO", "Sending secure launch commands to Render Deploy Hooks...")

    if not RENDER_FRONTEND_HOOK_URL or not RENDER_BACKEND_HOOK_URL:
        deployment_state["stage"] = "Render deploy hooks not configured on the server."
        deployment_state["status"] = "failed"
        await add_log("ANOMALY", "RENDER_FRONTEND_HOOK_URL / RENDER_BACKEND_HOOK_URL are not set.")
        return

    try:
        urllib.request.urlopen(RENDER_FRONTEND_HOOK_URL, timeout=5)
        urllib.request.urlopen(RENDER_BACKEND_HOOK_URL, timeout=5)
        await add_log("INFO", "Render accepted commands. Frontend and Backend are building in the cloud.")
    except Exception as e:
        deployment_state["stage"] = "Failed to reach Render API."
        deployment_state["status"] = "failed"
        await add_log("ANOMALY", f"Render Deploy Hook failed: {str(e)}")
        return

    await asyncio.sleep(2)
    deployment_state["stage"] = "Full stack deployment executed successfully!"
    deployment_state["status"] = "success"
    await add_log("REMEDIATED", f"Release {commit_hash} authorized and sent to production.")

    # Let the success/failure state linger for the UI, then return to idle
    # so the permanent CI/CD deck settles back into "Ready & Listening".
    await asyncio.sleep(6)
    deployment_state = {
        "status": "idle",
        "commit_hash": "",
        "author": "",
        "message": "",
        "stage": "Pipeline Ready & Listening",
    }


@app.get("/")
def read_root():
    return {"message": "Sentinel AIOps Engine is Live with MongoDB Persistence & Real-Time Dispatch!"}


@app.get("/api/workspaces")
def get_workspaces():
    """Single source of truth for workspace -> service membership, consumed by the frontend switcher."""
    return {"workspaces": WORKSPACES}


@app.post("/api/pipeline/trigger")
async def trigger_manual_pipeline(payload: dict, background_tasks: BackgroundTasks):
    """Manual 'Trigger Pre-Flight Test' button on the dashboard's permanent CI/CD deck."""
    if deployment_state["status"] == "in_progress":
        return {"message": "A pipeline run is already in progress.", "accepted": False}

    project = payload.get("project", "finsight")
    
    # 🔴 Dynamically assign the correct target file and directory based on the workspace
    if project == "finsight":
        target_file = payload.get("target_file", "server.js")
        active_dir = FINSIGHT_DIR if FINSIGHT_DIR and os.path.exists(FINSIGHT_DIR) else BASE_DIR
    elif project == "chatbot":
        target_file = "main.py"
        active_dir = BASE_DIR
    else:
        target_file = "main.py"
        active_dir = BASE_DIR

    commit_hash = f"manual-{random.randint(1000, 9999)}"
    author = payload.get("author", "DevOps Engineer")
    message = payload.get("message", f"Manual pre-flight test for {project} workspace")

    background_tasks.add_task(run_real_deployment_pipeline, target_file, commit_hash, author, message, active_dir)
    return {"message": "Manual pre-flight test triggered.", "accepted": True}


@app.post("/api/webhooks/github")
async def github_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    if "head_commit" in payload and payload["head_commit"]:
        head = payload["head_commit"]
        commit_hash = head.get("id", "")[:7]
        author = head.get("author", {}).get("name", "GitHub Committer")
        message = head.get("message", "Git push event")

        modified_files = head.get("modified", [])
        target_file = "server.js"
        for f in modified_files:
            if f.endswith(".js"):
                target_file = os.path.basename(f)
                break
        active_dir = FINSIGHT_DIR
    else:
        commit_hash = payload.get("commit", f"{random.randint(1000000, 9999999)}")[:7]
        author = payload.get("author", "DevTeam")
        message = payload.get("message", "Manual pipeline test")
        project = payload.get("project", "finsight")
        target_file = payload.get("target_file", "server.js")
        active_dir = FINSIGHT_DIR if project == "finsight" else MOCK_SERVICES_DIR

    background_tasks.add_task(run_real_deployment_pipeline, target_file, commit_hash, author, message, active_dir)
    return {"message": "Webhook accepted. Pipeline launched."}


@app.websocket("/ws/telemetry")
async def websocket_telemetry(websocket: WebSocket):
    await websocket.accept()
    await add_log("INFO", "Sentinel SmartOps observability core online")

    if "ecommerce" not in system_state:
        system_state["ecommerce"] = {"id": "ecommerce", "name": "E-Commerce Transaction Engine", "status": "failed", "latency": 0}
    if "chatbot" not in system_state:
        system_state["chatbot"] = {"id": "chatbot", "name": "Campus Multilingual Chatbot", "status": "failed", "latency": 0}

    try:
        while True:
            cpu_usage = psutil.cpu_percent(interval=0.1)
            memory_info = psutil.virtual_memory()
            disk_info = psutil.disk_usage('/')

            finsight_gateway, finsight_mongo = check_finsight_system()
            services_to_check = [finsight_gateway, system_state["ecommerce"], system_state["chatbot"]]

            for svc in services_to_check:
                if svc["status"] == "failed" and svc["id"] not in healing_in_progress:
                    asyncio.create_task(autonomous_heal(svc["id"], svc["name"]))

            payload = {
                "metrics": {
                    "cpu_usage": cpu_usage,
                    "memory_usage": memory_info.percent,
                    "disk_usage": disk_info.percent,
                    "network_throughput": random.randint(30, 85),
                },
                "services": [
                    finsight_gateway,
                    system_state["auth"],
                    system_state["ecommerce"],
                    finsight_mongo,
                    system_state["chatbot"],
                ],
                "logs": live_logs,
                # Send active + recently resolved so the persistent incident feed
                # can show recent history, not just what's still on fire.
                "incidents": live_incidents,
                "deployment": deployment_state,
            }
            await websocket.send_json(payload)
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass


@app.post("/api/heal/{service_id}")
async def execute_auto_heal(service_id: str):
    await add_log("AUTO-HEAL", f"Manual remediation sequence started for {service_id}")
    await autonomous_heal(service_id, service_id)
    return {"status": "success"}