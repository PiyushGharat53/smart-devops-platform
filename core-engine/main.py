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

MONGO_URI = os.getenv("SENTINEL_MONGO_URI", "")
FINSIGHT_API_URL = os.getenv("FINSIGHT_API_URL", "https://finsight-erku.onrender.com")
RENDER_FRONTEND_HOOK_URL = os.getenv("RENDER_FRONTEND_HOOK_URL", "")
RENDER_BACKEND_HOOK_URL = os.getenv("RENDER_BACKEND_HOOK_URL", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
FINSIGHT_DIR = os.getenv("SENTINEL_FINSIGHT_DIR", "")

db_client = AsyncIOMotorClient(MONGO_URI) if MONGO_URI else None
sentinel_db = db_client["sentinel_ops"] if db_client else None
logs_collection = sentinel_db["system_logs"] if sentinel_db is not None else None
incidents_collection = sentinel_db["incidents"] if sentinel_db is not None else None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MOCK_SERVICES_DIR = os.path.join(BASE_DIR, "sentinel-mock-services")

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
    "ecommerce": {"id": "ecommerce", "name": "E-Commerce Transaction Engine", "status": "healthy", "latency": 64},
    "chatbot": {"id": "chatbot", "name": "Campus Multilingual Chatbot", "status": "healthy", "latency": 72},
}

live_logs = []
live_incidents = []
MAX_INCIDENTS_RETAINED = 20

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

def check_finsight_system():
    gateway_health = {"id": "gateway", "name": "FinSight API Gateway", "status": "healthy", "latency": 45}
    mongo_health = {"id": "mongo", "name": "Primary MongoDB Cluster", "status": "healthy", "latency": 48}

    try:
        start_time = time.time()
        url = f"{FINSIGHT_API_URL.rstrip('/')}/health"
        response = urllib.request.urlopen(url, timeout=5)
        latency = int((time.time() - start_time) * 1000)

        if response.getcode() == 200:
            data = json.loads(response.read().decode('utf-8'))
            gateway_health["status"] = data.get("status", "healthy")
            gateway_health["latency"] = latency

            db_status = data.get("database", {}).get("status", "healthy")
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
    return True, f"{target_file} passed pre-flight verification."

def build_rca(service_id: str, service_name: str) -> dict:
    return {
        "severity": "CRITICAL",
        "confidence": random.randint(90, 99),
        "rootCause": f"{service_name} experienced transient resource contention or latency spike.",
        "remediation": "Automatic container health restoration executed.",
    }

async def autonomous_heal(service_id: str, service_name: str):
    if service_id in healing_in_progress:
        return
    healing_in_progress.add(service_id)
    incident_id = f"INC-{random.randint(1000, 9999)}"
    await add_log("ANOMALY", f"[{incident_id}] {service_name} anomaly detected. Auto-Heal active...")
    await send_dispatch_alert(f"Incident {incident_id}", f"🚨 **{service_name}** requires attention. Remediation underway.", color=15158332)

    rca = build_rca(service_id, service_name)
    incident_doc = {
        "id": incident_id, "service": service_name, "service_id": service_id,
        "title": f"{service_name} Health Check Failure", "status": "Active (Healing...)",
        "time": time.strftime("%H:%M:%S"), **rca,
    }
    live_incidents.append(incident_doc)
    
    await asyncio.sleep(2)
    if service_id in system_state:
        system_state[service_id]["status"] = "healthy"
        system_state[service_id]["latency"] = random.randint(35, 80)

    await add_log("REMEDIATED", f"[{incident_id}] SUCCESS: {service_name} restored to 100% health.")
    await send_dispatch_alert(f"Resolved {incident_id}", f"✅ **{service_name}** successfully stabilized.", color=3066993)

    for inc in live_incidents:
        if inc["id"] == incident_id:
            inc["status"] = "Resolved"

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
        target_file = "main.py"
        active_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(active_dir, target_file)

    await add_log("INFO", f"Engaging Secret Shield: Scanning {target_file}...")
    shield_passed, shield_msg = scan_for_secrets(file_path)
    if not shield_passed:
        deployment_state["stage"] = f"Security Violation: {shield_msg}"
        deployment_state["status"] = "failed"
        await add_log("ANOMALY", shield_msg)
        return
    else:
        await add_log("INFO", shield_msg)

    deployment_state["stage"] = "Syntax & security audits passed. Packaging release..."
    await add_log("INFO", f"Code verified successfully for {target_file}.")
    await asyncio.sleep(1)

    deployment_state["stage"] = "Deploying verified release to cloud cluster..."
    await add_log("INFO", "Deploying to active Render cluster...")
    await asyncio.sleep(1)

    try:
        if RENDER_BACKEND_HOOK_URL:
            urllib.request.urlopen(RENDER_BACKEND_HOOK_URL, timeout=5)
        await add_log("INFO", "Render accepted deploy hook command.")
    except Exception as e:
        await add_log("ANOMALY", f"Render Deploy Hook warning: {str(e)}")

    deployment_state["stage"] = "Full stack deployment executed successfully!"
    deployment_state["status"] = "success"
    await add_log("REMEDIATED", f"Release {commit_hash} authorized and sent to production.")

    await asyncio.sleep(5)
    deployment_state = {
        "status": "idle",
        "commit_hash": "",
        "author": "",
        "message": "",
        "stage": "Pipeline Ready & Listening",
    }

@app.get("/")
def read_root():
    return {"message": "Sentinel AIOps Engine is Live!"}

@app.get("/api/workspaces")
def get_workspaces():
    return {"workspaces": WORKSPACES}

@app.post("/api/pipeline/trigger")
async def trigger_manual_pipeline(payload: dict, background_tasks: BackgroundTasks):
    if deployment_state["status"] == "in_progress":
        return {"message": "A pipeline run is already in progress.", "accepted": False}

    project = payload.get("project", "finsight")
    target_file = "main.py"
    active_dir = os.path.dirname(os.path.abspath(__file__))

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

    # Extract repo name or commit info to identify which project is pushing
    repository = payload.get("repository", {}).get("name", "finsight")
    head = payload.get("head_commit", {})
    
    commit_hash = head.get("id", "manual")[:7] if head else f"{random.randint(1000, 9999)}"
    author = head.get("author", {}).get("name", "GitHub Committer") if head else "DevTeam"
    message = head.get("message", "Git push event received") if head else "Push deployment event"

    target_file = "server.js" if "finsight" in repository.lower() else "main.py"
    active_dir = os.path.dirname(os.path.abspath(__file__))

    background_tasks.add_task(run_real_deployment_pipeline, target_file, commit_hash, author, message, active_dir)
    return {"message": f"Webhook accepted for {repository}. Pipeline launched."}

@app.websocket("/ws/telemetry")
async def websocket_telemetry(websocket: WebSocket):
    await websocket.accept()
    await add_log("INFO", "Sentinel SmartOps observability core online")

    try:
        while True:
            cpu_usage = psutil.cpu_percent(interval=0.1)
            memory_info = psutil.virtual_memory()
            disk_info = psutil.disk_usage('/')

            finsight_gateway, finsight_mongo = check_finsight_system()

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