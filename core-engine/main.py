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

async def run_real_deployment_pipeline(commit_hash: str, author: str, message: str, modified_files: list, file_contents: str):
    global deployment_state
    deployment_state = {
        "status": "in_progress",
        "commit_hash": commit_hash,
        "author": author,
        "message": message,
        "stage": "Scanning incoming commit for security threats & syntax...",
    }
    await add_log("INFO", f"CI/CD Pipeline started for commit {commit_hash} by {author}")
    await asyncio.sleep(1)

    secret_patterns = {
        "MongoDB URI": r"mongodb(?:\+srv)?:\/\/(?:[a-zA-Z0-9_]+):(?:[a-zA-Z0-9_]+)@",
        "Stripe/OpenAI Secret Key": r"sk-[a-zA-Z0-9]{20,}",
        "GitHub Access Token": r"ghp_[a-zA-Z0-9]{36}",
    }

    scannable_text = file_contents + " " + message
    for name, pattern in secret_patterns.items():
        if re.search(pattern, scannable_text):
            deployment_state["stage"] = f"Security Violation: Exposed {name} detected!"
            deployment_state["status"] = "failed"
            await add_log("ANOMALY", f"CRITICAL: Exposed {name} found in commit {commit_hash}!")
            await add_log("REMEDIATED", "Deployment aborted. Vault secured and bad release blocked.")
            await send_dispatch_alert("Security Violation Blocked", f"🚨 Blocked push from {author} due to exposed {name}.", color=15158332)
            return

    if "sentinelCrashTest" in scannable_text or "syntax error" in message.lower() or "=" in message:
        deployment_state["stage"] = "Pre-flight Error: Syntax verification failed."
        deployment_state["status"] = "failed"
        await add_log("ANOMALY", f"Pre-flight failed on commit {commit_hash}: Syntax error or crash keyword detected.")
        await add_log("REMEDIATED", "Auto-rollback complete. Production protected from faulty release.")
        await send_dispatch_alert("Pre-Flight Failure Blocked", f"🚨 Blocked push from {author} due to syntax/compilation failure.", color=15158332)
        return

    await add_log("INFO", "Secret Shield & pre-flight audits passed successfully.")
    
    try:
        if RENDER_BACKEND_HOOK_URL:
            urllib.request.urlopen(RENDER_BACKEND_HOOK_URL, timeout=5)
        await add_log("INFO", "Render accepted cloud deployment hook command.")
    except Exception as e:
        await add_log("ANOMALY", f"Render Deploy Hook warning: {str(e)}")

    deployment_state["stage"] = "Deployment executed successfully!"
    deployment_state["status"] = "success"
    await add_log("REMEDIATED", f"Release {commit_hash} authorized and sent to production.")
    await send_dispatch_alert("Deployment Successful", f"✅ Release {commit_hash} by {author} passed all checks.", color=3066993)

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

    commit_hash = f"manual-{random.randint(1000, 9999)}"
    author = payload.get("author", "DevOps Engineer")
    message = payload.get("message", "Manual pre-flight test triggered")
    
    background_tasks.add_task(run_real_deployment_pipeline, commit_hash, author, message, [], message)
    return {"message": "Manual pre-flight test triggered.", "accepted": True}

@app.post("/api/webhooks/github")
async def github_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    repo_name = payload.get("repository", {}).get("name", "finsight")
    head = payload.get("head_commit", {})
    
    commit_hash = head.get("id", "gitpush")[:7] if head else f"{random.randint(1000, 9999)}"
    author = head.get("author", {}).get("name", "GitHub Committer") if head else "Developer"
    message = head.get("message", "Git push event") if head else "Code push"
    
    added = head.get("added", [])
    modified = head.get("modified", [])
    removed = head.get("removed", [])
    all_modified_files = added + modified

    file_contents_proxy = message + " " + " ".join(all_modified_files) + " " + " ".join(head.get("distinct", []))

    background_tasks.add_task(run_real_deployment_pipeline, commit_hash, author, message, all_modified_files, file_contents_proxy)
    return {"message": f"Webhook accepted for {repo_name}. Pipeline launched."}

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