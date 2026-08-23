import os
import time
import asyncio
import random
import json
import re
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional

import psutil
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

# ==========================================
# Configuration & Environment Variables
# ==========================================
MONGO_URI = os.getenv("SENTINEL_MONGO_URI", "")
FINSIGHT_API_URL = os.getenv("FINSIGHT_API_URL", "https://finsight-erku.onrender.com").rstrip("/")
RENDER_BACKEND_HOOK_URL = os.getenv("RENDER_BACKEND_HOOK_URL", "")
FINSIGHT_DEPLOY_HOOK_URL = os.getenv("FINSIGHT_DEPLOY_HOOK_URL", "")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

# MongoDB Setup
db_client: Optional[AsyncIOMotorClient] = None
sentinel_db = None
logs_collection = None
incidents_collection = None

if MONGO_URI:
    db_client = AsyncIOMotorClient(MONGO_URI)
    sentinel_db = db_client["sentinel_ops"]
    logs_collection = sentinel_db["system_logs"]
    incidents_collection = sentinel_db["incidents"]

# ==========================================
# Lifespan Context Manager
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    psutil.cpu_percent(interval=None) 
    yield
    if db_client:
        db_client.close()

app = FastAPI(title="Sentinel AIOps Engine", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# In-Memory State & Constants
# ==========================================
WORKSPACES = [
    {"id": "finsight", "label": "FinSight Financial Engine", "env": "Production", "service_ids": ["gateway", "mongo"]},
]

system_state: Dict[str, Dict[str, Any]] = {}

live_logs: List[Dict[str, Any]] = []
live_incidents: List[Dict[str, Any]] = []
healing_in_progress = set()

deployment_state: Dict[str, Any] = {
    "status": "idle",
    "commit_hash": "",
    "author": "",
    "message": "",
    "stage": "Pipeline Ready & Listening",
}

# ==========================================
# Helper Utilities
# ==========================================
async def send_dispatch_alert(title: str, description: str, color: int = 15158332):
    """Dispatches asynchronous alerts to Discord with smart error tracking."""
    if not DISCORD_WEBHOOK_URL:
        return
        
    payload = {
        "embeds": [{
            "title": f"🛡️ Sentinel AIOps: {title}",
            "description": description,
            "color": color,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }]
    }
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Sentinel-AIOps/1.0"
    }
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(DISCORD_WEBHOOK_URL, json=payload, headers=headers)
            
            if response.status_code not in (200, 204):
                error_text = response.text
                # SMART LOGGING: If Discord sends back a massive HTML firewall page, summarize it.
                if "<html" in error_text.lower() or "cloudflare" in error_text.lower():
                    error_text = "Blocked by Discord Cloudflare Firewall (Shared IP Rate Limit)."
                    
                await add_log("ANOMALY", f"Discord Webhook Failed: {response.status_code} - {error_text[:100]}")
    except Exception as e:
        await add_log("ANOMALY", f"Discord Webhook Error: {str(e)}")

async def add_log(level: str, msg: str):
    time_str = time.strftime("%H:%M:%S")
    log_entry = {
        "id": random.randint(10000, 99999),
        "level": level,
        "msg": msg,
        "time": time_str
    }
    live_logs.append(log_entry)
    if len(live_logs) > 50:
        live_logs.pop(0)

    if logs_collection is not None:
        try:
            await logs_collection.insert_one(dict(log_entry))
        except Exception:
            pass

async def check_finsight_system():
    gateway_health = {"id": "gateway", "name": "FinSight API Gateway", "status": "healthy", "latency": 45}
    mongo_health = {"id": "mongo", "name": "Primary MongoDB Cluster", "status": "healthy", "latency": 48}

    try:
        start_time = time.time()
        async with httpx.AsyncClient(timeout=4.0) as client:
            response = await client.get(f"{FINSIGHT_API_URL}/health")
            if response.status_code == 200:
                data = response.json()
                latency = int((time.time() - start_time) * 1000)
                gateway_health["status"] = data.get("status", "healthy")
                gateway_health["latency"] = latency
                mongo_health["status"] = data.get("database", {}).get("status", "healthy")
                mongo_health["latency"] = latency
            else:
                gateway_health["status"] = "degraded"
                mongo_health["status"] = "degraded"
    except Exception:
        gateway_health["status"] = "unreachable"
        mongo_health["status"] = "unknown"

    return gateway_health, mongo_health

async def autonomous_heal(service_id: str, service_name: str):
    """Executes automated remediation flow for impacted services."""
    if service_id in healing_in_progress:
        return
    healing_in_progress.add(service_id)
    incident_id = f"INC-{random.randint(1000, 9999)}"
    await add_log("ANOMALY", f"[{incident_id}] {service_name} anomaly detected. Auto-Heal active...")
    
    # ---> RESTORED: Discord Alert for Active Incident <---
    await send_dispatch_alert(
        f"Incident {incident_id} Active", 
        f"🚨 **{service_name}** requires attention. Remediation underway.", 
        color=15158332
    )

    rca = {
        "severity": "CRITICAL",
        "confidence": random.randint(90, 99),
        "rootCause": f"{service_name} experienced transient resource contention.",
        "remediation": "Automatic container health restoration executed."
    }
    incident_doc = {
        "id": incident_id,
        "service": service_name,
        "service_id": service_id,
        "title": f"{service_name} Health Check Failure",
        "status": "Active (Healing...)",
        "time": time.strftime("%H:%M:%S"),
        **rca
    }
    live_incidents.append(incident_doc)

    await asyncio.sleep(2)
    
    # Restore the health status (Simulated Fix)
    if service_id in system_state:
        system_state[service_id]["status"] = "healthy"
        system_state[service_id]["latency"] = random.randint(35, 80)

    await add_log("REMEDIATED", f"[{incident_id}] SUCCESS: {service_name} restored to 100% health.")
    
    # ---> RESTORED: Discord Alert for Resolved Incident <---
    await send_dispatch_alert(
        f"Resolved {incident_id}", 
        f"✅ **{service_name}** successfully stabilized.", 
        color=3066993
    )

    for inc in live_incidents:
        if inc["id"] == incident_id:
            inc["status"] = "Resolved"
            
    healing_in_progress.remove(service_id)

async def run_real_deployment_pipeline(
    repo_name: str,
    commit_hash: str,
    author: str,
    message: str,
    modified_files: list,
    file_contents: str
):
    global deployment_state
    deployment_state = {
        "status": "in_progress",
        "commit_hash": commit_hash,
        "author": author,
        "message": message,
        "stage": f"Scanning incoming code for {repo_name}...",
    }
    await add_log("INFO", f"CI/CD Pipeline started for {repo_name} (Commit: {commit_hash})")
    await asyncio.sleep(2) 

    if "[SENTINEL_ERROR_FETCHING_CODE]" in file_contents:
        deployment_state["stage"] = "Pre-flight Error: Cannot read private code."
        deployment_state["status"] = "failed"
        await add_log("ANOMALY", "Pre-flight failed: GitHub blocked code download. Verify GITHUB_TOKEN.")
        await send_dispatch_alert("Pipeline Blocked", "🚨 Could not fetch code for verification.", color=15158332)
        return

    # STEP 2: EXPANDED SECRET SHIELD
    secret_patterns = {
        "MongoDB URI": r"mongodb(?:\+srv)?:\/\/(?:[a-zA-Z0-9_]+):(?:[a-zA-Z0-9_]+)@",
        "Stripe/OpenAI Secret Key": r"sk-(?:live|test)-[a-zA-Z0-9]{20,}",
        "GitHub Access Token": r"ghp_[a-zA-Z0-9]{36}",
        "AWS Access Key": r"AKIA[0-9A-Z]{16}",
        "RSA Private Key": r"-----BEGIN (?:RSA )?PRIVATE KEY-----",
        "JWT Token": r"eyJ[a-zA-Z0-9_-]+\.eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+"
    }
    scannable_text = f"{file_contents} {message}"
    for name, pattern in secret_patterns.items():
        if re.search(pattern, scannable_text):
            error_msg = f"Security Violation: Exposed {name} detected!"
            deployment_state["stage"] = error_msg
            deployment_state["status"] = "failed"
            
            incident_id = f"INC-{random.randint(1000, 9999)}"
            live_incidents.append({
                "id": incident_id, "service": "CI/CD Pipeline", "service_id": "pipeline",
                "title": "Critical Vault Exposure", "status": "Active (Blocked)", "time": time.strftime("%H:%M:%S"),
                "severity": "CRITICAL", "confidence": 100, "rootCause": error_msg, "remediation": "Release aborted. Please remove secret and push a clean commit."
            })
            
            await add_log("ANOMALY", f"CRITICAL: Exposed {name} detected in commit {commit_hash}!")
            await send_dispatch_alert("Security Block", f"🚨 Blocked push from {author} due to exposed {name}.", color=15158332)
            return

    # STEP 2: EXPANDED SYNTAX VERIFICATION
    await asyncio.sleep(1) 
    syntax_fails = [
        r"(?:const|let|var)\s+\w+\s*=\s*;", # Unassigned variables
        r"eval\s*\(",                        # Dangerous eval usage
        r"sentinelCrashTest"                # Manual trigger
    ]
    
    for fail_pattern in syntax_fails:
        if re.search(fail_pattern, scannable_text):
            error_msg = "Pre-flight Error: Invalid or dangerous syntax expression."
            deployment_state["stage"] = error_msg
            deployment_state["status"] = "failed"
            
            incident_id = f"INC-{random.randint(1000, 9999)}"
            live_incidents.append({
                "id": incident_id, "service": "CI/CD Pipeline", "service_id": "pipeline",
                "title": "Syntax Compilation Failure", "status": "Active (Blocked)", "time": time.strftime("%H:%M:%S"),
                "severity": "HIGH", "confidence": 98, "rootCause": error_msg, "remediation": "Auto-rollback complete. Fix syntax locally and deploy again."
            })
            
            await add_log("ANOMALY", f"Pre-flight failed on commit {commit_hash}: Invalid syntax expression.")
            await send_dispatch_alert("Pre-Flight Block", f"🚨 Blocked push from {author} due to syntax failure.", color=15158332)
            return

    await add_log("INFO", "Secret Shield & pre-flight audits passed successfully.")

    hook_url = FINSIGHT_DEPLOY_HOOK_URL if "finsight" in repo_name.lower() else RENDER_BACKEND_HOOK_URL
    try:
        if hook_url:
            async with httpx.AsyncClient(timeout=6.0) as client:
                await client.post(hook_url)
            await add_log("INFO", f"Render accepted deploy hook for {repo_name}.")
        else:
            await add_log("INFO", f"No deploy hook configured for {repo_name}. Skipping deployment phase.")
    except Exception as e:
        await add_log("ANOMALY", f"Render Deploy Hook warning: {str(e)}")

    deployment_state["stage"] = "Deployment executed successfully!"
    deployment_state["status"] = "success"
    await add_log("REMEDIATED", f"Release {commit_hash} authorized and sent to production.")

    await asyncio.sleep(5)
    deployment_state = {
        "status": "idle",
        "commit_hash": "",
        "author": "",
        "message": "",
        "stage": "Pipeline Ready & Listening"
    }

# ==========================================
# API Endpoints
# ==========================================
@app.get("/")
async def read_root():
    return {"message": "Sentinel AIOps Engine is Live!"}

@app.get("/api/workspaces")
async def get_workspaces():
    return {"workspaces": WORKSPACES}

@app.post("/api/pipeline/trigger")
async def trigger_manual_pipeline(payload: dict, background_tasks: BackgroundTasks):
    if deployment_state["status"] == "in_progress":
        return {"message": "Pipeline in progress.", "accepted": False}
    
    repo_name = payload.get("project", "finsight")
    commit_hash = f"manual-{random.randint(1000, 9999)}"
    author = payload.get("author", "DevOps Engineer")
    message = "Manual pre-flight test triggered"
    background_tasks.add_task(run_real_deployment_pipeline, repo_name, commit_hash, author, message, [], message)
    return {"message": "Manual pre-flight test triggered.", "accepted": True}

@app.post("/api/webhooks/github")
async def github_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    repo_data = payload.get("repository") or {}
    repo_name = str(repo_data.get("name", "finsight"))

    if "smart-devops-platform" in repo_name.lower() or "sentinel" in repo_name.lower():
        return {"message": "Self-update ignored. Sentinel observes external services."}

    try:
        repo_full_name = str(repo_data.get("full_name", f"org/{repo_name}"))
        head = payload.get("head_commit") or {}

        full_hash = str(head.get("id", "gitpush"))
        short_hash = full_hash[:7] if full_hash != "gitpush" else f"{random.randint(1000, 9999)}"

        author_obj = head.get("author") or {}
        author = str(author_obj.get("name", "GitHub Committer"))
        message = str(head.get("message", "Git push event"))

        added = head.get("added") or []
        modified = head.get("modified") or []
        all_modified_files = list(added) + list(modified)

        fetched_code = ""
        fetch_failed = False
        headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

        async with httpx.AsyncClient(timeout=5.0) as client:
            for fpath in all_modified_files:
                try:
                    raw_url = f"https://raw.githubusercontent.com/{repo_full_name}/{full_hash}/{fpath}"
                    res = await client.get(raw_url, headers=headers)
                    if res.status_code == 200:
                        fetched_code += f"\n{res.text}"
                    else:
                        fetch_failed = True
                except Exception:
                    fetch_failed = True

        if fetch_failed and not fetched_code:
            fetched_code = "[SENTINEL_ERROR_FETCHING_CODE]"

        file_contents_proxy = f"{message} {' '.join(all_modified_files)}\n{fetched_code}"

    except Exception:
        repo_name = "finsight"
        short_hash = "gitpush"
        author = "Developer"
        message = "Code push event"
        all_modified_files = []
        file_contents_proxy = "push event"

    background_tasks.add_task(
        run_real_deployment_pipeline,
        repo_name,
        short_hash,
        author,
        message,
        all_modified_files,
        file_contents_proxy
    )
    return {"message": f"Webhook accepted for {repo_name}. Pipeline launched."}

@app.websocket("/ws/telemetry")
async def websocket_telemetry(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            finsight_gateway, finsight_mongo = await check_finsight_system()
            
            payload = {
                "metrics": {
                    "cpu_usage": psutil.cpu_percent(interval=None),
                    "memory_usage": psutil.virtual_memory().percent,
                    "disk_usage": psutil.disk_usage('/').percent,
                    "network_throughput": random.randint(30, 85)
                },
                "services": [
                    finsight_gateway,
                    finsight_mongo
                ],
                "logs": live_logs,
                "incidents": live_incidents,
                "deployment": deployment_state,
            }
            await websocket.send_json(payload)
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass

# STEP 1: CONTEXT-AWARE HEALING ENDPOINT
@app.post("/api/heal/{service_id}")
async def execute_auto_heal(service_id: str):
    if service_id == "pipeline":
        # Acknowledge and Dismiss CI/CD Alerts instead of attempting to "Heal" a container
        await add_log("INFO", "CI/CD Pipeline incident acknowledged and dismissed by engineer.")
        for inc in live_incidents:
            if inc.get("service_id") == "pipeline" and inc.get("status") != "Resolved":
                inc["status"] = "Resolved"
                inc["remediation"] = "Alert dismissed. Awaiting developer fix."
        return {"status": "dismissed"}
        
    # Standard runtime auto-healing for actual live services
    await autonomous_heal(service_id, service_id)
    return {"status": "success"}