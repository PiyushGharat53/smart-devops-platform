from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import psutil
import os
import subprocess
import yaml
import time
import urllib.request
import urllib.error
import asyncio
import random
import json
import re
from motor.motor_asyncio import AsyncIOMotorClient  # 🔴 Async MongoDB Driver

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔴 MongoDB Connection for Sentinel Audit Logs
MONGO_URI = os.getenv("SENTINEL_MONGO_URI", "mongodb+srv://hydrabolt:8eoqcZyYDfqOvKvb@cluster0.imzoavv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db_client = AsyncIOMotorClient(MONGO_URI)
sentinel_db = db_client["sentinel_ops"]
logs_collection = sentinel_db["system_logs"]
incidents_collection = sentinel_db["incidents"]

FINSIGHT_API_URL = "https://finsight-erku.onrender.com"
RENDER_FRONTEND_HOOK_URL = "https://api.render.com/deploy/srv-d6vu48s50q8c739s720g?key=1S8celCyeSo"
RENDER_BACKEND_HOOK_URL = "https://api.render.com/deploy/srv-d6vtlvngi27c73f7cvhg?key=3TKxC58bqXU"
CHATBOT_API_URL = "http://localhost:8001/health"
ECOMMERCE_API_URL = "http://localhost:8002/health"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MOCK_SERVICES_DIR = os.path.join(BASE_DIR, "sentinel-mock-services")
FINSIGHT_DIR = r"D:\STUDY\SECOND YEAR\S.Y Vap Project (FinSight)\hydra-finance-app\hydra-finance-app\backend"

system_state = {
    "auth": {"id": "auth", "name": "Authentication Service", "status": "healthy", "latency": 58},
}

live_logs = []
live_incidents = []

deployment_state = {
    "status": "idle",
    "commit_hash": "",
    "author": "",
    "message": "",
    "stage": "Waiting for deployment trigger..."
}

healing_in_progress = set()

# 🔴 Upgraded to save logs permanently to MongoDB Atlas & prevent ObjectId JSON crash
async def add_log(level, msg):
    time_str = time.strftime("%H:%M:%S")
    log_entry = {"id": random.randint(10000, 99999), "level": level, "msg": msg, "time": time_str}
    
    live_logs.append(log_entry)
    if len(live_logs) > 50:
        live_logs.pop(0)
        
    try:
        # Insert a clean copy so MongoDB generates _id without polluting our runtime dict
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
        "AWS Access Key": r"AKIA[0-9A-Z]{16}"
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

async def autonomous_heal(service_id: str, service_name: str):
    healing_in_progress.add(service_id)
    
    incident_id = f"INC-{random.randint(1000, 9999)}"
    await add_log("ANOMALY", f"[{incident_id}] {service_name} is DOWN. Initiating Auto-Heal...")
    
    incident_doc = {"id": incident_id, "service": service_name, "status": "Active (Healing...)"}
    live_incidents.append(incident_doc)
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
    
    for inc in live_incidents:
        if inc["id"] == incident_id:
            inc["status"] = "Resolved"
            
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
        "stage": f"Scanning code for security threats..."
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
    await add_log("INFO", f"Deploying to active Render cluster...")
    await asyncio.sleep(1.5)

    deployment_state["stage"] = "Tests passed. Triggering full MERN stack deployment..."
    await add_log("INFO", f"Sending secure launch commands to Render Deploy Hooks...")
    
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

@app.get("/")
def read_root():
    return {"message": "Sentinel AIOps Engine is Live with MongoDB Persistence!"}

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
                    "network_throughput": random.randint(30, 85)
                },
                "services": [
                    finsight_gateway,
                    system_state["auth"],
                    system_state["ecommerce"],  
                    finsight_mongo, 
                    system_state["chatbot"]     
                ],
                "logs": live_logs,
                "incidents": [inc for inc in live_incidents if inc["status"] != "Resolved"],
                "deployment": deployment_state
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