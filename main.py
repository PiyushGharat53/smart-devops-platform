# main.py
import os
import signal
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(title="Infra Monitoring Platform")
app.mount("/static", StaticFiles(directory="static"), name="static")

Instrumentator().instrument(app).expose(app, endpoint="/metrics")

# --- Active Defense State ---
DEFENSE_MODE = False


@app.middleware("http")
async def defense_middleware(request: Request, call_next):
    """
    While DEFENSE_MODE is active, shed load on /health so that
    monitoring/self-healing traffic doesn't add to the overload.
    /metrics and /defense/* remain reachable at all times.
    """
    path = request.url.path

    if DEFENSE_MODE and path == "/health":
        return JSONResponse(
            status_code=429,
            content={"detail": "Defense mode active: /health temporarily unavailable."},
        )

    return await call_next(request)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/crash")
def crash():
    # Intentionally kill the process to test self-healing/restart logic
    os.kill(os.getpid(), signal.SIGKILL)


@app.post("/defense/enable")
def enable_defense():
    global DEFENSE_MODE
    DEFENSE_MODE = True
    return {"defense_mode": DEFENSE_MODE}


@app.post("/defense/disable")
def disable_defense():
    global DEFENSE_MODE
    DEFENSE_MODE = False
    return {"defense_mode": DEFENSE_MODE}


@app.get("/")
def serve_dashboard():
    with open("static/index.html", "r") as f:
        return HTMLResponse(content=f.read(), status_code=200)