# main.py
import os
import signal

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(title="Infra Monitoring Platform")

Instrumentator().instrument(app).expose(app, endpoint="/metrics")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/crash")
def crash():
    # Intentionally kill the process to test self-healing/restart logic
    os.kill(os.getpid(), signal.SIGKILL)