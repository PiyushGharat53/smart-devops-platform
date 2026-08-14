# main.py
import os
import signal

from fastapi import FastAPI

app = FastAPI(title="Infra Monitoring Platform")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/crash")
def crash():
    # Intentionally kill the process to test self-healing/restart logic
    os.kill(os.getpid(), signal.SIGKILL)