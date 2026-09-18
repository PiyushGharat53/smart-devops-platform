FROM python:3.10-slim

# Force Python stdout/stderr to stream directly without buffering
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir --default-timeout=100 -r requirements.txt
COPY . .

RUN useradd -m sentineluser
USER sentineluser

CMD ["python", "traffic_watchdog.py"]