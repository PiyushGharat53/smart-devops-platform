FROM python:3.10-slim

# Force Python stdout/stderr to stream directly without buffering
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir --default-timeout=100 -r requirements.txt
COPY . .

RUN useradd -m sentineluser
USER sentineluser

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000}"]