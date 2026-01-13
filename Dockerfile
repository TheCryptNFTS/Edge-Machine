FROM python:3.11-slim

WORKDIR /app

# curl is optional, but useful. requests is in python, so this is safe.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["sh", "-c", "uvicorn pm.api:app --host 0.0.0.0 --port ${PORT:-8000}"]