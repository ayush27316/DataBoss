FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg2 and dbt
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# dbt profiles.yml is written at runtime by dbt_runner.py — not baked in
# credentials.json is NOT copied; use GCP_SA_JSON env var on Railway

ENV PORT=8000
EXPOSE 8000

CMD sh -c "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"
