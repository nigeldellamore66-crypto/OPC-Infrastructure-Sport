FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    great-expectations==0.18.15 \
    psycopg2-binary \
    sqlalchemy

# Scipt monté en volume
CMD ["python", "run_quality_checks.py"]
