FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie tous les modules Python du simulateur
COPY *.py ./

CMD ["python", "-u", "simulator.py"]
