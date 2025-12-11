# --- Basisimage ---
FROM python:3.10-slim

# --- Arbeitsverzeichnis ---
WORKDIR /app

# --- Systemabhängigkeiten (optional falls pip etwas braucht) ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# --- Python-Abhängigkeiten installieren ---
# Falls du eine requirements.txt hast, sonst entfernen:
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# OPTIONAL: Beispielabhängigkeiten für Kafka
RUN pip install --no-cache-dir kafka-python requests

# --- Dein Python-Skript in das Image kopieren ---
COPY weather_kafka_producer.py .

# --- Command / EntryPoint ---
CMD ["python", "weather_kafka_producer.py"]
