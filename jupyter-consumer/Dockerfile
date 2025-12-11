FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Modell-Datei ins Image kopieren
# (vorher: python training/train_rf_with_mlflow.py ausgeführt haben!)
COPY models/rf_model.pkl /models/rf_model.pkl

# Optional: Notebooks
COPY notebooks/ ./notebooks

ENV PYTHONUNBUFFERED=1
ENV MODEL_PATH="/models/rf_model.pkl"

EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token="]
