FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    curl \
    ca-certificates \
    libgdal-dev \
    gdal-bin \
    libgeos-dev \
    libproj-dev \
    proj-data \
    proj-bin \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Training Script + Grid CSV ins Image
COPY train_and_log_mlflow.py /app/train_and_log_mlflow.py
COPY de_grid_sym_400.csv /app/de_grid_sym_400.csv

# optional
COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 8888

# Default: Jupyter (optional). Der Job überschreibt sowieso command/args.
CMD ["/start.sh"]

RUN chmod +x /app/train_and_log_mlflow.py