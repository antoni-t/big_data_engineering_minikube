FROM python:3.11-slim

WORKDIR /app

# System libs (für geopandas/shapely/matplotlib oft nötig)
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

# Optional: Notebook(s) / Scripts mit ins Image
COPY start.sh /start.sh
RUN chmod +x /start.sh

# Wenn du ein Template-Notebook mitgeben willst:
# COPY ml-notebook.ipynb /app/ml-notebook.ipynb

EXPOSE 8888

CMD ["/start.sh"]
