FROM python:3.11

WORKDIR /app

# system packages (da wir pandas & req-cache benötigen)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scraper scripts
COPY autobahn_scraper.py .
COPY wetter_scraper.py .

# Copy CSV
COPY de_grid_cell_centers.csv .

# Environment defaults
ENV SCRAPER_DATA_DIR="/data"
ENV PYTHONUNBUFFERED=1

# Default CMD (wird NICHT genutzt, CronJobs überschreiben das!)
CMD ["bash", "-c", "echo 'Scraper container ready.' && sleep infinity"]
