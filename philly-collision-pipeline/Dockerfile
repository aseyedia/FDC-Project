FROM apache/airflow:2.7.3-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies needed for geopandas
RUN apt-get update && apt-get install -y \
    build-essential \
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    gdal-bin \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# Create necessary directories
# Application code will be mounted as volumes in docker-compose.yml
RUN mkdir -p /app/scripts /app/utils /app/data/raw /app/data/processed /app/data/final /app/metadata /app/logs /app/dags

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command (will be overridden by docker-compose)
CMD ["airflow", "standalone"]
