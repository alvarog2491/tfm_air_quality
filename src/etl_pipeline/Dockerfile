# ETL Pipeline Container
FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy ETL-specific requirements and install dependencies
COPY src/etl_pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/

# Set Python path for ETL pipeline
ENV PYTHONPATH=/app/src:/app/src/etl_pipeline

# Create necessary directories for shared volumes
RUN mkdir -p /app/data/raw \
    /app/data/processed \
    /app/data/output \
    /app/logs

# Set working directory
WORKDIR /app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command - run ETL pipeline
CMD ["python", "src/etl_pipeline/main_orchestrator.py"]