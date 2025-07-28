# Modeling Pipeline Container
FROM python:3.13-slim

WORKDIR /app

# Install system dependencies including git for DVC
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy modeling-specific requirements and install dependencies
COPY src/modeling/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and configuration
COPY src/ src/
COPY params.yaml .
COPY dvc.yaml .

# Set Python path
ENV PYTHONPATH=/app/src

# Create necessary directories for shared volumes
RUN mkdir -p /app/data \
    /app/models \
    /app/metrics \
    /app/logs

# Initialize DVC (without git repo)
RUN dvc init --no-scm || true

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sklearn, pandas, joblib; print('Modeling dependencies OK')"

# Default command - run DVC pipeline
CMD ["dvc", "repro"]