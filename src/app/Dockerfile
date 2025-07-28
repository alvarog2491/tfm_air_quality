# Flask Application Container
FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy app-specific requirements and install dependencies
COPY src/app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/app/ src/app/
COPY src/common/ src/common/

# Set Python path
ENV PYTHONPATH=/app/src

# Create necessary directories for shared volumes
RUN mkdir -p /app/models \
    /app/data \
    /app/logs

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app
USER app

# Expose Flask port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Default command - run Flask app
CMD ["python", "src/app/main_app.py"]