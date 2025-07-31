#!/bin/bash

# Start Flask app with Gunicorn
echo "Starting Flask app with Gunicorn on port 9001..."
cd /app/src/app
gunicorn --bind 0.0.0.0:9001 --workers 4 --timeout 120 app:app