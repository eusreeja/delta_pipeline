#!/bin/bash

# Ensure Java 11 is available (matching local environment setup)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Start Jupyter Lab in background if JUPYTER_MODE is set
if [ "$JUPYTER_MODE" = "true" ]; then
    echo "Starting Jupyter Lab on port ${JUPYTER_PORT}..."
    jupyter lab \
        --ip=0.0.0.0 \
        --port=${JUPYTER_PORT} \
        --no-browser \
        --allow-root \
        --notebook-dir=/app/notebooks &
fi

# Run the main pipeline with demo job (matching README local setup)
if [ "$#" -eq 0 ]; then
    echo "Running default demo job..."
    exec python src/main.py --job demo --config config/pipeline_config.yaml
else
    echo "Running with custom arguments: $@"
    exec "$@"
fi

# Wait for all background processes to finish
wait  