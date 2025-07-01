FROM bde2020/spark-python-template:3.3.0-hadoop3.3

# Remove wordcount.py reference - not needed for this energy data pipeline
# COPY wordcount.py /app/

# Set correct application environment variables aligned with local setup
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/src/main.py
ENV SPARK_APPLICATION_ARGS "--job demo --config /app/config/pipeline_config.yaml"

# Configure Delta Lake and Spark versions to match local environment setup from README
ENV PYSPARK_VERSION=3.2.0 \
    DELTA_VERSION=2.0.0 \
    PYTHON_VERSION=3.9 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin \
    JUPYTER_PORT=8888 \
    PYTHONPATH=/app/src:/app:$PYTHONPATH \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Configure PySpark to use Delta Lake matching local spark-submit configuration
ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:2.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"

# Install system dependencies including Java 11 to match local environment
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    wget \
    build-essential \
    gcc \
    g++ \
    make \
    cmake \
    libfreetype6-dev \
    libpng-dev \
    pkg-config \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python development packages
RUN pip install --upgrade pip setuptools wheel && \
    pip install jupyterlab ipykernel findspark

WORKDIR /app

# Copy and install Python requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt --verbose

# Copy project files
COPY . .

# Create directory structure matching config/pipeline_config.yaml and local environment
RUN mkdir -p /app/delta_lake/bronze/energy_charts/de/public_power && \
    mkdir -p /app/delta_lake/bronze/energy_charts/de/price && \
    mkdir -p /app/delta_lake/bronze/energy_charts/de/installed_power && \
    mkdir -p /app/delta_lake/silver/public_power && \
    mkdir -p /app/delta_lake/silver/price && \
    mkdir -p /app/delta_lake/silver/installed_power && \
    mkdir -p /app/delta_lake/gold/dim_production_type && \
    mkdir -p /app/delta_lake/gold/gold_analysis_of_daily_price && \
    mkdir -p /app/delta_lake/gold/prediction_of_underperformance && \
    mkdir -p /app/logs && \
    mkdir -p /app/artifacts && \
    mkdir -p /app/notebooks && \
    chmod -R 777 /app/delta_lake /app/logs /app/artifacts /app/notebooks



# -----------------------------------------------------------------------------
# Expose Jupyter port
# -----------------------------------------------------------------------------
EXPOSE ${JUPYTER_PORT}

# -----------------------------------------------------------------------------
# Entrypoint and startup scripts
# -----------------------------------------------------------------------------
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]



# -----------------------------------------------------------------------------
# Example: To run different pipeline jobs matching local setup examples
# -----------------------------------------------------------------------------
# Basic demo (default):
# CMD ["python", "src/main.py", "--job", "demo", "--config", "config/pipeline_config.yaml"]

# Specific ingestion jobs:
# CMD ["python", "src/main.py", "--job", "installed_power", "--config", "config/pipeline_config.yaml"]
# CMD ["python", "src/main.py", "--job", "public_power", "--config", "config/pipeline_config.yaml"]
# CMD ["python", "src/main.py", "--job", "price", "--config", "config/pipeline_config.yaml"]

# Bronze to Silver transformations:
# CMD ["python", "src/main.py", "--job", "bronze_to_silver_all", "--config", "config/pipeline_config.yaml"]

# Silver to Gold transformations:
# CMD ["python", "src/main.py", "--job", "silver_to_gold_all", "--config", "config/pipeline_config.yaml"]
