# approach
The pipeline implements a medallion architecture with three distinct layers:
- **Bronze (Raw):** Exact copy of source data with minimal transformation
- **Silver (Refined):** Cleaned, deduplicated, and validated data
- **Gold (Curated):** Business-ready datasets optimized for analytics and ML

## BI/ML Use Cases

This pipeline provides three main Business Intelligence and Machine Learning use cases:

1. **Daily Production Trend Analysis** - Analyze electricity production trends by type in Germany
2. **Underperformance Prediction** - Predict underperformance using 30-minute interval data with lag features  
3. **Wind Power Price Analysis** - Analyze price vs production correlation for offshore/onshore wind



# assumptions and decision made
1. The landing zone is not created as it has only one source.
2. Surrogate Keys UUID() for the id columns ensures uniqueness across environments and avoids dependency on source-system keys.
3. Delta Merge Operations:Optimized for deduplication and incremental updates.
4. Local Testing: Limit data volume with ingestion parameters and proceesing maximum 1 day data ata time.
5. Scalability:Partition Gold tables by timestamp for query optimization.




# Setup and Test the Solution

## 1. Clone the Repository
```bash
git clone https://github.com/eusreeja/delta_pipeline
cd datapipeline-1
```

## 2. Install Java 11
Install OpenJDK 11 and verify the installation:

```bash
# Install Java 11 (using Homebrew on macOS)
brew install openjdk@11

# Verify installation
java --version
```

Expected output:
```
openjdk 11.0.27 2025-04-15
OpenJDK Runtime Environment Temurin-11.0.27+6 (build 11.0.27+6)
OpenJDK 64-Bit Server VM Temurin-11.0.27+6 (build 11.0.27+6, mixed mode)
```

## 3. Set up the Python Environment
```bash
brew install python@3.9 
python3.9 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 4. Spark Setup
Download and configure Apache Spark:

```bash
# Download Spark 3.2.0
wget https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz

# Extract to home directory
tar -xzf spark-3.2.0-bin-hadoop3.2.tgz -C ~/

# Set environment variables
export SPARK_HOME=~/spark-3.2.0-bin-hadoop3.2
export PATH=$SPARK_HOME/bin:$PATH

# Add to your shell profile (.bashrc, .zshrc, etc.)
echo 'export SPARK_HOME=~/spark-3.2.0-bin-hadoop3.2' >> ~/.zshrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.zshrc
```

## 5. Run the Demo Project

### Basic demo:
```bash
python src/main.py --job demo --config config/pipeline_config.yaml
```

### Advanced demo with Spark Submit:
```bash
spark-submit \
    --packages io.delta:delta-core_2.12:3.1.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    src/main.py --job demo --layer silver --config config/pipeline_config.yaml
```





### Quick Start with Docker
For the easiest way to run and demonstrate all BI/ML use cases:

```bash
# Start the environment
docker-compose up -d

# Run complete demo (data ingestion + transformations)
docker-compose exec datapipeline-app python src/main.py --job demo --config config/pipeline_config.yaml

# Generate BI/ML reports
docker-compose exec datapipeline-app python src/main.py --job report --config config/pipeline_config.yaml

# Access Jupyter Lab for interactive analysis
# Open: http://localhost:8888
```
or 

# Quick start 
docker-compose up -d
docker-compose exec datapipeline-app python src/main.py --job demo --config config/pipeline_config.yaml
docker-compose exec datapipeline-app python src/main.py --job report --config config/pipeline_config.yaml