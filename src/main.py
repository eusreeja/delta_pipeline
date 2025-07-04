import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
from delta import *
from src.logger.custom_logger import Logger
import pyspark.sql.functions as F
from datetime import datetime
import argparse
import yaml
import uuid
from src.connectors.energy_charts_api import EnergyChartsConnector
from src.jobs.ingest_public_power_job import ingest_public_power_job
from src.jobs.ingest_price_job import ingest_price_job
from src.jobs.ingest_installed_power_job import ingest_installed_power_job
from src.jobs.bronze_to_silver_job import (
    run_public_power_bronze_to_silver,
    run_price_bronze_to_silver,
    run_installed_power_bronze_to_silver,
    run_all_bronze_to_silver
)
from src.jobs.silver_to_gold_job import (
    run_dim_production_type,
    run_fact_power,
    run_fact_power_30min_agg,
    run_all_silver_to_gold
)

def init_spark():
    """Initialize Spark with basic configuration"""
    return (SparkSession.builder.master("local[*]")
    .appName("DataPipeline")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.sql.warehouse.dir", "./delta_lake")  # Local directory for Delta Lake
    .config("spark.driver.memory", "4g")  # Increase to 4GB
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.sql.shuffle.partitions", "200")  # Reduce shuffle partitions
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
    .config("spark.executor.memory", "4g")  # If using executors
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate())
def init_energy_charts_connector(endpoint, logger, config):
    """Initialize EnergyChartsConnector with endpoint, logger, and config"""
    # Restructure config for the connector
    connector_config = {
        'energy_charts_api': {
            'base_url': config['energy_charts_api']['base_url'],
            'request_timeout': config['energy_charts_api']['request_timeout']
        }
    }
    return EnergyChartsConnector(endpoint, logger, connector_config) 

def load_config(config_path: str) -> dict:
    print(f"Loading configuration from {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    try:
        
        spark = init_spark()
        #spark.conf.set("spark.sql.warehouse.dir", "/app/delta_lake")
        # Generate a unique batch ID for this ingestion run
        job_id = str(uuid.uuid4())
        logger = Logger("Job",job_id).get_logger()
        logger.info("main: ...................................................................................")
        logger.info(f"main: Logging system initialized successfully" )
    
        # Load config
        parser = argparse.ArgumentParser(description="Run Ingestion and Transformation Jobs")
        parser.add_argument("--job", type=str, required=True, help="Job to run: public_power | price | installed_power | bronze_to_silver | bronze_to_silver_all")
        parser.add_argument("--layer", type=str, required=False, help="Pipeline layer: bronze | silver | gold", default="silver")
        parser.add_argument("--interval", type=str, required=False, help="Job to run interval in minutes : 15 | 30 | 60 | 'monthly' | 'yearly' ")
        parser.add_argument("--country", type=str, required=False, help="Job to run country : 'de' | 'AT ")
        parser.add_argument("--bulk_load_flag", type=str, required=False, help="Job to run Historical load : 0 | False | 1 | True ")
        parser.add_argument("--start_date", type=str, required=False, help="Job to run start date ")
        parser.add_argument("--end_date", type=str, required=False, help="Job to run end date ")
        parser.add_argument("--config", help="Path to pipeline config file", default="config/pipeline_config.yaml")
        args = parser.parse_args()
        logger.info(f"main: Arguments parsed: {args}")

        # Load config
        config = load_config(args.config)
        logger.info(f"main: Configuration loaded: {config}")
        
        # Determine parameters - use args if provided, otherwise config defaults
        bulk_load_flag = args.bulk_load_flag.lower() == 'true' if args.bulk_load_flag is not None else True
        start_date = args.start_date if args.start_date else config['energy_charts_api']['default_params']['start_date']
        end_date = args.end_date if args.end_date else config['energy_charts_api']['default_params']['end_date']
        country = args.country if args.country else config['energy_charts_api']['default_params']['country']
        interval = args.interval if args.interval else config['energy_charts_api']['schedule']['public_power_interval_minutes']


        logger.info(f"main: job params start date : {config['energy_charts_api']['default_params']['start_date']} , end date {config['energy_charts_api']['default_params']['end_date']} ...")
        
        # Bronze Layer Jobs (Data Ingestion)
        if args.job == "public_power":
            # Initialize EnergyChartsConnector
            energy_charts_connector = init_energy_charts_connector(config['energy_charts_api']['endpoints']['public_power'], logger, config)

            (ingest_public_power_job(spark 
                                , logger
                                , job_id 
                                , energy_charts_connector 
                                , config
                                , endpoint = config['energy_charts_api']['endpoints']['public_power'] 
                                , bulk_load_flag= bulk_load_flag 
                                , start_date = start_date 
                                , end_date = end_date
                                , country = country 
                                , interval = interval 
                               ))
            logger.info("main: Public power data ingestion completed successfully")
            
            # Automatically run Bronze to Silver transformation after ingestion
            if args.layer in ["silver", "all"]:
                logger.info("main: Starting Bronze to Silver transformation for public power")
                run_public_power_bronze_to_silver(spark, logger, job_id, config, country)
                logger.info("main: Public power Bronze to Silver transformation completed")
            
        elif args.job == "price":
             # Initialize EnergyChartsConnector
            energy_charts_connector = init_energy_charts_connector(config['energy_charts_api']['endpoints']['price'], logger, config)

            ingest_price_job(spark \
                                , logger\
                                , job_id \
                                , energy_charts_connector \
                                , config\
                                , endpoint = config['energy_charts_api']['endpoints']['price'] \
                                , bulk_load_flag= bulk_load_flag \
                                , start_date = start_date \
                                , end_date = end_date\
                                , country = country \
                                , interval = interval \
                               )
            logger.info("main: Price data ingestion completed successfully")
            
            # Automatically run Bronze to Silver transformation after ingestion
            if args.layer in ["silver", "all"]:
                logger.info("main: Starting Bronze to Silver transformation for price")
                run_price_bronze_to_silver(spark, logger, job_id, config, country)
                logger.info("main: Price Bronze to Silver transformation completed")
        
        
        elif args.job == "installed_power":
             # Initialize EnergyChartsConnector
            energy_charts_connector = init_energy_charts_connector(config['energy_charts_api']['endpoints']['installed_power'], logger, config)

            ingest_installed_power_job(spark \
                                , logger\
                                , job_id \
                                , energy_charts_connector \
                                , config\
                                , endpoint = config['energy_charts_api']['endpoints']['installed_power'] \
                                , bulk_load_flag= bulk_load_flag \
                                , start_date = start_date \
                                , end_date = end_date\
                                , country = country \
                                , interval = interval \
                               )
            logger.info("main: Installed power data ingestion completed successfully")
            
            # Automatically run Bronze to Silver transformation after ingestion
            if args.layer in ["silver", "all"]:
                logger.info("main: Starting Bronze to Silver transformation for installed power")
                run_installed_power_bronze_to_silver(spark, logger, job_id, config, country)
                logger.info("main: Installed power Bronze to Silver transformation completed")
                
        # Silver Layer Jobs (Bronze to Silver Transformation Only)
        elif args.job == "bronze_to_silver_public_power":
            logger.info("main: Running Bronze to Silver transformation for public power only")
            run_public_power_bronze_to_silver(spark, logger, job_id, config, country)
            logger.info("main: Public power Bronze to Silver transformation completed")
            
        elif args.job == "bronze_to_silver_price":
            logger.info("main: Running Bronze to Silver transformation for price only")
            run_price_bronze_to_silver(spark, logger, job_id, config, country)
            logger.info("main: Price Bronze to Silver transformation completed")
            
        elif args.job == "bronze_to_silver_installed_power":
            logger.info("main: Running Bronze to Silver transformation for installed power only")
            run_installed_power_bronze_to_silver(spark, logger, job_id, config, country)
            logger.info("main: Installed power Bronze to Silver transformation completed")
            
        elif args.job == "bronze_to_silver_all":
            logger.info("main: Running all Bronze to Silver transformations")
            run_all_bronze_to_silver(spark, logger, job_id, config, country)
            logger.info("main: All Bronze to Silver transformations completed")
        
        elif args.job == "gold_dim_production_type":
            logger.info("main: Running all Silver to Gold transformations")
            run_dim_production_type(spark, logger, job_id, config)
            logger.info("main: gold_dim_production_type job  load completed")
        
        elif args.job == "gold_fact_power":
            logger.info("main: Running all Silver to Gold transformations")
            run_fact_power(spark, logger, job_id, config)
            logger.info("main: gold_fact_power job completed")
        
        elif args.job == "gold_fact_power_30min_agg":
            logger.info("main: Running all Silver to Gold transformations")
            run_fact_power_30min_agg(spark, logger, job_id, config)
            logger.info("main: gold_fact_power_30min_agg job  completed")

        elif args.job == "silver_to_gold_all":
            logger.info("main: Running all Silver to Gold transformations")
            run_all_silver_to_gold(spark, logger, job_id, config)
            logger.info("main: All Silver to Gold transformations completed")
            
        elif args.job == "demo":
            # Initialize EnergyChartsConnector
            energy_charts_connector = init_energy_charts_connector(config['energy_charts_api']['endpoints']['public_power'], logger, config)

            ingest_public_power_job(spark \
                                , logger\
                                , job_id \
                                , energy_charts_connector \
                                , config\
                                , endpoint = config['energy_charts_api']['endpoints']['public_power'] \
                                , bulk_load_flag= bulk_load_flag \
                                , start_date = start_date \
                                , end_date = end_date\
                                , country = country \
                                , interval = interval \
                               )
            logger.info("main: Public power data ingestion completed successfully")
             # Initialize EnergyChartsConnector
            energy_charts_connector = init_energy_charts_connector(config['energy_charts_api']['endpoints']['price'], logger, config)
            ingest_price_job(spark \
                                , logger\
                                , job_id \
                                , energy_charts_connector \
                                , config\
                                , endpoint = config['energy_charts_api']['endpoints']['price'] \
                                , bulk_load_flag= bulk_load_flag \
                                , start_date = start_date \
                                , end_date = end_date\
                                , country = country \
                                , interval = interval \
                               )
            logger.info("main: Price data ingestion completed successfully")
            # Initialize EnergyChartsConnector
            energy_charts_connector = init_energy_charts_connector(config['energy_charts_api']['endpoints']['installed_power'], logger, config)
            ingest_installed_power_job(spark \
                                , logger\
                                , job_id \
                                , energy_charts_connector \
                                , config\
                                , endpoint = config['energy_charts_api']['endpoints']['installed_power'] \
                                , bulk_load_flag= bulk_load_flag \
                                , start_date = start_date \
                                , end_date = end_date\
                                , country = country \
                                , interval = interval \
                               )
            logger.info("main: Installed power data ingestion completed successfully")
            logger.info("main: Running all Bronze to Silver transformations")
            run_all_bronze_to_silver(spark, logger, job_id, config, country)
            logger.info("main: All Bronze to Silver transformations completed")

            logger.info("main: Running all Silver to Gold transformations")
            run_all_silver_to_gold(spark, logger, job_id, config)
            logger.info("main: All Silver to Gold transformations completed")

            
        else:
            logger.error(f"main: Unknown job: {args.job}")
            logger.error("main: Available jobs: public_power, price, installed_power, "
                        "bronze_to_silver_public_power, bronze_to_silver_price, bronze_to_silver_installed_power, bronze_to_silver_all, "
                        "gold_dim_production_type, gold_fact_power, gold_fact_power_30min_agg, silver_to_gold_all, "
                        "demo")
            sys.exit(1) 
    except Exception as e:
        print(f"main: An error occurred: {str(e)}")
        raise e
    finally:
        spark.stop()
        print("main: Spark session stopped")

if __name__ == "__main__":
    
    main()