from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, to_timestamp, lit
from src.logger.custom_logger import Logger
from typing import Dict, Any
import os
from pathlib import Path
import yaml
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from src.connectors.energy_charts_api import EnergyChartsConnector
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType, DecimalType

class DataIngestionJob:
    def __init__(self, spark: SparkSession, logger: Logger, job_id, config: Dict[str, Any], endpoint: str, current_date: str, country:str):
        self.spark = spark
        self.logger = logger
        self.current_date = current_date
        self.endpoint = endpoint
        self.country = country
        self.start_date = current_date
        self.end_date = current_date
        self.config = config
        self.ingestion_batch_id = job_id
        
        # Construct the path (same as before)
        self.bronze_path = os.path.join(
            config['delta_lake']['storage']['bronze_path'], 
            country, 
            endpoint
        )
        self.bronze_table_name = 'bronze_'+ country + endpoint
        self.logger.info(f"Pipeline: Bronze path set to {self.bronze_path}")

        # Create directory if it doesn't exist (including parent dirs)
        Path(self.bronze_path).mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Pipeline: Bronze path set to {self.bronze_path}")
        self.bronze_schema = None
        if endpoint == "public_power":
            self.bronze_schema = StructType([
                StructField("record_id", StringType(), False),
                StructField("timestamp", TimestampType(), True),
                StructField("unix_seconds", LongType(), True),
                StructField("production_type", StringType(), True),
                StructField("electricity_generated", DoubleType(), True),
                StructField("country", StringType(), True),
                StructField("data_source", StringType(), True),
                StructField("source_system", StringType(), True),
                StructField("ingestion_batch_id", StringType(), True),
                StructField("ingestion_date", TimestampType(), True)
            ])
        elif endpoint == "price":
            self.bronze_schema = StructType([
                StructField("record_id", StringType(), False),
                StructField("timestamp", TimestampType(), True),
                StructField("unix_seconds", LongType(), True),
                StructField("electricity_price", DecimalType(10,4), True),
                StructField("electricity_unit", StringType(), True),
                StructField("country", StringType(), True),
                StructField("data_source", StringType(), True),
                StructField("source_system", StringType(), True),
                StructField("ingestion_batch_id", StringType(), True),
                StructField("ingestion_date", TimestampType(), True)
            ])
        elif endpoint == "installed_power":
            self.bronze_schema = StructType([
                StructField("record_id", StringType(), False),
                StructField("timestamp", TimestampType(), True),
                StructField("time_period", StringType(), True),
                StructField("production_type", StringType(), True),
                StructField("installed_capacity", DoubleType(), True),
                StructField("country", StringType(), True),
                StructField("data_source", StringType(), True),
                StructField("source_system", StringType(), True),
                StructField("ingestion_batch_id", StringType(), True),
                StructField("ingestion_date", TimestampType(), True)
            ])
        else:
            self.logger.error(f"Pipeline: Unknown endpoint: {endpoint}")
            raise ValueError(f"Pipeline: Unknown endpoint: {endpoint}")
        

    def _transform_public_power_data(self, api_data: Dict, timestamp_column:str = "unix_seconds") -> list[Dict[str, Any]]:
        """Transform the API response into our desired format"""
        self.logger.info("Pipeline: ..........Transforming data..................")
        formatted_data = []
        timestamps = []
        unix_timestamps = []
        
        try:
            if timestamp_column in api_data:
                # Store original Unix timestamps and convert to datetime objects
                self.logger.info(f"Pipeline: Converting timestamps from column: {timestamp_column}")
                unix_timestamps = api_data[timestamp_column]
                timestamps = [datetime.fromtimestamp(ts, tz=timezone.utc) for ts in unix_timestamps]
            
            # Process each production type
            for production_type in api_data["production_types"]:
                if not isinstance(production_type, dict) or "name" not in production_type or "data" not in production_type:
                    continue  # Skip malformed entries
                
                production_name = production_type["name"]
                values = production_type["data"]
                
                # Pair each timestamp with its corresponding value
                for ts, unix_ts, val in zip(timestamps, unix_timestamps, values):
                    formatted_data.append({
                        "record_id": str(uuid.uuid4()),
                        "timestamp": ts,
                        "unix_seconds": int(unix_ts),
                        "production_type": production_name,
                        "electricity_generated": float(val) if val is not None else None,
                        "country": self.country,
                        "data_source": "api",
                        "source_system": "energy_charts_api",
                        "ingestion_batch_id": self.ingestion_batch_id,
                        "ingestion_date": datetime.now(timezone.utc)
                    })
            
            if not formatted_data:
                self.logger.warning("Pipeline: No valid data points found after transformation")
            else:
                self.logger.info(f"Pipeline: Transformed {len(formatted_data)} data points")
            self.logger.info(f"Pipeline: Transformed data power data: {formatted_data[:2]}... (showing first 2 entries)")
            return formatted_data
        except Exception as e:
            self.logger.error(f"Pipeline: Error transforming public power data: {str(e)}")
            raise ValueError(f"Pipeline: Failed to transform public power data: {str(e)}")
    def _transform_price_data(self, api_data: Dict, timestamp_column: str = "unix_seconds") -> list[Dict[str, Any]]:
        """Transform the price API response into our desired format"""
        self.logger.info("Pipeline: ..........Transforming price data..................")
        formatted_data = []
        
        try:
            timestamps = []
            unix_timestamps = []
            
            if timestamp_column in api_data:
                # Store original Unix timestamps and convert to datetime objects
                self.logger.info(f"Pipeline: Converting timestamps from column: {timestamp_column}")
                unix_timestamps = api_data[timestamp_column]
                timestamps = [datetime.fromtimestamp(ts, tz=timezone.utc) for ts in unix_timestamps]
            
            # Process each price point
            for ts, unix_ts, price, unit in zip(timestamps, unix_timestamps, api_data["price"],api_data["unit"]):
                formatted_data.append({
                    "record_id": str(uuid.uuid4()),
                    "timestamp": ts,
                    "unix_seconds": int(unix_ts),
                    "electricity_price": Decimal(str(price)) if price is not None else None,
                    "electricity_unit": unit if unit else "EUR/MWh",  
                    "country": self.country,
                    "data_source": "api",
                    "source_system": "energy_charts_api",
                    "ingestion_batch_id": self.ingestion_batch_id,
                    "ingestion_date": datetime.now(timezone.utc)
                })
            
            if not formatted_data:
                self.logger.warning("Pipeline: No valid price data points found after transformation")
            else:
                self.logger.info(f"Pipeline: Transformed {len(formatted_data)} price data points")
            self.logger.info(f"Pipeline: Transformed price data: {formatted_data[:2]}... (showing first 2 entries)")
            return formatted_data
            
        except Exception as e:
            self.logger.error(f"Pipeline: Error transforming price data: {str(e)}")
            raise ValueError(f"Pipeline: Failed to transform price data: {str(e)}")
        
    def _transform_installed_power_data(self, api_data: Dict) -> list[Dict[str, Any]]:
        """Transform the installed power API response into our desired format"""
        self.logger.info("Pipeline: ..........Transforming installed power data..................")
        formatted_data = []
        
        try:
            # Validate required fields
            if not api_data.get("time") or not api_data.get("production_types"):
                self.logger.warning("Pipeline: Missing required fields in installed power data")
                return formatted_data
            
            # Convert month.year timestamps to datetime objects (first day of each month)
            try:
                timestamps = [datetime.strptime(ts, "%m.%Y") for ts in api_data["time"]]
                time_periods = api_data["time"]  # Keep original time period strings
            except ValueError as e:
                self.logger.error(f"Pipeline: Error parsing timestamps: {str(e)}")
                return formatted_data
            
            # Process each production type
            for production_type in api_data["production_types"]:
                if not isinstance(production_type, dict) or "name" not in production_type or "data" not in production_type:
                    self.logger.warning("Pipeline: Skipping malformed production type entry")
                    continue
                
                production_name = production_type["name"]
                values = production_type["data"]
                
                # Check array lengths match
                if len(timestamps) != len(values):
                    self.logger.warning(f"Pipeline: Mismatched array lengths for {production_name}")
                    continue
                
                # Create records for each timestamp-value pair
                for ts, time_period, val in zip(timestamps, time_periods, values):
                    formatted_data.append({
                        "record_id": str(uuid.uuid4()),
                        "timestamp": ts,
                        "time_period": time_period,
                        "production_type": production_name,
                        "installed_capacity": float(val) if val is not None else None,
                        "country": self.country,
                        "data_source": "api",
                        "source_system": "energy_charts_api",
                        "ingestion_batch_id": self.ingestion_batch_id,
                        "ingestion_date": datetime.now(timezone.utc)
                    })
            
            self.logger.info(f"Pipeline: Transformed {len(formatted_data)} installed power data points")
            if formatted_data:
                self.logger.info(f"Pipeline: Sample transformed data: {formatted_data[:2]}")
            
            return formatted_data
            
        except Exception as e:
            self.logger.error(f"Pipeline: Error transforming installed power data: {str(e)}")
            raise ValueError(f"Pipeline: Failed to transform installed power data: {str(e)}")
        
    def _validate_api_response(self, api_data: Dict) -> bool:
            try:
                """Validate the API response structure"""
                self.logger.info("Pipeline: Validating API response structure")
                # Check if the API data is empty
                if api_data is None:
                    self.logger.warning("Pipeline: Received None from API")
                    return []
                # Check if the API data is empty
                if isinstance(api_data, list) and not api_data:
                    self.logger.warning("Pipeline: Received empty list from API")
                    return []
                if not api_data:
                    self.logger.warning("Pipeline: Received empty API data")
                    return []
                # Check if the API data is a dictionary
                if not isinstance(api_data, dict):
                    self.logger.error("Pipeline: API response is not a dictionary")
                    raise ValueError("Pipeline: API response must be a dictionary")
                
                self.logger.info(f"Pipeline: Received API data for {self.country} from {self.start_date} to {self.end_date} .")
                self.logger.info(f"Pipeline: API Response keys: {list(api_data.keys()) if isinstance(api_data, dict) else 'Invalid response type'}")
                 
                # Validate API response structure
                self.logger.info("Pipeline: Validating API response structure") 
                if self.endpoint == "public_power" and not api_data.get("production_types") and not api_data.get("unix_seconds"):
                    self.logger.warning("Pipeline: No production types or timestamps found in public_power API data")
                    return []
                elif self.endpoint == "price" and not api_data.get("price") and not api_data.get("unix_seconds"):
                    self.logger.warning("Pipeline: No price data or timestamps found in price API data")
                    return []
                elif self.endpoint == "installed_power" and not api_data.get("production_types") and not api_data.get("time"):
                    self.logger.warning("Pipeline: No installed power data or timestamps found in installed_power API data")
                    return []
                self.logger.info("Pipeline: API response structure validated successfully")

                return True

            except Exception as e:
                self.logger.error(f"Pipeline: Failed to validate API response: {str(e)}")
                raise ValueError(f"Pipeline: Failed to validate API response: {str(e)}")    

    def _transform_api_data(self, api_data: Dict[str, Any]) -> DataFrame:
        """Ingest and transform public power data from the API"""
        self.logger.info("Pipeline val: Transforming API data")
        formatted_data = []
        try:
            # Transform the API data into the desired format
            self.logger.info("Pipeline val: Transforming public power data")
            if self.endpoint == "public_power":
                formatted_data = self._transform_public_power_data(api_data, timestamp_column="unix_seconds")
            elif self.endpoint == "price":
                formatted_data = self._transform_price_data(api_data, timestamp_column="unix_seconds")
            elif self.endpoint == "installed_power":
                formatted_data = self._transform_installed_power_data(api_data)
            else:
                self.logger.error(f"Pipeline val: Unknown endpoint: {self.endpoint}")
                raise ValueError(f"Pipeline val: Unknown endpoint: {self.endpoint}")
            

            self.logger.info(f"Pipeline val: Creating DataFrame for {self.endpoint} data")
            df = self.spark.createDataFrame(formatted_data, schema=self.bronze_schema)
            self.logger.info(f"Pipeline val: Created DataFrame with {df.count()} rows")

            return df
            
        except Exception as e:
            #self.logger.info(f"Raw API Data: {api_data}")
            self.logger.error(f"Pipeline: Failed to ingest  data: {str(e)}")
            raise Exception(f"Pipeline: Failed to ingest data: {str(e)}")
        
    def _write_bronze_table(self, df, table_name, path, mode="append"):
        """Write DataFrame as Parquet table (fallback when Delta is not available)"""
        try:
            # Try Delta first
            (df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .option("delta.enableChangeDataFeed", "true")
            .option("mergeSchema", "true")
            .save(path))
            self.logger.info(f"Pipeline: Data written successfully to Delta format at {path}")
        except Exception as e:
            if "delta" in str(e).lower() or "DATA_SOURCE_NOT_FOUND" in str(e):
                self.logger.warning(f"Pipeline: Delta format not available, falling back to Parquet: {str(e)}")
                # Fallback to Parquet
                (df.write
                .format("parquet")
                .mode(mode)
                .option("compression", "snappy")
                .save(path))
                self.logger.info(f"Pipeline: Data written successfully to Parquet format at {path}")
            else:
                raise e
    
    def run(self, api_data: Dict[str, Any]) -> None:
        """Run the ingestion job with proper error handling"""
        dfs = []
        self.logger.info("Pipeline - Run:  ...................Starting data ingestion pipeline....................")
        try:
            # 1. Validate API data
            self.logger.info("Pipeline - Run: Validating API response")
            if  not self._validate_api_response(api_data):
                self.logger.error("Pipeline - Run: API response validation failed, no data to process")
                return None
            self.logger.info("Pipeline - Run: API response validation successful")

            # 2. Transform API data
            self.logger.info("Pipeline - Run: Transforming API data")
            dfs = self._transform_api_data(api_data)
            self.logger.info(f"Pipeline - Run: DataFrame created with {dfs.count()} rows")

            # 3. Write to bronze table (Delta or Parquet)
            self.logger.info("Pipeline - Run: Writing DataFrame to bronze table in path " + self.bronze_path)
            self._write_bronze_table(dfs, self.bronze_table_name , self.bronze_path, mode="append")

            # 4. Verify write by reading back the data
            self.logger.info("Pipeline - Run: Verifying data write...")
            try:
                # Try to read as Delta first
                verification_df = self.spark.read.format("delta").load(self.bronze_path)
                self.logger.info("Pipeline - Run: Data verification successful (Delta format):")
                #verification_df.show(5)
            except:
                # Fallback to Parquet
                verification_df = self.spark.read.format("parquet").load(self.bronze_path)
                self.logger.info("Pipeline - Run: Data verification successful (Parquet format):")
                #verification_df.show(5)
            
            self.logger.info("Pipeline - Run: Data ingestion completed successfully")
            
            # Note: Table registration skipped for Parquet format
            # Delta table registration would be done here if Delta format is available
            
            # 5. Future: Transform data for Silver layer
            self.logger.info("Pipeline - Run: Bronze layer ingestion completed successfully")
            # Silver layer transformation would be implemented here
                
        except Exception as e:
            self.logger.error(f"Pipeline - Run - Error: {str(e)}")
            raise
    def run_all_transformations(self, country: str = "de") -> None:
        """Run all Bronze to Silver transformations"""
        
        self.logger.info("Starting all Bronze to Silver transformations")
        
        try:
            # Initialize schemas
            self._create_silver_schemas()
            
            # Transform each data type
            self.ingest_public_power_to_silver(country)
            self.ingest_price_to_silver(country)
            self.ingest_installed_power_to_silver(country)
            
            self.logger.info("All Bronze to Silver transformations completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in Bronze to Silver transformations: {str(e)}")
            raise
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

# Convenience functions for individual job execution
def run_public_power_ingestion(spark: SparkSession, logger: Logger, config: Dict[str, Any], country: str = "de"):
    IngestionJob = DataIngestionJob(spark, logger, config, endpoint = config['energy_charts_api']['endpoints']['public_power'])
    IngestionJob.ingest_public_power_to_bronze(country)

def run_price_bronze_ingestion(spark: SparkSession, logger: Logger, config: Dict[str, Any], country: str = "de"):
    IngestionJob = DataIngestionJob(spark, logger, config)
    IngestionJob.ingest_price_to_silver(country)

def run_installed_power_ingestion(spark: SparkSession, logger: Logger, config: Dict[str, Any], country: str = "de"):
    IngestionJob = DataIngestionJob(spark, logger, config)
    IngestionJob.ingest_installed_power_to_silver(country)

def run_all_data_ingestion(spark: SparkSession, logger: Logger, config: Dict[str, Any], country: str = "de"):
    IngestionJob = BronzeToSilverTransformation(spark, logger, config)
    IngestionJob.run_all_IngestionJob(country)