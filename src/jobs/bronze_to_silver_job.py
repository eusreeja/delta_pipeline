import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, minute, 
    when, isnan, isnull, regexp_replace, trim, upper,
    current_timestamp, lit, coalesce, avg, stddev,
    row_number, desc, asc, expr, sha2, concat_ws, unix_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, BooleanType
from delta.tables import DeltaTable
from src.logger.custom_logger import Logger
from typing import Dict, Any
from pathlib import Path
from datetime import datetime, timezone


class BronzeToSilverTransformation:
    """
    Handles Bronze to Silver layer transformations for all energy data types.
    Applies data quality checks, cleansing, and standardization.
    """
    
    def __init__(self, spark: SparkSession, logger: Logger, job_id:str, config: Dict[str, Any]):
        self.spark = spark
        self.logger = logger
        self.job_id = job_id
        self.config = config
        self.silver_path = config['delta_lake']['storage']['silver_path']
        
        # Configure Spark optimizations
        self._configure_spark_optimizations()
        
    def _create_silver_schemas(self):
        """Define schemas for Silver layer tables"""
        
        # Silver Public Power Schema
        self.silver_public_power_schema = StructType([
            StructField("record_id", StringType(), False),
            StructField("business_key", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("unix_seconds", IntegerType(), False),
            StructField("production_type", StringType(), False),
            StructField("electricity_generated", DoubleType(), True),
            StructField("country", StringType(), True),
            StructField("data_source", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("dq_check_status", StringType(), False),
            StructField("dq_check_date", StringType(), False),
            StructField("dq_score", StringType(), False),
            StructField("is_anomaly", BooleanType(), False),
            StructField("bronze_record_id", StringType(), False),
            StructField("silver_batch_id", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("data_quality_score", DoubleType(), True),
            StructField("created_by", DoubleType(), True),
        ])
        
        # Silver Price Schema
        self.silver_price_schema = StructType([
            StructField("record_id", StringType(), False),
            StructField("business_key", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("unix_seconds", IntegerType(), False),
            StructField("electricity_price", DoubleType(), True),
            StructField("electricity_unit", StringType(), True),
            StructField("country", StringType(), True),
            StructField("data_source", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("dq_check_status", StringType(), False),
            StructField("dq_check_date", StringType(), False),
            StructField("dq_score", StringType(), False),
            StructField("is_anomaly", BooleanType(), False),
            StructField("bronze_record_id", StringType(), False),
            StructField("silver_batch_id", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("data_quality_score", DoubleType(), True),
            StructField("created_by", DoubleType(), True)
        ])
        
        # Silver Installed Power Schema
        self.silver_installed_power_schema = StructType([
            StructField("record_id", StringType(), False),
            StructField("business_key", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("time_period", StringType(), False),
            StructField("production_type", StringType(), False),
            StructField("installed_capacity", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("country", StringType(), True),
            StructField("data_source", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("dq_check_status", StringType(), False),
            StructField("dq_check_date", StringType(), False),
            StructField("dq_score", StringType(), False),
            StructField("is_anomaly", BooleanType(), False),
            StructField("bronze_record_id", StringType(), False),
            StructField("silver_batch_id", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("data_quality_score", DoubleType(), True),
            StructField("created_by", DoubleType(), True)
        ])

    def _apply_data_quality_checks(self, df: DataFrame, data_type: str) -> DataFrame:
        """Apply comprehensive data quality checks and scoring"""
        
        self.logger.info(f"Applying data quality checks for {data_type}")
        
        if data_type == "public_power":
            # Public Power specific quality checks
            df_with_quality = df.withColumn(
                "data_quality_score",
                when(col("electricity_generated").isNull(), 0.0)
                .when(col("electricity_generated") < 0, 0.3)  # Negative values are suspicious
                .when(col("electricity_generated") > 100000, 0.5)  # Very high values might be outliers
                .otherwise(1.0)
            ).withColumn(
                "is_valid",
                when(col("timestamp").isNull(), False)
                .when(col("production_type").isNull(), False)
                .when(col("electricity_generated").isNull(), False)
                .when(col("electricity_generated") < 0, False)
                .otherwise(True)
            )
            
        elif data_type == "price":
            # Price specific quality checks
            df_with_quality = df.withColumn(
                "data_quality_score",
                when(col("electricity_price").isNull(), 0.0)
                .when(col("electricity_price") < 0, 0.1)  # Negative prices can occur but are unusual
                .when(col("electricity_price") > 1000, 0.5)  # Very high prices might be outliers
                .otherwise(1.0)
            ).withColumn(
                "is_valid",
                when(col("timestamp").isNull(), False)
                .when(col("electricity_price").isNull(), False)
                .otherwise(True)
            )
        elif data_type == "installed_power":
            # Installed Power specific quality checks
            df_with_quality = df.withColumn(
                "data_quality_score",
                when(col("installed_capacity").isNull(), 0.0)
                .when(col("installed_capacity") < 0, 0.0)  # Cannot have negative capacity
                .when(col("installed_capacity") > 1000000, 0.5)  # Very high capacity might be outliers
                .otherwise(1.0)
            ).withColumn(
                "is_valid",
                when(col("timestamp").isNull(), False)
                .when(col("installed_capacity").isNull(), False)
                .when(col("installed_capacity") < 0, False)
                .otherwise(True)
            )
        else:
            # Default quality checks for unknown data types
            df_with_quality = df.withColumn("data_quality_score", lit(1.0)).withColumn("is_valid", lit(True))
            
        return df_with_quality

    def _detect_anomalies(self, df: DataFrame, value_column: str) -> DataFrame:
        """Detect statistical anomalies using Z-score method"""
        
        self.logger.info(f"Detecting anomalies in {value_column}")
        
        # Calculate statistics for anomaly detection
        stats = df.select(
            avg(col(value_column)).alias("mean_value"),
            stddev(col(value_column)).alias("stddev_value")
        ).collect()[0]
        
        mean_val = float(stats["mean_value"] or 0.0)
        stddev_val = float(stats["stddev_value"] or 0.0)
        
        # Mark anomalies (values > 3 standard deviations from mean)
        if stddev_val > 0:
            df_with_anomalies = df.withColumn(
                "is_anomaly",
                when(
                    (col(value_column) > (mean_val + 3 * stddev_val)) |
                    (col(value_column) < (mean_val - 3 * stddev_val)),
                    True
                ).otherwise(False)
            )
        else:
            df_with_anomalies = df.withColumn("is_anomaly", lit(False))
            
        return df_with_anomalies

    def _standardize_production_types(self, df: DataFrame) -> DataFrame:
        """Standardize production type names"""
        
        self.logger.info("Standardizing production type names")
        
        # Define production type mapping for standardization
        production_type_mapping = {
            "wind": "Wind",
            "solar": "Solar", 
            "nuclear": "Nuclear",
            "fossil": "Fossil",
            "hydro": "Hydro",
            "biomass": "Biomass",
            "geothermal": "Geothermal",
            "wind_offshore": "Wind Offshore",
            "wind_onshore": "Wind Onshore",
            "pv": "Solar PV",
            "run_of_river": "Run of River",
            "pumped_storage": "Pumped Storage"
        }
        
        # Apply standardization
        df_standardized = df
        for old_name, new_name in production_type_mapping.items():
            df_standardized = df_standardized.withColumn(
                "production_type",
                when(upper(trim(col("production_type"))).contains(old_name.upper()), new_name)
                .otherwise(col("production_type"))
            )
            
        return df_standardized

    def _deduplicate_records(self, df: DataFrame, partition_columns: list, order_column: str = "ingestion_timestamp") -> DataFrame:
        """Remove duplicate records keeping the latest one"""
        
        self.logger.info("Removing duplicate records")
        
        window_spec = Window.partitionBy(*partition_columns).orderBy(desc(order_column))
        
        df_deduped = df.withColumn(
            "row_number", row_number().over(window_spec)
        ).filter(col("row_number") == 1).drop("row_number")
        
        return df_deduped

    def transform_public_power_to_silver(self, country: str = "de") -> None:
        """Transform public power data from Bronze to Silver"""
        
        self.logger.info("Starting public power Bronze to Silver transformation")
        
        try:
            # Read from Bronze layer
            bronze_path = f"{self.config['delta_lake']['tables']['bronze_public_power']}"
            silver_path = f"{self.config['delta_lake']['tables']['silver_public_power']}"
            
            self.logger.info(f"Reading from Bronze path: {bronze_path}")
            bronze_df = self.spark.read.format("delta").load(bronze_path)
            
            if bronze_df.count() == 0:
                self.logger.warning("No data found in Bronze public_power table")
                return
            
            # Apply transformations using the proper schema
            silver_df = bronze_df.select(
                # Generate record_id using UUID
                expr("uuid()").alias("record_id"),
                # Generate business_key from timestamp, production_type, country
                sha2(concat_ws("_", col("timestamp"), col("production_type"), lit(country)), 256).alias("business_key"),
                col("timestamp"),
                unix_timestamp(col("timestamp")).alias("unix_seconds"),
                col("production_type"),
                col("electricity_generated"),
                lit(country).alias("country"),
                lit("energy_charts_api").alias("data_source"),
                lit("energy_charts_api").alias("source_system"),
                lit("PASSED").alias("dq_check_status"),  # Will be updated by data quality checks
                current_timestamp().cast("string").alias("dq_check_date"),
                lit("100").alias("dq_score"),  # Will be updated by data quality checks
                lit(False).alias("is_anomaly"),  # Will be updated by anomaly detection
                col("record_id").alias("bronze_record_id"),  # Reference to bronze record
                lit(self.job_id).alias("silver_batch_id"),
                col("ingestion_date").alias("ingestion_timestamp"),
                current_timestamp().alias("processing_timestamp"),
                lit(1.0).alias("data_quality_score"),  # Will be updated by data quality checks
                lit(1.0).alias("created_by")  # Using 1.0 as default, should be user ID in production
            )
            
            # Standardize production types
            silver_df = self._standardize_production_types(silver_df)
            
            # Apply data quality checks
            silver_df = self._apply_data_quality_checks(silver_df, "public_power")
            
            # Update DQ fields based on quality checks
            silver_df = silver_df.withColumn(
                "dq_check_status", 
                when(col("is_valid") == True, "PASSED").otherwise("FAILED")
            ).withColumn(
                "dq_score",
                (col("data_quality_score") * 100).cast("string")
            )
            
            # Detect anomalies
            silver_df = self._detect_anomalies(silver_df, "electricity_generated")
            
            # Remove duplicates before merge
            partition_cols = ["timestamp", "production_type", "country"]
            silver_df = self._deduplicate_records(silver_df, partition_cols)
            
            # Create Silver directory if it doesn't exist
            Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Perform optimized MERGE operation
            self.logger.info("Performing optimized MERGE operation")
            zorder_columns = ["business_key", "timestamp", "production_type", "country"]
            self._merge_with_optimizations(silver_df, silver_path, "business_key", zorder_columns)
            
            # Log transformation stats
            total_records = silver_df.count()
            valid_records = silver_df.filter(col("is_valid") == True).count()
            invalid_records = total_records - valid_records
            
            self.logger.info(f"Public Power transformation completed:")
            self.logger.info(f"  Total records processed: {total_records}")
            self.logger.info(f"  Valid records: {valid_records}")
            self.logger.info(f"  Invalid records: {invalid_records}")
            
            # Verify final silver table
            final_silver_df = self.spark.read.format("delta").load(silver_path)
            final_count = final_silver_df.count()
            self.logger.info(f"  Final silver table count: {final_count}")
            
        except Exception as e:
            self.logger.error(f"Error in public power transformation: {str(e)}")
            raise

    def transform_price_to_silver(self, country: str = "de") -> None:
        """Transform price data from Bronze to Silver"""
        
        self.logger.info("Starting price Bronze to Silver transformation")
        
        try:
            # Read from Bronze layer
            bronze_path = f"{self.config['delta_lake']['tables']['bronze_price']}"
            silver_path = f"{self.config['delta_lake']['tables']['silver_price']}"
            
            self.logger.info(f"Reading from Bronze path: {bronze_path}")
            bronze_df = self.spark.read.format("delta").load(bronze_path)
            
            if bronze_df.count() == 0:
                self.logger.warning("No data found in Bronze price table")
                return
            
            # Apply transformations using the proper schema
            silver_df = bronze_df.select(
                # Generate record_id using UUID
                expr("uuid()").alias("record_id"),
                # Generate business_key from timestamp and country (no production_type for price data)
                sha2(concat_ws("_", col("timestamp"), lit(country)), 256).alias("business_key"),
                col("timestamp"),
                unix_timestamp(col("timestamp")).alias("unix_seconds"),
                col("electricity_price"),
                coalesce(col("electricity_unit"), lit("EUR/MWh")).alias("electricity_unit"),
                lit(country).alias("country"),
                lit("energy_charts_api").alias("data_source"),
                lit("energy_charts_api").alias("source_system"),
                lit("PASSED").alias("dq_check_status"),  # Will be updated by data quality checks
                current_timestamp().cast("string").alias("dq_check_date"),
                lit("100").alias("dq_score"),  # Will be updated by data quality checks
                lit(False).alias("is_anomaly"),  # Will be updated by anomaly detection
                col("record_id").alias("bronze_record_id"),  # Reference to bronze record
                lit(self.job_id).alias("silver_batch_id"),
                col("ingestion_date").alias("ingestion_timestamp"),
                current_timestamp().alias("processing_timestamp"),
                lit(1.0).alias("data_quality_score"),  # Will be updated by data quality checks
                lit(1.0).alias("created_by")  # Using 1.0 as default, should be user ID in production
            )
            
            # Apply data quality checks
            silver_df = self._apply_data_quality_checks(silver_df, "price")
            
            # Update DQ fields based on quality checks
            silver_df = silver_df.withColumn(
                "dq_check_status", 
                when(col("is_valid") == True, "PASSED").otherwise("FAILED")
            ).withColumn(
                "dq_score",
                (col("data_quality_score") * 100).cast("string")
            )
            
            # Detect anomalies
            silver_df = self._detect_anomalies(silver_df, "electricity_price")
            
            # Remove duplicates before merge
            partition_cols = ["timestamp", "production_type", "country"]
            silver_df = self._deduplicate_records(silver_df, partition_cols)
            
            # Create Silver directory if it doesn't exist
            Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Perform optimized MERGE operation
            self.logger.info("Performing optimized MERGE operation")
            zorder_columns = ["business_key", "timestamp", "production_type", "country"]
            self._merge_with_optimizations(silver_df, silver_path, "business_key", zorder_columns)
            
            # Log transformation stats
            total_records = silver_df.count()
            valid_records = silver_df.filter(col("is_valid") == True).count()
            invalid_records = total_records - valid_records
            
            self.logger.info(f"Price transformation completed:")
            self.logger.info(f"  Total records processed: {total_records}")
            self.logger.info(f"  Valid records: {valid_records}")
            self.logger.info(f"  Invalid records: {invalid_records}")
            
            # Verify final silver table
            final_silver_df = self.spark.read.format("delta").load(silver_path)
            final_count = final_silver_df.count()
            self.logger.info(f"  Final silver table count: {final_count}")
            
        except Exception as e:
            self.logger.error(f"Error in price transformation: {str(e)}")
            raise

    def transform_installed_power_to_silver(self, country: str = "de") -> None:
        """Transform installed power data from Bronze to Silver"""
        
        self.logger.info("Starting installed power Bronze to Silver transformation")
        
        try:
            # Read from Bronze layer
            bronze_path = f"{self.config['delta_lake']['tables']['bronze_installed_power']}"
            silver_path = f"{self.config['delta_lake']['tables']['silver_installed_power']}"
            
            self.logger.info(f"Reading from Bronze path: {bronze_path}")
            bronze_df = self.spark.read.format("delta").load(bronze_path)
            
            if bronze_df.count() == 0:
                self.logger.warning("No data found in Bronze installed_power table")
                return
            
            # Apply transformations using the proper schema
            silver_df = bronze_df.select(
                # Generate record_id using UUID
                expr("uuid()").alias("record_id"),
                # Generate business_key from timestamp, production_type, country
                sha2(concat_ws("_", col("timestamp"), col("production_type"), lit(country)), 256).alias("business_key"),
                col("timestamp"),
                col("time_period"),
                col("production_type"),
                col("installed_capacity"),
                lit("GW").alias("unit"),
                lit(country).alias("country"),
                lit("energy_charts_api").alias("data_source"),
                col("source_system"),
                lit("PASSED").alias("dq_check_status"),  # Will be updated by data quality checks
                current_timestamp().cast("string").alias("dq_check_date"),
                lit("100").alias("dq_score"),  # Will be updated by data quality checks
                lit(False).alias("is_anomaly"),  # Will be updated by anomaly detection
                col("record_id").alias("bronze_record_id"),  # Reference to bronze record
                lit(self.job_id).alias("silver_batch_id"),
                col("ingestion_date").alias("ingestion_timestamp"),
                current_timestamp().alias("processing_timestamp"),
                lit(1.0).alias("data_quality_score"),  # Will be updated by data quality checks
                lit(1.0).alias("created_by")  # Using 1.0 as default, should be user ID in production
            )
            
            # Standardize production types
            silver_df = self._standardize_production_types(silver_df)
            
            # Apply data quality checks
            silver_df = self._apply_data_quality_checks(silver_df, "installed_power")
            
            # Update DQ fields based on quality checks
            silver_df = silver_df.withColumn(
                "dq_check_status", 
                when(col("is_valid") == True, "PASSED").otherwise("FAILED")
            ).withColumn(
                "dq_score",
                (col("data_quality_score") * 100).cast("string")
            )
            
            # Detect anomalies
            silver_df = self._detect_anomalies(silver_df, "installed_capacity")
            
            # Remove duplicates before merge
            partition_cols = ["timestamp", "production_type", "country"]
            silver_df = self._deduplicate_records(silver_df, partition_cols)
            
            # Create Silver directory if it doesn't exist
            Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Perform optimized MERGE operation
            self.logger.info("Performing optimized MERGE operation")
            zorder_columns = ["business_key", "timestamp", "production_type", "country"]
            self._merge_with_optimizations(silver_df, silver_path, "business_key", zorder_columns)
            
            # Log transformation stats
            total_records = silver_df.count()
            valid_records = silver_df.filter(col("is_valid") == True).count()
            invalid_records = total_records - valid_records
            
            self.logger.info(f"Installed Power transformation completed:")
            self.logger.info(f"  Total records processed: {total_records}")
            self.logger.info(f"  Valid records: {valid_records}")
            self.logger.info(f"  Invalid records: {invalid_records}")
            
            # Verify final silver table
            final_silver_df = self.spark.read.format("delta").load(silver_path)
            final_count = final_silver_df.count()
            self.logger.info(f"  Final silver table count: {final_count}")
            
        except Exception as e:
            self.logger.error(f"Error in installed power transformation: {str(e)}")
            raise

    def _optimize_table_with_zorder(self, table_path: str, zorder_columns: list) -> None:
        """Optimize table with Z-ordering for better query performance"""
        try:
            self.logger.info(f"Optimizing table with Z-ordering: {table_path}")
            zorder_cols = ", ".join(zorder_columns)
            self.spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_cols})")
            self.logger.info(f"Z-ordering optimization completed for columns: {zorder_cols}")
        except Exception as e:
            self.logger.warning(f"Z-ordering optimization failed for {table_path}: {str(e)}")

    def _vacuum_silver_table(self, table_path: str, retention_hours: int = 168) -> None:
        """Vacuum Silver table to remove old file versions (default 7 days retention)"""
        try:
            self.logger.info(f"Starting VACUUM operation on: {table_path}")
            self.spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS")
            self.logger.info(f"VACUUM completed for: {table_path}")
        except Exception as e:
            self.logger.warning(f"VACUUM operation failed for {table_path}: {str(e)}")

    def _analyze_table_statistics(self, table_path: str) -> None:
        """Update table statistics for query optimization"""
        try:
            self.logger.info(f"Analyzing table statistics: {table_path}")
            self.spark.sql(f"ANALYZE TABLE delta.`{table_path}` COMPUTE STATISTICS")
            self.logger.info(f"Table statistics updated for: {table_path}")
        except Exception as e:
            self.logger.warning(f"Table statistics analysis failed for {table_path}: {str(e)}")

    def _create_empty_silver_tables(self) -> None:
        """Create empty Silver tables with proper schemas if they don't exist"""
        try:
            # Initialize schemas first
            self._create_silver_schemas()
            
            # Create empty public power table
            public_power_path = self.config['delta_lake']['tables']['silver_public_power']
            if not self._table_exists(public_power_path):
                self.logger.info("Creating empty silver_public_power table")
                empty_df = self.spark.createDataFrame([], self.silver_public_power_schema)
                (empty_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("delta.enableChangeDataFeed", "true")
                 .partitionBy("country", "production_type")
                 .save(public_power_path))

            # Create empty price table
            price_path = self.config['delta_lake']['tables']['silver_price']
            if not self._table_exists(price_path):
                self.logger.info("Creating empty silver_price table")
                empty_df = self.spark.createDataFrame([], self.silver_price_schema)
                (empty_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("delta.enableChangeDataFeed", "true")
                 .partitionBy("country", "production_type")
                 .save(price_path))

            # Create empty installed power table
            installed_power_path = self.config['delta_lake']['tables']['silver_installed_power']
            if not self._table_exists(installed_power_path):
                self.logger.info("Creating empty silver_installed_power table")
                empty_df = self.spark.createDataFrame([], self.silver_installed_power_schema)
                (empty_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("delta.enableChangeDataFeed", "true")
                 .partitionBy("country", "production_type")
                 .save(installed_power_path))

            self.logger.info("All empty Silver tables created successfully")

        except Exception as e:
            self.logger.error(f"Error creating empty Silver tables: {str(e)}")
            raise

    def _table_exists(self, table_path: str) -> bool:
        """Check if a Delta table exists"""
        try:
            self.spark.read.format("delta").load(table_path)
            return True
        except:
            return False

    def _merge_with_optimizations(self, silver_df: DataFrame, silver_path: str, 
                                 business_key: str, zorder_columns: list) -> None:
        """Perform MERGE operation with post-merge optimizations"""
        
        # Check if table exists
        table_exists = self._table_exists(silver_path)
        
        if table_exists:
            # Perform MERGE operation
            delta_table = DeltaTable.forPath(self.spark, silver_path)
            
            self.logger.info("Performing MERGE operation with Delta Lake")
            (delta_table.alias("target")
             .merge(
                 silver_df.alias("source"),
                 f"target.{business_key} = source.{business_key}"
             )
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
            
            # Post-merge optimizations
            self._optimize_table_with_zorder(silver_path, zorder_columns)
            
        else:
            # Create new table
            self.logger.info(f"Creating new silver table at: {silver_path}")
            (silver_df.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .option("delta.enableChangeDataFeed", "true")
             .partitionBy("country", "production_type")
             .save(silver_path))
            
            # Initial optimization
            self._optimize_table_with_zorder(silver_path, zorder_columns)
    

    def _detect_anomalies_with_windowing(self, df: DataFrame, value_column: str, 
                                        partition_cols: list = None) -> DataFrame:
        """Enhanced anomaly detection using windowing for better context"""
        
        self.logger.info(f"Detecting anomalies with windowing for {value_column}")
        
        try:
            # Define window for rolling statistics
            if partition_cols:
                window_spec = Window.partitionBy(*partition_cols).orderBy("timestamp").rowsBetween(-10, 10)
            else:
                window_spec = Window.orderBy("timestamp").rowsBetween(-10, 10)
            
            # Calculate rolling statistics
            df_with_rolling = df.withColumn(
                "rolling_mean", avg(col(value_column)).over(window_spec)
            ).withColumn(
                "rolling_stddev", stddev(col(value_column)).over(window_spec)
            )
            
            # Detect anomalies using rolling statistics
            df_with_anomalies = df_with_rolling.withColumn(
                "is_anomaly",
                when(
                    (col("rolling_stddev") > 0) & (
                        (col(value_column) > (col("rolling_mean") + 3 * col("rolling_stddev"))) |
                        (col(value_column) < (col("rolling_mean") - 3 * col("rolling_stddev")))
                    ), True
                ).otherwise(False)
            ).drop("rolling_mean", "rolling_stddev")
            
            return df_with_anomalies
            
        except Exception as e:
            self.logger.warning(f"Enhanced anomaly detection failed, falling back to simple method: {str(e)}")
            return self._detect_anomalies(df, value_column)

    def run_all_transformations(self, country: str = "de") -> None:
        """Run all Bronze to Silver transformations with optimizations"""
        
        self.logger.info("Starting all Bronze to Silver transformations")
        
        try:
            # Initialize schemas and create empty tables if needed
            self._create_empty_silver_tables()
            
            # Transform each data type
            self.transform_public_power_to_silver(country)
            self.transform_price_to_silver(country)
            self.transform_installed_power_to_silver(country)
            
            self.logger.info("All Bronze to Silver transformations completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in Bronze to Silver transformations: {str(e)}")
            raise
    
    def _configure_spark_optimizations(self) -> None:
        """Configure Spark optimizations for better performance"""
        try:
            # Enable Adaptive Query Execution (AQE)
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            
            # Delta Lake optimizations
            self.spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
            self.spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
            
            # Broadcast threshold optimization
            self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20MB")
            
            self.logger.info("Spark optimizations configured successfully")
            
        except Exception as e:
            self.logger.warning(f"Failed to configure some Spark optimizations: {str(e)}")




    def _run_transformation_step(self, step_name: str, country: str) -> None:
        """Run a specific transformation step with error handling"""
        try:
            if step_name == "public_power":
                self.transform_public_power_to_silver(country)
            elif step_name == "price":
                self.transform_price_to_silver(country)
            elif step_name == "installed_power":
                self.transform_installed_power_to_silver(country)
            else:
                self.logger.warning(f"Unknown transformation step: {step_name}")
        except Exception as e:
            self.logger.error(f"Error in {step_name} transformation: {str(e)}")
            raise

    def run_incremental_transformations(self, country: str = "de") -> None:
        """Run incremental transformations for all data types"""
        self.logger.info("Starting incremental transformations")
        
        try:
            # Get the latest processed timestamp for each data type
            latest_timestamps = {}
            silver_tables = ["silver_public_power", "silver_price", "silver_installed_power"]
            
            for table_name in silver_tables:
                table_path = self.config['delta_lake']['tables'][table_name]
                if self._table_exists(table_path):
                    # Read the latest record
                    latest_record = self.spark.read.format("delta").load(table_path).orderBy(desc("timestamp")).limit(1)
                    latest_timestamps[table_name] = latest_record.collect()[0] if latest_record.count() > 0 else None
                else:
                    latest_timestamps[table_name] = None
            
            # Run transformations for each data type
            for table_name, latest_record in latest_timestamps.items():
                if latest_record is not None:
                    country_filter = latest_record["country"] if "country" in latest_record else "de"
                    self.logger.info(f"Running transformation for {table_name} since {latest_record['timestamp']}")
                    self._run_transformation_step(table_name.split("_")[-1], country_filter)
                else:
                    self.logger.info(f"No new data to process for {table_name}")
            
            # Update table statistics after transformations
            self.logger.info("Updating table statistics for query optimization")
            for table_name in silver_tables:
                table_path = self.config['delta_lake']['tables'][table_name]
                if self._table_exists(table_path):
                    self._analyze_table_statistics(table_path)
            
            self.logger.info("Incremental transformations completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in incremental transformations: {str(e)}")
            raise
    

# Convenience functions for individual job execution
def run_public_power_bronze_to_silver(spark: SparkSession, logger: Logger, job_id:str, config: Dict[str, Any], country: str = "de"):
    """Run public power Bronze to Silver transformation"""
    transformer = BronzeToSilverTransformation(spark, logger, job_id, config)
    transformer.transform_public_power_to_silver(country)

def run_price_bronze_to_silver(spark: SparkSession, logger: Logger, job_id:str, config: Dict[str, Any], country: str = "de"):
    """Run price Bronze to Silver transformation"""
    transformer = BronzeToSilverTransformation(spark, logger, job_id, config)
    transformer.transform_price_to_silver(country)

def run_installed_power_bronze_to_silver(spark: SparkSession, logger: Logger, job_id:str, config: Dict[str, Any], country: str = "de"):
    """Run installed power Bronze to Silver transformation using SQL"""
    transformer = BronzeToSilverTransformation(spark, logger, job_id, config)
    transformer.transform_installed_power_to_silver(country)

def run_all_bronze_to_silver(spark: SparkSession, logger: Logger, job_id:str, config: Dict[str, Any], country: str = "de"):
    """Run all Bronze to Silver transformations"""
    transformer = BronzeToSilverTransformation(spark, logger, job_id, config)
    transformer.run_all_transformations(country)


