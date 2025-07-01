import sys
import os
from pyspark.sql.functions import (
    sha2, concat_ws, current_timestamp, lit, col, year, month, dayofmonth, hour, minute, 
    avg, stddev, count, min as min_, max as max_, sum as sum_, expr, when, coalesce, 
    date_format, to_date, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, BooleanType, DateType
from delta.tables import DeltaTable
from src.logger.custom_logger import Logger
from src.utils import map_production_attributes
from typing import Dict, Any
from pathlib import Path
from datetime import datetime, timezone

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

class SilverToGoldTransformation:
    """
    Handles Bronze to Silver layer transformations for all energy data types.
    Applies data quality checks, cleansing, and standardization.
    """
    def __init__(self, spark, logger, job_id, config):
        self.spark = spark
        self.logger = logger
        self.job_id = job_id
        self.config = config
        self.gold_path = config['delta_lake']['storage']['gold_path']

    def _create_gold_schemas(self):
        """Define schemas for Gold layer tables"""
        self.gold_dim_production_type_schema = StructType([
            StructField("production_type_id", StringType(), nullable=False),
            StructField("production_type", StringType(), nullable=False),
            StructField("energy_category", StringType(), nullable=False),
            StructField("controllability_type", StringType(), nullable=False),
            StructField("description", StringType(), nullable=True),
            StructField("country", StringType(), nullable=False),
            StructField("effective_date", DateType(), nullable=False),
            StructField("expiry_date", DateType(), nullable=True),
            StructField("active_flag", BooleanType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=True)
        ])

        self.gold_fact_power_schema = StructType([
            StructField("record_id", StringType(), nullable=False),
            StructField("year", IntegerType(), nullable=False),
            StructField("month", IntegerType(), nullable=False),
            StructField("day", IntegerType(), nullable=False),
            StructField("hour", IntegerType(), nullable=False),
            StructField("minute", IntegerType(), nullable=False),
            StructField("minute_interval_30", IntegerType(), nullable=False),
            StructField("timestamp_30min", TimestampType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("production_type_id", StringType(), nullable=False),
            StructField("electricity_produced", DoubleType(), nullable=False),
            StructField("electricity_price", DoubleType(), nullable=True),
            StructField("country", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=True)
        ])

        self.gold_fact_power_30min_agg_schema = StructType([
            StructField("record_id", StringType(), nullable=False),
            StructField("year", IntegerType(), nullable=False),
            StructField("month", IntegerType(), nullable=False),
            StructField("day", IntegerType(), nullable=False),
            StructField("hour", IntegerType(), nullable=False),
            StructField("minute", IntegerType(), nullable=False),
            StructField("minute_interval_30", IntegerType(), nullable=False),
            StructField("timestamp_30min", TimestampType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("production_type_id", StringType(), nullable=False),
            StructField("avg_electricity_produced", DoubleType(), nullable=True),
            StructField("total_electricity_produced", DoubleType(), nullable=True),
            StructField("min_electricity_produced", DoubleType(), nullable=True),
            StructField("max_electricity_produced", DoubleType(), nullable=True),
            StructField("production_volatility", DoubleType(), nullable=True),
            StructField("data_points_count", IntegerType(), nullable=True),
            StructField("avg_electricity_price", DoubleType(), nullable=True),
            StructField("country", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=True)
        ])

    def load_dim_production_type(self) -> None:
        """
        Loads the gold_dim_production_type dimension table from standardized silver tables.
        Extracts distinct production types from both silver_public_power and silver_installed_power,
        maps to gold attributes, and writes to the gold layer using Delta Lake MERGE.
        """
        self.logger.info("Starting Silver to Gold load for dim_production_type")
        try:
            # Read from Silver Delta tables
            silver_public_path = self.config['delta_lake']['tables']['silver_public_power']
            silver_installed_path = self.config['delta_lake']['tables']['silver_installed_power']
            gold_path = self.config['delta_lake']['tables']['gold_dim_production_type']

            self.logger.info("Reading silver tables for production type dimension")
            public_power_df = self.spark.read.format("delta").load(silver_public_path)
            installed_power_df = self.spark.read.format("delta").load(silver_installed_path)

            # Get distinct production types from both silver tables
            distinct_prod_types = (
                public_power_df.select("production_type", "country")
                .union(installed_power_df.select("production_type", "country"))
                .distinct()
            )

            self.logger.info(f"Found {distinct_prod_types.count()} distinct production types")

            # Map production attributes using UDF
            from pyspark.sql.functions import udf
            attr_schema = StructType([
                StructField("production_type", StringType()),
                StructField("energy_category", StringType()),
                StructField("controllability_type", StringType()),
                StructField("description", StringType())
            ])
            map_udf = udf(map_production_attributes, attr_schema)

            # Apply mapping and flatten the struct
            mapped_df = distinct_prod_types.withColumn("mapped", map_udf(col("production_type")))
            for field in attr_schema.fieldNames():
                mapped_df = mapped_df.withColumn(field, col(f"mapped.{field}"))

            # Transform to match Gold schema exactly
            gold_df = mapped_df.select(
                # Generate production_type_id as business key
                sha2(concat_ws("_", col("production_type"), col("country")), 256).alias("production_type_id"),
                col("production_type"),
                col("energy_category"),
                col("controllability_type"),
                col("description"),
                col("country"),
                to_date(current_timestamp()).alias("effective_date"),
                lit(None).cast(DateType()).alias("expiry_date"),
                lit(True).alias("active_flag"),
                current_timestamp().alias("created_at"),
                lit(None).cast(TimestampType()).alias("updated_at")
            )

            # Create Gold directory if it doesn't exist
            Path(gold_path).parent.mkdir(parents=True, exist_ok=True)

            # Check if gold table exists for merge operation
            try:
                existing_gold_df = self.spark.read.format("delta").load(gold_path)
                table_exists = True
                self.logger.info("Existing gold dim_production_type table found, performing merge operation")
            except:
                table_exists = False
                self.logger.info("No existing gold dim_production_type table found, creating new table")

            if table_exists:
                # Perform MERGE operation using DeltaTable
                delta_table = DeltaTable.forPath(self.spark, gold_path)
                
                # Perform merge operation - SCD Type 1 (update in place)
                self.logger.info("Performing MERGE operation on gold dim_production_type table")
                (delta_table.alias("target")
                 .merge(
                     gold_df.alias("source"),
                     "target.production_type_id = source.production_type_id"
                 )
                 .whenMatchedUpdate(set={
                     "production_type": "source.production_type",
                     "energy_category": "source.energy_category",
                     "controllability_type": "source.controllability_type",
                     "description": "source.description",
                     "country": "source.country",
                     "active_flag": "source.active_flag",
                     "updated_at": "current_timestamp()"
                 })
                 .whenNotMatchedInsertAll()
                 .execute())

                # Optimize table with Z-ordering
                self.logger.info("Optimizing table with Z-ordering")
                self.spark.sql(f"OPTIMIZE `{gold_path}` ZORDER BY (production_type_id, country)")

            else:
                # Create new table with proper schema and partitioning
                self.logger.info(f"Creating new gold dim_production_type table at: {gold_path}")
                (gold_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .option("delta.enableChangeDataFeed", "true")
                 .partitionBy("country")
                 .save(gold_path))

                # Apply Z-ordering after table creation
                self.logger.info("Applying Z-ordering for optimal performance")
                self.spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (production_type_id, country)")

            # Verify final gold table
            final_gold_df = self.spark.read.format("delta").load(gold_path)
            final_count = final_gold_df.count()
            self.logger.info(f"Gold dim_production_type loaded successfully: {final_count} records")

        except Exception as e:
            self.logger.error(f"Error loading dim_production_type: {str(e)}")
            raise

    def load_gold_fact_power(self) -> None:
        """
        Loads the gold_fact_power table from silver_public_power, silver_price, and gold_dim_production_type.
        Joins production and price data, links to production_type_id, and writes to the gold layer using Delta Lake MERGE.
        """
        self.logger.info("Starting loading gold_fact_power table")
        try:
            # Read from Delta tables
            silver_public_path = self.config['delta_lake']['tables']['silver_public_power']
            silver_price_path = self.config['delta_lake']['tables']['silver_price']
            gold_dim_production_type_path = self.config['delta_lake']['tables']['gold_dim_production_type']
            gold_fact_power_path = self.config['delta_lake']['tables']['gold_fact_power']

            self.logger.info("Reading silver and gold dimension tables")
            public_power_df = self.spark.read.format("delta").load(silver_public_path)
            price_df = self.spark.read.format("delta").load(silver_price_path)
            dim_production_type_df = self.spark.read.format("delta").load(gold_dim_production_type_path)

            self.logger.info("Joining data sources for fact table")
            
            # Create production type mapping
            prodtype_map_df = dim_production_type_df.select(
                "production_type", "country", "production_type_id"
            ).withColumnRenamed("country", "dim_country")

            # Join public power with production type dimension
            fact_df = public_power_df.join(
                prodtype_map_df,
                (public_power_df.production_type == prodtype_map_df.production_type) &
                (public_power_df.country == prodtype_map_df.dim_country),
                how="left"
            )

            # Join with price data
            price_df_clean = price_df.select(
                col("timestamp").alias("price_timestamp"),
                col("electricity_price").alias("joined_electricity_price"),
                col("country").alias("price_country")
            )
            
            fact_df = fact_df.join(
                price_df_clean,
                (fact_df.timestamp == price_df_clean.price_timestamp) &
                (fact_df.country == price_df_clean.price_country),
                how="left"
            )

            # Calculate 30-minute intervals
            fact_df = fact_df.withColumn(
                "minute_interval_30",
                when(minute(col("timestamp")) < 30, 0).otherwise(30)
            )
            
            fact_df = fact_df.withColumn(
                "timestamp_30min",
                expr("""
                    CAST(
                        CONCAT(
                            CAST(year(timestamp) AS STRING), '-',
                            LPAD(CAST(month(timestamp) AS STRING), 2, '0'), '-',
                            LPAD(CAST(day(timestamp) AS STRING), 2, '0'), ' ',
                            LPAD(CAST(hour(timestamp) AS STRING), 2, '0'), ':',
                            LPAD(CAST(
                                CASE WHEN minute(timestamp) < 30 THEN 0 ELSE 30 END
                            AS STRING), 2, '0'), ':00'
                        ) AS TIMESTAMP
                    )
                """)
            )

            # Generate record_id as business key
            fact_df = fact_df.withColumn(
                "record_id",
                sha2(concat_ws("_",
                    col("timestamp").cast("string"),
                    coalesce(col("production_type_id"), lit("unknown")),
                    col("country")
                ), 256)
            )

            # Transform to match Gold fact power schema exactly
            gold_fact_df = fact_df.select(
                col("record_id"),
                year(col("timestamp")).alias("year"),
                month(col("timestamp")).alias("month"),
                dayofmonth(col("timestamp")).alias("day"),
                hour(col("timestamp")).alias("hour"),
                minute(col("timestamp")).alias("minute"),
                col("minute_interval_30"),
                col("timestamp_30min"),
                col("timestamp"),
                coalesce(col("production_type_id"), lit("unknown")).alias("production_type_id"),
                coalesce(col("electricity_generated"), lit(0.0)).alias("electricity_produced"),
                col("joined_electricity_price").cast(DoubleType()).alias("electricity_price"),
                col("country"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at")
            ).filter(col("production_type_id").isNotNull())  # Filter out records without production_type_id

            # Create Gold directory if it doesn't exist
            Path(gold_fact_power_path).parent.mkdir(parents=True, exist_ok=True)

            # Check if gold table exists for merge operation
            try:
                existing_gold_df = self.spark.read.format("delta").load(gold_fact_power_path)
                table_exists = True
                self.logger.info("Existing gold fact_power table found, performing merge operation")
            except:
                table_exists = False
                self.logger.info("No existing gold fact_power table found, creating new table")

            if table_exists:
                # Perform MERGE operation using DeltaTable
                delta_table = DeltaTable.forPath(self.spark, gold_fact_power_path)
                
                # Perform merge operation
                self.logger.info("Performing MERGE operation on gold fact_power table")
                (delta_table.alias("target")
                 .merge(
                     gold_fact_df.alias("source"),
                     "target.record_id = source.record_id"
                 )
                 .whenMatchedUpdate(set={
                     "year": "source.year",
                     "month": "source.month",
                     "day": "source.day",
                     "hour": "source.hour",
                     "minute": "source.minute",
                     "minute_interval_30": "source.minute_interval_30",
                     "timestamp_30min": "source.timestamp_30min",
                     "timestamp": "source.timestamp",
                     "production_type_id": "source.production_type_id",
                     "electricity_produced": "source.electricity_produced",
                     "electricity_price": "source.electricity_price",
                     "country": "source.country",
                     "updated_at": "current_timestamp()"
                 })
                 .whenNotMatchedInsertAll()
                 .execute())

                # Optimize with Z-ordering
                self.logger.info("Optimizing table with Z-ordering")
                self.spark.sql(f"OPTIMIZE delta.`{gold_fact_power_path}` ZORDER BY (production_type_id, country, year, month)")

            else:
                # Create new table with proper schema and partitioning
                self.logger.info(f"Creating new gold fact_power table at: {gold_fact_power_path}")
                (gold_fact_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .option("delta.enableChangeDataFeed", "true")
                 .partitionBy("year", "month", "country")
                 .save(gold_fact_power_path))

                # Apply Z-ordering after table creation
                self.logger.info("Applying Z-ordering for optimal performance")
                self.spark.sql(f"OPTIMIZE `{gold_fact_power_path}` ZORDER BY (production_type_id, country, year, month)")

            # Verify final gold table
            final_gold_df = self.spark.read.format("delta").load(gold_fact_power_path)
            final_count = final_gold_df.count()
            self.logger.info(f"Gold fact_power table loaded successfully: {final_count} records")

        except Exception as e:
            self.logger.error(f"Error loading gold_fact_power table: {str(e)}")
            raise

    def load_gold_fact_power_30min_agg(self) -> None:
        """
        Loads the gold_fact_power_30min_agg table by aggregating gold_fact_power at 30-minute intervals.
        Computes aggregates for electricity production and price, grouped by production_type_id, country, and 30-min interval.
        Uses Delta Lake MERGE for efficient upserts.
        """
        self.logger.info("Starting loading gold_fact_power_30min_agg table")
        try:
            gold_fact_power_path = self.config['delta_lake']['tables']['gold_fact_power']
            gold_fact_power_30min_agg_path = self.config['delta_lake']['tables']['gold_fact_power_30min_agg']

            self.logger.info("Reading gold fact_power table for aggregation")
            fact_df = self.spark.read.format("delta").load(gold_fact_power_path)

            # Ensure consistent 30-minute intervals
            fact_df = fact_df.withColumn(
                "minute_interval_30",
                when(minute(col("timestamp")) < 30, 0).otherwise(30)
            )
            
            fact_df = fact_df.withColumn(
                "timestamp_30min",
                expr("""
                    CAST(
                        FROM_UNIXTIME(
                            UNIX_TIMESTAMP(timestamp) - (UNIX_TIMESTAMP(timestamp) % 1800)
                        ) AS TIMESTAMP
                    )
                """)
            )

            self.logger.info("Performing 30-minute aggregations")
            # Perform aggregations grouped by 30-minute intervals
            agg_df = (
                fact_df.groupBy(
                    "production_type_id", "country", "timestamp_30min"
                ).agg(
                    year(col("timestamp_30min")).alias("year"),
                    month(col("timestamp_30min")).alias("month"),
                    dayofmonth(col("timestamp_30min")).alias("day"),
                    hour(col("timestamp_30min")).alias("hour"),
                    minute(col("timestamp_30min")).alias("minute"),
                    min_(col("minute_interval_30")).alias("minute_interval_30"),
                    min_(col("timestamp")).alias("timestamp"),
                    avg(col("electricity_produced")).alias("avg_electricity_produced"),
                    sum_(col("electricity_produced")).alias("total_electricity_produced"),
                    min_(col("electricity_produced")).alias("min_electricity_produced"),
                    max_(col("electricity_produced")).alias("max_electricity_produced"),
                    stddev(col("electricity_produced")).alias("production_volatility"),
                    count(col("electricity_produced")).alias("data_points_count"),
                    avg(col("electricity_price")).alias("avg_electricity_price")
                )
            )

            # Generate record_id as business key for the aggregated data
            agg_df = agg_df.withColumn(
                "record_id",
                sha2(concat_ws("_",
                    col("production_type_id"),
                    col("country"),
                    col("timestamp_30min").cast("string")
                ), 256)
            )

            # Add audit columns
            agg_df = agg_df.withColumn("created_at", current_timestamp())
            agg_df = agg_df.withColumn("updated_at", current_timestamp())

            # Select columns to match Gold 30min aggregation schema exactly
            gold_agg_df = agg_df.select(
                col("record_id"),
                col("year"),
                col("month"),
                col("day"),
                col("hour"),
                col("minute"),
                col("minute_interval_30"),
                col("timestamp_30min"),
                col("timestamp"),
                col("production_type_id"),
                col("avg_electricity_produced"),
                col("total_electricity_produced"),
                col("min_electricity_produced"),
                col("max_electricity_produced"),
                col("production_volatility"),
                col("data_points_count"),
                col("avg_electricity_price"),
                col("country"),
                col("created_at"),
                col("updated_at")
            )

            # Create Gold directory if it doesn't exist
            Path(gold_fact_power_30min_agg_path).parent.mkdir(parents=True, exist_ok=True)

            # Check if gold table exists for merge operation
            try:
                existing_gold_agg_df = self.spark.read.format("delta").load(gold_fact_power_30min_agg_path)
                table_exists = True
                self.logger.info("Existing gold fact_power_30min_agg table found, performing merge operation")
            except:
                table_exists = False
                self.logger.info("No existing gold fact_power_30min_agg table found, creating new table")

            if table_exists:
                # Perform MERGE operation using DeltaTable
                delta_table = DeltaTable.forPath(self.spark, gold_fact_power_30min_agg_path)
                
                # Perform merge operation
                self.logger.info("Performing MERGE operation on gold fact_power_30min_agg table")
                (delta_table.alias("target")
                 .merge(
                     gold_agg_df.alias("source"),
                     "target.record_id = source.record_id"
                 )
                 .whenMatchedUpdate(set={
                     "year": "source.year",
                     "month": "source.month",
                     "day": "source.day",
                     "hour": "source.hour",
                     "minute": "source.minute",
                     "minute_interval_30": "source.minute_interval_30",
                     "timestamp_30min": "source.timestamp_30min",
                     "timestamp": "source.timestamp",
                     "production_type_id": "source.production_type_id",
                     "avg_electricity_produced": "source.avg_electricity_produced",
                     "total_electricity_produced": "source.total_electricity_produced",
                     "min_electricity_produced": "source.min_electricity_produced",
                     "max_electricity_produced": "source.max_electricity_produced",
                     "production_volatility": "source.production_volatility",
                     "data_points_count": "source.data_points_count",
                     "avg_electricity_price": "source.avg_electricity_price",
                     "country": "source.country",
                     "updated_at": "current_timestamp()"
                 })
                 .whenNotMatchedInsertAll()
                 .execute())

                # Optimize with Z-ordering
                self.logger.info("Optimizing table with Z-ordering")
                self.spark.sql(f"OPTIMIZE `{gold_fact_power_30min_agg_path}` ZORDER BY (production_type_id, country, year, month, day)")

            else:
                # Create new table with proper schema and partitioning
                self.logger.info(f"Creating new gold fact_power_30min_agg table at: {gold_fact_power_30min_agg_path}")
                (gold_agg_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .option("delta.enableChangeDataFeed", "true")
                 .partitionBy("year", "month", "country")
                 .save(gold_fact_power_30min_agg_path))

                # Apply Z-ordering after table creation
                self.logger.info("Applying Z-ordering for optimal performance")
                self.spark.sql(f"OPTIMIZE `{gold_fact_power_30min_agg_path}` ZORDER BY (production_type_id, country, year, month, day)")

            # Verify final gold table
            final_gold_agg_df = self.spark.read.format("delta").load(gold_fact_power_30min_agg_path)
            final_count = final_gold_agg_df.count()
            self.logger.info(f"Gold fact_power_30min_agg table loaded successfully: {final_count} records")

        except Exception as e:
            self.logger.error(f"Error loading gold_fact_power_30min_agg table: {str(e)}")
            raise

    def _create_empty_gold_tables(self) -> None:
        """Create empty Gold tables with proper schemas if they don't exist"""
        
        try:
            # Create empty dimension table
            dim_path = self.config['delta_lake']['tables']['gold_dim_production_type']
            if not Path(dim_path).exists():
                self.logger.info("Creating empty gold_dim_production_type table")
                empty_dim_df = self.spark.createDataFrame([], self.gold_dim_production_type_schema)
                (empty_dim_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("delta.enableChangeDataFeed", "true")
                 .partitionBy("country")
                 .save(dim_path))

            # Create empty fact power table
            fact_path = self.config['delta_lake']['tables']['gold_fact_power']
            if not Path(fact_path).exists():
                self.logger.info("Creating empty gold_fact_power table")
                empty_fact_df = self.spark.createDataFrame([], self.gold_fact_power_schema)
                (empty_fact_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("delta.enableChangeDataFeed", "true")
                 .partitionBy("year", "month", "country")
                 .save(fact_path))

            # Create empty fact power 30min agg table
            fact_agg_path = self.config['delta_lake']['tables']['gold_fact_power_30min_agg']
            if not Path(fact_agg_path).exists():
                self.logger.info("Creating empty gold_fact_power_30min_agg table")
                empty_fact_agg_df = self.spark.createDataFrame([], self.gold_fact_power_30min_agg_schema)
                (empty_fact_agg_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("delta.enableChangeDataFeed", "true")
                 .partitionBy("year", "month", "country")
                 .save(fact_agg_path))

            self.logger.info("All empty Gold tables created successfully")

        except Exception as e:
            self.logger.error(f"Error creating empty Gold tables: {str(e)}")
            raise


    def run_all_transformations(self) -> None:
        """Run all Silver to Gold transformations with optimizations"""
        self.logger.info("Starting all Silver to Gold transformations")
        try:
            # Initialize schemas
            self._create_gold_schemas()
            
            # Create empty tables if they don't exist
            self._create_empty_gold_tables()
            
            # Run transformations in dependency order
            self.load_dim_production_type()
            self.load_gold_fact_power()
            self.load_gold_fact_power_30min_agg()
            
            self.logger.info("All Silver to Gold transformations completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in Silver to Gold transformations: {str(e)}")
            raise



# Convenience functions for individual job execution
def run_dim_production_type(spark, logger, job_id, config):
    transformer = SilverToGoldTransformation(spark, logger, job_id, config)
    transformer.load_dim_production_type()

def run_fact_power(spark, logger, job_id, config):
    transformer = SilverToGoldTransformation(spark, logger, job_id, config)
    transformer.load_gold_fact_power()

def run_fact_power_30min_agg(spark, logger, job_id, config):
    transformer = SilverToGoldTransformation(spark, logger, job_id, config)
    transformer.load_gold_fact_power_30min_agg()

def run_all_silver_to_gold(spark, logger, job_id, config):
    transformer = SilverToGoldTransformation(spark, logger, job_id, config)
    transformer.run_all_transformations()

