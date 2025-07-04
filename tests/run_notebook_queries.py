#!/usr/bin/env python3
"""
Run the notebook queries directly as a Python script to test them outside of the notebook environment
"""
import sys
import os

# Add the project root to Python path
project_root = "/Users/srsu/Documents/Personal/git/delta_pipeline"
sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
from delta import *
import pyspark.sql.functions as F
from pyspark.sql.functions import avg, count

def init_spark():
    """Initialize Spark with basic configuration"""
    return (SparkSession.builder.master("local[*]")
    .appName("NotebookQueries")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.sql.warehouse.dir", "./delta_lake")
    .config("spark.driver.memory", "4g")
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
    .config("spark.executor.memory", "4g")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate())

def run_notebook_queries():
    """Execute the notebook queries"""
    print("🚀 Initializing Spark session...")
    spark = init_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark Version: {spark.version}")
    print(f"✅ Application Name: {spark.sparkContext.appName}")
    
    # Define Delta table paths
    delta_base_path = "/Users/srsu/Documents/Personal/git/delta_pipeline/delta_lake"
    
    print("\n📊 Reading Delta tables...")
    
    try:
        # Read dimension table
        gold_dim_production_type = spark.read.format("delta").load(f"{delta_base_path}/gold/dim_production_type")
        gold_dim_production_type.createOrReplaceTempView("gold_dim_production_type")
        print(f"✅ Loaded gold_dim_production_type: {gold_dim_production_type.count()} records")
        
        # Read daily fact table for underperformance prediction
        gold_fact_power = spark.read.format("delta").load(f"{delta_base_path}/gold/prediction_of_underperformance")
        gold_fact_power.createOrReplaceTempView("gold_fact_power")
        print(f"✅ Loaded gold_fact_power: {gold_fact_power.count()} records")
        
        # Read 30-minute aggregated fact table for daily price analysis
        gold_fact_power_30min_agg = spark.read.format("delta").load(f"{delta_base_path}/gold/gold_analysis_of_daily_price")
        gold_fact_power_30min_agg.createOrReplaceTempView("gold_fact_power_30min_agg")
        print(f"✅ Loaded gold_fact_power_30min_agg: {gold_fact_power_30min_agg.count()} records")
        
        print("✅ Successfully loaded all Delta tables")
        
        print("\n" + "="*70)
        print("EXECUTING NOTEBOOK QUERIES")
        print("="*70)
        
        # Query 1: Daily Production Trends
        print("\n🔍 Query 1: Daily Production Trends")
        print("=" * 50)
        
        daily_production_query = """
        SELECT
          f.year,
          f.month,
          f.day,
          d.production_type AS production_type,
          SUM(f.electricity_produced) AS total_daily_production
        FROM gold_fact_power f
        JOIN gold_dim_production_type d ON f.production_type_id = d.production_type_id
        WHERE f.country = 'de'
        GROUP BY f.year, f.month, f.day, d.production_type
        ORDER BY f.year, f.month, f.day, d.production_type
        """
        
        daily_trends_df = spark.sql(daily_production_query)
        print(f"Query executed successfully!")
        print(f"Results: {daily_trends_df.count()} records found")
        print("\nSample Results:")
        daily_trends_df.show(10, truncate=False)
        
        # Query 2: Underperformance Prediction Features
        print("\n🔍 Query 2: ML Features for Underperformance Prediction")
        print("=" * 65)
        
        underperformance_query = """
        SELECT
            f.timestamp_30min,
            f.production_type_id,
            d.production_type,
            d.energy_category,
            d.controllability_type,
            f.total_electricity_produced,
            f.year, f.month, f.day, f.hour, f.minute_interval_30,
            LAG(f.total_electricity_produced, 48) OVER (PARTITION BY f.production_type_id ORDER BY f.timestamp_30min) AS lag_1d,
            LAG(f.total_electricity_produced, 336) OVER (PARTITION BY f.production_type_id ORDER BY f.timestamp_30min) AS lag_1w,
            AVG(f.total_electricity_produced) OVER (
                PARTITION BY f.production_type_id, f.hour, f.minute_interval_30
                ORDER BY f.timestamp_30min
                ROWS BETWEEN 336 PRECEDING AND 1 PRECEDING
            ) AS rolling_7d_avg
        FROM gold_fact_power_30min_agg f
        JOIN gold_dim_production_type d ON f.production_type_id = d.production_type_id
        WHERE f.country = 'de' AND d.active_flag = TRUE
        LIMIT 10
        """
        
        underperformance_query_df = spark.sql(underperformance_query)
        print(f"Query executed successfully!")
        print(f"Results: {underperformance_query_df.count()} records found")
        print("\nSample ML Features:")
        underperformance_query_df.show(5, truncate=False)
        
        # Query 3: Wind Price Analysis
        print("\n🔍 Query 3: Wind Power vs Price Analysis")
        print("=" * 50)
        
        wind_price_query = """
        SELECT
          f.year, f.month, f.day,
          d.production_type AS production_type,
          SUM(f.electricity_produced) AS total_daily_production_mw,
          AVG(f.electricity_price) AS avg_daily_price_eur_per_mwh
        FROM gold_fact_power f
        JOIN gold_dim_production_type d ON f.production_type_id = d.production_type_id
        WHERE f.country = 'de'
          AND d.production_type LIKE '%Wind%'
          AND d.active_flag = TRUE 
        GROUP BY f.year, f.month, f.day, d.production_type
        ORDER BY f.year, f.month, f.day, d.production_type
        """
        
        wind_analysis_df = spark.sql(wind_price_query)
        print(f"Query executed successfully!")
        print(f"Results: {wind_analysis_df.count()} records found")
        print("\nWind Power vs Price Results:")
        wind_analysis_df.show(10, truncate=False)
        
        # Summary statistics by wind type
        if wind_analysis_df.count() > 0:
            print("\nSummary by Wind Type:")
            wind_summary = wind_analysis_df.groupBy("production_type").agg(
                avg("total_daily_production_mw").alias("avg_production"),
                avg("avg_daily_price_eur_per_mwh").alias("avg_price"),
                count("*").alias("total_days")
            )
            wind_summary.show(truncate=False)
        
        print("\n" + "="*70)
        print("✅ ALL NOTEBOOK QUERIES COMPLETED SUCCESSFULLY!")
        print("The Delta Lake pipeline is working correctly and all queries execute as expected.")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ Error during query execution: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🔄 Stopping Spark session...")
        spark.stop()
        print("✅ Test completed!")

if __name__ == "__main__":
    run_notebook_queries()
