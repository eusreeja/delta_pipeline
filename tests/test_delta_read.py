#!/usr/bin/env python3
"""
Test script to verify Delta tables can be read and queried successfully
"""
import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
from delta import *
import pyspark.sql.functions as F

def init_spark():
    """Initialize Spark with basic configuration"""
    return (SparkSession.builder.master("local[*]")
    .appName("DeltaTableTest")
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

def test_delta_tables():
    """Test reading and querying Delta tables"""
    print("🚀 Initializing Spark session...")
    spark = init_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark Version: {spark.version}")
    print(f"✅ Application Name: {spark.sparkContext.appName}")
    
    # Define Delta table paths
    delta_base_path = "/Users/srsu/Documents/Personal/git/delta_pipeline/delta_lake"
    
    print("\n📊 Testing Delta table reads...")
    
    try:
        # Test 1: Read dimension table
        print("\n1. Testing gold_dim_production_type...")
        gold_dim_production_type = spark.read.format("delta").load(f"{delta_base_path}/gold/dim_production_type")
        print(f"   ✅ Loaded successfully: {gold_dim_production_type.count()} records")
        gold_dim_production_type.createOrReplaceTempView("gold_dim_production_type")
        
        # Test 2: Read fact table for underperformance prediction
        print("\n2. Testing prediction_of_underperformance...")
        gold_fact_power = spark.read.format("delta").load(f"{delta_base_path}/gold/prediction_of_underperformance")
        print(f"   ✅ Loaded successfully: {gold_fact_power.count()} records")
        gold_fact_power.createOrReplaceTempView("gold_fact_power")
        
        # Test 3: Read daily analysis table
        print("\n3. Testing gold_analysis_of_daily_price...")
        gold_fact_power_30min_agg = spark.read.format("delta").load(f"{delta_base_path}/gold/gold_analysis_of_daily_price")
        print(f"   ✅ Loaded successfully: {gold_fact_power_30min_agg.count()} records")
        gold_fact_power_30min_agg.createOrReplaceTempView("gold_fact_power_30min_agg")
        
        print("\n🎯 Running sample queries...")
        
        # Query 1: Daily Production Trends
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
        LIMIT 10
        """
        
        print("\n🔍 Query 1: Daily Production Trends")
        daily_trends_df = spark.sql(daily_production_query)
        print(f"   Results: {daily_trends_df.count()} records found")
        print("   Sample data:")
        daily_trends_df.show(5, truncate=False)
        
        # Query 2: Wind Price Analysis
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
        LIMIT 10
        """
        
        print("\n🔍 Query 2: Wind Power Analysis")
        wind_analysis_df = spark.sql(wind_price_query)
        print(f"   Results: {wind_analysis_df.count()} records found")
        print("   Sample data:")
        wind_analysis_df.show(5, truncate=False)
        
        print("\n✅ All Delta table tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🔄 Stopping Spark session...")
        spark.stop()
        print("✅ Test completed!")

if __name__ == "__main__":
    test_delta_tables()
