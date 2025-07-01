"""
Simple BI/ML Analytics Module - Technical Challenge POC
Basic implementation of three core energy data queries
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *


class EnergyDataAnalytics:
    """Simple analytics class for energy data - POC level"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def execute_daily_production_trends(self, start_date=None, end_date=None, country='de'):
        """Query 1: Daily electricity production trends by production type"""
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND DATE(CONCAT(f.year, '-', LPAD(f.month,2,'0'), '-', LPAD(f.day,2,'0'))) BETWEEN '{start_date}' AND '{end_date}'"
        
        query = f"""
        SELECT
          f.year,
          f.month,
          f.day,
          d.production_plant_name AS production_type,
          SUM(f.electricity_produced) AS total_daily_production
        FROM gold_fact_power f
        JOIN gold_dim_production_type d ON f.production_type_id = d.production_type_id
        WHERE f.country = '{country}'
          {date_filter}
        GROUP BY f.year, f.month, f.day, d.production_plant_name
        ORDER BY f.year, f.month, f.day, d.production_plant_name
        """
        
        return self.spark.sql(query)
    
    def execute_underperformance_prediction_features(self, start_date=None, end_date=None, country='de'):
        """Query 2: ML features for underperformance prediction"""
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND f.timestamp_30min BETWEEN '{start_date}' AND '{end_date}'"
        
        query = f"""
        SELECT
            f.timestamp_30min,
            f.production_type_id,
            d.production_plant_name,
            d.energy_category,
            d.controllability_type,
            f.total_electricity_produced,
            f.year,
            f.month,
            f.day,
            f.hour,
            f.minute_interval_30,
            
            LAG(f.total_electricity_produced, 48) OVER (
                PARTITION BY f.production_type_id 
                ORDER BY f.timestamp_30min
            ) AS lag_1d,

            LAG(f.total_electricity_produced, 336) OVER (
                PARTITION BY f.production_type_id 
                ORDER BY f.timestamp_30min
            ) AS lag_1w,

            AVG(f.total_electricity_produced) OVER (
                PARTITION BY f.production_type_id, f.hour, f.minute_interval_30
                ORDER BY f.timestamp_30min
                RANGE BETWEEN 336 PRECEDING AND 1 PRECEDING
            ) AS rolling_7d_avg
        FROM gold_fact_power_30min_agg f
        JOIN gold_dim_production_type d ON f.production_type_id = d.production_type_id
        WHERE f.country = '{country}'
        AND d.active_flag = TRUE
        {date_filter}
        ORDER BY f.timestamp_30min DESC, f.production_type_id
        """
        
        return self.spark.sql(query)
    
    def execute_wind_price_analysis(self, start_date=None, end_date=None, country='de'):
        """Query 3: Wind power vs price analysis"""
        
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND DATE(CONCAT(f.year, '-', LPAD(f.month,2,'0'), '-', LPAD(f.day,2,'0'))) BETWEEN '{start_date}' AND '{end_date}'"
        
        query = f"""
        SELECT
          f.year,
          f.month,
          f.day,
          d.production_plant_name AS production_type,
          SUM(f.electricity_produced) AS total_daily_production_mw,
          AVG(f.electricity_price) AS avg_daily_price_eur_per_mwh
        FROM gold_fact_power f
        JOIN gold_dim_production_type d ON f.production_type_id = d.production_type_id
        WHERE f.country = '{country}'
          AND d.production_plant_name IN ('Wind_Offshore', 'Wind_Onshore') 
          AND d.active_flag = TRUE
          {date_filter}
        GROUP BY f.year, f.month, f.day, d.production_plant_name
        ORDER BY f.year, f.month, f.day, d.production_plant_name
        """
        
        return self.spark.sql(query)
    
    def execute_all_reports(self, start_date=None, end_date=None, country='de'):
        """Execute all three queries and return results"""
        
        results = {
            'daily_production_trends': self.execute_daily_production_trends(start_date, end_date, country),
            'underperformance_prediction_features': self.execute_underperformance_prediction_features(start_date, end_date, country),
            'wind_price_analysis': self.execute_wind_price_analysis(start_date, end_date, country)
        }
        
        return results
    
    def get_query_templates(self):
        """Get the SQL query templates"""
        
        queries = {
            'daily_production_trends': """
                SELECT
                  f.year,
                  f.month,
                  f.day,
                  d.production_plant_name AS production_type,
                  SUM(f.electricity_produced) AS total_daily_production
                FROM gold_fact_power f
                JOIN gold_dim_production_type d ON f.production_type_id = d.production_type_id
                WHERE f.country = 'de'
                GROUP BY f.year, f.month, f.day, d.production_plant_name
                ORDER BY f.year, f.month, f.day, d.production_plant_name
            """,
            
            'underperformance_prediction_features': """
                SELECT
                    f.timestamp_30min,
                    f.production_type_id,
                    d.production_plant_name,
                    d.energy_category,
                    d.controllability_type,
                    f.total_electricity_produced,
                    f.year, f.month, f.day, f.hour, f.minute_interval_30,
                    LAG(f.total_electricity_produced, 48) OVER (PARTITION BY f.production_type_id ORDER BY f.timestamp_30min) AS lag_1d,
                    LAG(f.total_electricity_produced, 336) OVER (PARTITION BY f.production_type_id ORDER BY f.timestamp_30min) AS lag_1w,
                    AVG(f.total_electricity_produced) OVER (
                        PARTITION BY f.production_type_id, f.hour, f.minute_interval_30
                        ORDER BY f.timestamp_30min
                        RANGE BETWEEN 336 PRECEDING AND 1 PRECEDING
                    ) AS rolling_7d_avg
                FROM gold_fact_power_30min_agg f
                JOIN gold_dim_production_type d ON f.production_type_id = d.production_type_id
                WHERE f.country = 'de' AND d.active_flag = TRUE
            """,
            
            'wind_price_analysis': """
                SELECT
                  f.year, f.month, f.day,
                  d.production_plant_name AS production_type,
                  SUM(f.electricity_produced) AS total_daily_production_mw,
                  AVG(f.electricity_price) AS avg_daily_price_eur_per_mwh
                FROM gold_fact_power f
                JOIN gold_dim_production_type d ON f.production_type_id = d.production_type_id
                WHERE f.country = 'de'
                  AND d.production_plant_name IN ('Wind_Offshore', 'Wind_Onshore') 
                  AND d.active_flag = TRUE 
                GROUP BY f.year, f.month, f.day, d.production_plant_name
                ORDER BY f.year, f.month, f.day, d.production_plant_name
            """
        }
        
        return queries
    
    def validate_data_quality(self, df, query_name):
        """Simple data quality check"""
        
        total_rows = df.count()
        
        if total_rows == 0:
            return {
                'query_name': query_name,
                'total_rows': 0,
                'status': 'EMPTY_RESULT',
                'issues': ['No data returned']
            }
        
        issues = []
        
        # Basic null checks
        if query_name == 'daily_production_trends':
            null_production = df.filter(col("total_daily_production").isNull()).count()
            if null_production > 0:
                issues.append(f"Found {null_production} rows with null production values")
        
        elif query_name == 'underperformance_prediction_features':
            null_features = df.filter(col("total_electricity_produced").isNull()).count()
            if null_features > 0:
                issues.append(f"Found {null_features} rows with null electricity production")
        
        elif query_name == 'wind_price_analysis':
            null_prices = df.filter(col("avg_daily_price_eur_per_mwh").isNull()).count()
            if null_prices > 0:
                issues.append(f"Found {null_prices} rows with null price values")
        
        status = 'PASSED' if len(issues) == 0 else 'WARNING'
        
        return {
            'query_name': query_name,
            'total_rows': total_rows,
            'status': status,
            'issues': issues
        }


def create_spark_session(app_name="reports"):
    """Create simple Spark session"""
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    return spark


# Simple usage example
if __name__ == "__main__":
    spark = create_spark_session('reports')
    report = EnergyDataAnalytics()
    report.execute_all_reports(start_date="2025-01-01", end_date="2025-01-02", country='de')
