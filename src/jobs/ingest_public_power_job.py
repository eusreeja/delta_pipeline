import sys
import os
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import pytz

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.pipelines.data_ingestion import DataIngestionJob
from src.logger.custom_logger import Logger


class PublicPowerIngestion:
    """Optimized public power data ingestion with enhanced error handling and performance"""
    
    def __init__(self, spark, logger: Logger, job_id: str, connector, config: Dict[str, Any]):
        self.spark = spark
        self.logger = logger
        self.job_id = job_id
        self.connector = connector
        self.config = config
        
        # Cache frequently accessed config values
        self._default_params = config.get('energy_charts_api', {}).get('default_params', {})
        self._schedule_config = config.get('energy_charts_api', {}).get('schedule', {})
        self._retry_config = config.get('retry', {'max_attempts': 3, 'delay_seconds': 5})
        
    def _validate_inputs(self, **kwargs) -> Dict[str, Any]:
        """Validate and normalize input parameters"""
        
        # Extract and validate parameters
        country = kwargs.get('country', self._default_params.get('country', 'de'))
        endpoint = kwargs.get('endpoint', 'public_power')
        interval = kwargs.get('interval', self._schedule_config.get('public_power_interval_minutes', 15))
        
        # Validate country code
        valid_countries = ['de', 'fr', 'at', 'ch']  # Add more as needed
        if country not in valid_countries:
            self.logger.warning(f"Country '{country}' not in validated list, proceeding anyway")
        
        # Validate interval
        if not isinstance(interval, int) or interval <= 0:
            raise ValueError(f"Invalid interval '{interval}'. Must be a positive integer")
        
        return {
            'country': country,
            'endpoint': endpoint,
            'interval': interval
        }
    
    
    def _get_api_data_with_retry(self, endpoint: str, start_date: str, end_date: str, country: str) -> Optional[Any]:
        """Get API data with retry logic and error handling"""
        
        max_attempts = self._retry_config.get('max_attempts', 3)
        delay_seconds = self._retry_config.get('delay_seconds', 5)
        
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"API call attempt {attempt + 1}/{max_attempts} for {start_date} to {end_date}")
                
                api_data = self.connector.get_api_data(
                    endpoint=endpoint,
                    start_date=start_date,
                    end_date=end_date,
                    country=country
                )
                
                if api_data:
                    self.logger.info(f"API call successful on attempt {attempt + 1}")
                    return api_data
                else:
                    self.logger.warning(f"No data returned on attempt {attempt + 1}")
                    
            except Exception as e:
                self.logger.error(f"API call failed on attempt {attempt + 1}: {str(e)}")
                
                if attempt < max_attempts - 1:
                    self.logger.info(f"Retrying in {delay_seconds} seconds...")
                    import time
                    time.sleep(delay_seconds)
                else:
                    self.logger.error(f"All {max_attempts} API call attempts failed")
                    raise
        
        return None
    
    def _create_ingestion_job(self, endpoint: str, country: str, current_date) -> DataIngestionJob:
        """Create and configure ingestion job"""
        
        ingestion_job = DataIngestionJob(
            spark=self.spark,
            logger=self.logger,
            job_id=self.job_id,
            config=self.config,
            endpoint=endpoint,
            current_date=current_date,
            country=country
        )
        
        return ingestion_job
    

    
    def _run_bulk_load(self, **kwargs) -> bool:
        """Run bulk load for historical data"""
        
        start_date = kwargs.get('start_date', self._default_params.get('start_date'))
        end_date = kwargs.get('end_date', self._default_params.get('end_date'))
        country = kwargs.get('country', self._default_params.get('country', 'de'))
        endpoint = kwargs.get('endpoint', 'public_power')
        
        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required for bulk load")
        
        self.logger.info(f"Bulk load: {start_date} to {end_date}")
        
        current_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        
        ingestion_job = self._create_ingestion_job(endpoint, country, current_date)
        
        while current_date <= datetime.strptime(end_date, "%Y-%m-%d").date():
            day_start_time = datetime.now(pytz.utc)
            
            try:
                self.logger.info(f"Processing date: {current_date}")
                
                api_data = self._get_api_data_with_retry(
                    endpoint, str(current_date), str(current_date), country
                )
                
                if api_data:
                    data_size = len(api_data) if isinstance(api_data, list) else None
                    self.logger.info(f"Data found for {current_date}, size: {data_size} records" if data_size else "Data found for {current_date}")
                    
                    ingestion_job.run(api_data)
                    total_success += 1
                    self.logger.info(f"Successfully ingested data for {current_date}")
                   
                else:
                    self.logger.warning(f"No data found for {current_date}")
                   
                    
            except Exception as e:
                self.logger.error(f"Error ingesting data for {current_date}: {str(e)}")
                
                
            current_date += timedelta(days=1)
        
        self.logger.info(f"Bulk load completed: days successful")
        
        return True
    def _run_incremental_load(self, **kwargs) -> bool:
        """Run incremental load for recent data"""
        
        params = self._validate_inputs(**kwargs)
        country = params['country']
        endpoint = params['endpoint']
        interval = params['interval']
        
        # Get current UTC time
        now = datetime.now(pytz.utc)
        
        # Round down to the last interval mark
        rounded_minute = (now.minute // interval) * interval
        start_time = now.replace(minute=rounded_minute, second=0, microsecond=0)
        
        # The interval we're interested in is the **previous** interval block
        end_time = start_time
        start_time = end_time - timedelta(minutes=interval)
        
        date_range = f"{start_time.isoformat()} to {end_time.isoformat()}"
        self.logger.info(f"Incremental load: last {interval} minutes from {start_time} to {end_time}")
        
        ingestion_job = self._create_ingestion_job(endpoint, country, start_time.date())
        
        try:
            api_data = self._get_api_data_with_retry(
                endpoint, start_time.isoformat(), end_time.isoformat(), country
            )
            
            if api_data:
                data_size = len(api_data) if isinstance(api_data, list) else None
                self.logger.info(f"Data found for last {interval} minutes, size: {data_size} records" if data_size else f"Data found for last {interval} minutes")
                
                ingestion_job.run(api_data)
                self.logger.info("Successfully ingested incremental data")
                return True
            else:
                self.logger.warning("No data found for the last interval")
                return False
                
        except Exception as e:
            self.logger.error(f"Error ingesting incremental data: {str(e)}")
            raise
    
    def run_ingestion(self, bulk_load_flag: bool = False, **kwargs) -> bool:
        """Run the public power ingestion job"""
        
        start_time = datetime.now(pytz.utc)
        
        try:
            self.logger.info(f"=== Public Power Ingestion Job Started ===")
            self.logger.info(f"Mode: {'Bulk Load' if bulk_load_flag else 'Incremental'}")
            
            if bulk_load_flag:
                success = self._run_bulk_load(**kwargs)
            else:
                success = self._run_incremental_load(**kwargs)
            
            return success
                
        except ValueError as ve:
            self.logger.error(f"Input validation error: {str(ve)}")
            raise
            
        except Exception as e:
            self.logger.error(f"Unexpected error in public power ingestion: {str(e)}")
            raise


def ingest_public_power_job(spark, logger: Logger, job_id: str, connector, 
                           config: Dict[str, Any], bulk_load_flag: bool = False, **kwargs) -> bool:
    # Create optimizer instance and run ingestion
    optimizer = PublicPowerIngestion(spark, logger, job_id, connector, config)
    return optimizer.run_ingestion(bulk_load_flag, **kwargs)