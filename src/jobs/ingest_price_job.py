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


class PriceIngestion:
    """Optimized price data ingestion with enhanced error handling and performance"""
    
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
        endpoint = kwargs.get('endpoint', 'price')
        interval = kwargs.get('interval', self._schedule_config.get('public_power_interval_minutes', 15))
        
        # Validate country code
        valid_countries = ['de', 'AT']  # Add more as needed
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
    
    def _validate_date_range(self, start_date: str, end_date: str) -> tuple:
        """Validate and parse date range for bulk loads"""
        
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(f"Invalid date format. Expected YYYY-MM-DD: {str(e)}")
        
        if start_date_obj > end_date_obj:
            raise ValueError(f"Start date {start_date} cannot be after end date {end_date}")
        
        # Adjust end date if it's in the future (price data typically has 1-day lag)
        now = datetime.now(pytz.utc)
        max_date = now.date() - timedelta(days=1)
        
        if end_date_obj > max_date:
            self.logger.warning(f"End date {end_date_obj} adjusted to {max_date} (price data has 1-day lag)")
            end_date_obj = max_date
        
        return start_date_obj, end_date_obj
    
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
    
    def _log_ingestion_metrics(self, start_time: datetime, success: bool, 
                              country: str, date_range: str, data_size: Optional[int] = None) -> None:
        """Log comprehensive ingestion metrics"""
        
        end_time = datetime.now(pytz.utc)
        duration = (end_time - start_time).total_seconds()
        
        metrics = {
            'job_id': self.job_id,
            'endpoint': 'price',
            'country': country,
            'date_range': date_range,
            'success': success,
            'duration_seconds': duration,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat()
        }
        
        if data_size is not None:
            metrics['data_records'] = data_size
            metrics['records_per_second'] = data_size / duration if duration > 0 else 0
        
        self.logger.info(f"Ingestion metrics: {metrics}")
    
    def _run_bulk_load(self, **kwargs) -> bool:
        """Run bulk load for historical price data"""
        
        start_date = kwargs.get('start_date', self._default_params.get('start_date'))
        end_date = kwargs.get('end_date', self._default_params.get('end_date'))
        country = kwargs.get('country', self._default_params.get('country', 'de'))
        endpoint = kwargs.get('endpoint', 'price')
        
        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required for bulk load")
        
        start_date_obj, end_date_obj = self._validate_date_range(start_date, end_date)
        
        self.logger.info(f"Bulk load: {start_date_obj} to {end_date_obj}")
        
        current_date = start_date_obj
        total_success = 0
        total_attempts = 0
        
        ingestion_job = self._create_ingestion_job(endpoint, country, current_date)
        
        while current_date <= end_date_obj:
            day_start_time = datetime.now(pytz.utc)
            total_attempts += 1
            
            try:
                self.logger.info(f"Processing endpoint: {endpoint}, date: {current_date}")
                
                api_data = self._get_api_data_with_retry(
                    endpoint, str(current_date), str(current_date), country
                )
                
                if api_data:
                    data_size = len(api_data) if isinstance(api_data, list) else None
                    self.logger.info(f"Data found for {current_date}, size: {data_size} records" if data_size else f"Data found for {current_date}")
                    
                    ingestion_job.run(api_data)
                    total_success += 1
                    self.logger.info(f"Successfully ingested data for {current_date}")
                    self._log_ingestion_metrics(day_start_time, True, country, str(current_date), data_size)
                else:
                    self.logger.warning(f"No data found for {current_date}")
                    self._log_ingestion_metrics(day_start_time, False, country, str(current_date))
                    
            except Exception as e:
                self.logger.error(f"Error ingesting data for {current_date}: {str(e)}")
                self._log_ingestion_metrics(day_start_time, False, country, str(current_date))
                
            current_date += timedelta(days=1)
        
        success_rate = total_success / total_attempts if total_attempts > 0 else 0
        self.logger.info(f"Bulk load completed: {total_success}/{total_attempts} days successful ({success_rate:.1%})")
        
        return success_rate > 0.8  # Consider successful if 80%+ of days processed
    
    def _run_incremental_load(self, **kwargs) -> bool:
        """Run incremental load for recent price data (previous day)"""
        
        params = self._validate_inputs(**kwargs)
        country = params['country']
        endpoint = params['endpoint']
        interval = params['interval']  # Used for logging context, but price data is daily
        
        # Get current UTC time and calculate previous day
        now = datetime.now(pytz.utc)
        previous_day = now.date() - timedelta(days=1)
        
        date_range = f"{previous_day}"
        self.logger.info(f"Incremental load: previous day {previous_day} (interval context: {interval} minutes)")
        
        ingestion_job = self._create_ingestion_job(endpoint, country, previous_day)
        
        try:
            api_data = self._get_api_data_with_retry(
                endpoint, previous_day.isoformat(), previous_day.isoformat(), country
            )
            
            if api_data:
                data_size = len(api_data) if isinstance(api_data, list) else None
                self.logger.info(f"Data found for {previous_day}, size: {data_size} records" if data_size else f"Data found for {previous_day}")
                
                ingestion_job.run(api_data)
                self.logger.info("Successfully ingested incremental price data")
                return True
            else:
                self.logger.warning("No data found for the previous day")
                return False
                
        except Exception as e:
            self.logger.error(f"Error ingesting incremental price data: {str(e)}")
            raise
    
    def run_ingestion(self, bulk_load_flag: bool = False, **kwargs) -> bool:

        start_time = datetime.now(pytz.utc)
        
        try:
            self.logger.info(f"=== Price Ingestion Job Started ===")
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
            self.logger.error(f"Unexpected error in price ingestion: {str(e)}")
            raise


def ingest_price_job(spark, logger: Logger, job_id: str, connector, 
                    config: Dict[str, Any], bulk_load_flag: bool = False, **kwargs) -> bool:

    # Create optimizer instance and run ingestion
    optimizer = PriceIngestion(spark, logger, job_id, connector, config)
    return optimizer.run_ingestion(bulk_load_flag, **kwargs)