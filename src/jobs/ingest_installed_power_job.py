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


class InstalledPowerIngestion:
    """Optimized installed power data ingestion with enhanced error handling and performance"""
    
    def __init__(self, spark, logger: Logger, job_id: str, connector, config: Dict[str, Any]):
        self.spark = spark
        self.logger = logger
        self.job_id = job_id
        self.connector = connector
        self.config = config
        
        # Cache frequently accessed config values
        self._default_params = config.get('energy_charts_api', {}).get('default_params', {})
        self._retry_config = config.get('retry', {'max_attempts': 3, 'delay_seconds': 5})
        
    def _validate_inputs(self, **kwargs) -> Dict[str, Any]:
        """Validate and normalize input parameters"""
        
        # Extract and validate parameters
        country = kwargs.get('country', self._default_params.get('country', 'de'))
        endpoint = kwargs.get('endpoint', 'installed_power')
        time_step = kwargs.get('time_step', 'monthly')
        
        # Validate country code
        valid_countries = ['de', 'fr', 'at', 'ch']  # Add more as needed
        if country not in valid_countries:
            self.logger.warning(f"Country '{country}' not in validated list, proceeding anyway")
        
        # Validate time step
        valid_time_steps = ['monthly', 'yearly']
        if time_step not in valid_time_steps:
            raise ValueError(f"Invalid time_step '{time_step}'. Must be one of: {valid_time_steps}")
        
        return {
            'country': country,
            'endpoint': endpoint,
            'time_step': time_step
        }
    
    def _get_api_data_with_retry(self, endpoint: str, time_step: str, country: str) -> Optional[Any]:
        """Get API data with retry logic and error handling"""
        
        max_attempts = self._retry_config.get('max_attempts', 3)
        delay_seconds = self._retry_config.get('delay_seconds', 5)
        
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"API call attempt {attempt + 1}/{max_attempts}")
                
                api_data = self.connector.get_api_data(
                    endpoint=endpoint,
                    start_date='',  # Installed power doesn't use date ranges
                    end_date='',
                    time_step=time_step,
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
    
    def _create_ingestion_job(self, endpoint: str, country: str) -> DataIngestionJob:
        """Create and configure ingestion job"""
        
        # Use current UTC date for installed power data
        now = datetime.now(pytz.utc)
        current_date = now.date().isoformat()
        
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
    
    def run_ingestion(self, bulk_load_flag: bool = False, **kwargs) -> bool:
        """Run the installed power ingestion job"""
        
        start_time = datetime.now(pytz.utc)
        success = False
        
        try:
            # Validate inputs
            params = self._validate_inputs(**kwargs)
            country = params['country']
            endpoint = params['endpoint']
            time_step = params['time_step']
            
            self.logger.info(f"=== Installed Power Ingestion Job Started ===")
            self.logger.info(f"Parameters: country={country}, time_step={time_step}, bulk_load={bulk_load_flag}")
            
            # Note: bulk_load_flag is not applicable for installed power as it's monthly capacity data
            if bulk_load_flag:
                self.logger.info("Note: bulk_load_flag ignored for installed power (monthly capacity data)")
            
            # Create ingestion job
            ingestion_job = self._create_ingestion_job(endpoint, country)
            self.logger.info(f"Ingestion job created for {endpoint} with country {country}")
            
            # Get API data with retry logic
            self.logger.info(f"Fetching installed power data for country {country} with time_step {time_step}")
            api_data = self._get_api_data_with_retry(endpoint, time_step, country)
            
            if api_data:
                # Determine data size for metrics
                data_size = len(api_data) if isinstance(api_data, list) else None
                
                self.logger.info(f"Data found for {time_step}, proceeding with ingestion")
                self.logger.info(f"Data size: {data_size} records" if data_size else "Data size: unknown")
                
                # Run ingestion
                ingestion_job.run(api_data)
                
                success = True
                self.logger.info(f"Successfully ingested installed power data")
                
            else:
                self.logger.warning(f"No data found for {endpoint} and country {country}")
                
        except ValueError as ve:
            self.logger.error(f"Input validation error: {str(ve)}")
            raise
            
        except Exception as e:
            self.logger.error(f"Unexpected error in installed power ingestion: {str(e)}")
            raise
        
        return success


def ingest_installed_power_job(spark, logger: Logger, job_id: str, connector, 
                              config: Dict[str, Any], bulk_load_flag: bool = False, **kwargs) -> bool:
    
    # Create optimizer instance and run ingestion
    optimizer = InstalledPowerIngestion(spark, logger, job_id, connector, config)
    return optimizer.run_ingestion(bulk_load_flag, **kwargs)