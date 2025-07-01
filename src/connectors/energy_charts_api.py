from src.logger.custom_logger import Logger
from typing import Dict, Any
import json
import requests
# Add retry decorator for API calls
from tenacity import retry, stop_after_attempt, wait_exponential

class EnergyChartsConnector:
    
    def __init__(self, endpoint:str, logger: Logger, config: Dict[str, Any]):
        """Initialize the EnergyChartsConnector with configuration and logger. """
        self.logger = logger
        self.logger.info("Initializing EnergyChartsConnector")

        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "BayWa-re-DataPipeline/1.0"
        })
        self.base_url = config['energy_charts_api']['base_url']
        self.logger.info(f"Base URL set to {self.base_url}")
        self.endpoints = endpoint
        self.logger.info(f"Endpoints configured: {self.endpoints}")
        self.timeout = config['energy_charts_api']['request_timeout']
        self.logger.info(f"Default timeout parameters: {self.timeout}")
        
        if not all([self.base_url, self.timeout, self.endpoints]):
            self.logger.error("Invalid configuration: base_url, timeout, or endpoints are not set")
            self.logger.debug(f"Configuration: base_url={self.base_url}, timeout={self.timeout}, endpoints={self.endpoints}")
            raise ValueError("Invalid configuration")
 


    @retry(stop=stop_after_attempt(3),wait=wait_exponential(multiplier=1, min=4, max=10))
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """Helper method to make API requests"""
        self.logger.info(f"Connector: Making request to {endpoint} with params: {params}")
        try:
            response = self._session.get(f"{self.base_url}/{endpoint}", params=params)
            response.raise_for_status()
            self.logger.info(f"Connector: Received response from {endpoint}: {response.status_code}")
            return response.json()  # This should return a dictionary
        
        except json.JSONDecodeError:
            self.logger.error("API response is not valid JSON")
            self.logger.debug(f"Response content: {response.text}")
            raise ValueError("API returned invalid JSON")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            self.logger.debug(f"Request params: {params}")
            self.logger.debug(f"Response content: {response.text if 'response' in locals() else 'No response'}")
            raise ConnectionError(f"API request failed: {str(e)}")  

    def get_api_data(self, endpoint: str, start_date: str = None, end_date: str = None, time_step: str = None, country: str = "de") -> Dict:
        """Get public net electricity production data"""
        self.logger.info(f"Connector: Fetching API data from endpoint: {endpoint} for {country} from {start_date} to {end_date}")
        try:
            if endpoint == "installed_power":
                return self._make_request(endpoint, {
                    "country": country,
                    "time_step": time_step
                })
            if endpoint == "price" or endpoint == "public_power":
                return self._make_request(endpoint, {
                    "country": country,
                    "start": start_date,
                    "end": end_date
                }) 
        except Exception as e:  
            # Log the raw API data for debugging
            self.logger.error(f"Connector: Failed to fetch json data from API: {str(e)}")
            raise Exception(f"Connector: Failed to ingest json data from API: {str(e)}")
        
    def close(self):
        """Close the HTTP session"""
        self.logger.info("Closing HTTP session")
        self._session.close()