import os
import yaml
from pathlib import Path

# Determine environment
ENV = os.getenv("ENVIRONMENT", "dev")

# Load appropriate YAML config
config_path = Path(__file__).parent / "settings" / f"{ENV}.yaml"
with open(config_path) as f:
    EnvConfig = yaml.safe_load(f)

# Import other configurations
from .pipeline_config import EnergyChartsApiConfig
from .secrets.energy_charts import EnergyChartsSecrets

__all__ = ['EnvConfig', 'EnergyChartsSecrets', 'EnergyChartsApiConfig']