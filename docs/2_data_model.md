# This document contails the Data model design details


1. Bronze Tables

Table Name 1 : bronze_public_power
Description: Raw data table containing public electricity production data from various energy sources. Data is ingested at 15-minute intervals from external APIs for real-time energy monitoring and analysis.

Columns: 
- record_id: STRING - Unique identifier for each record (UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
- timestamp: TIMESTAMP - Converted timestamp from unix_seconds for human-readable datetime representation
- unix_seconds: BIGINT - Original Unix timestamp from source system for precise time tracking and traceability
- production_type: STRING - Type of electricity generation plant (e.g., 'solar', 'wind', 'nuclear', 'coal', 'gas', 'hydro')
- electricity_generated: DOUBLE - Quantity of electricity produced in megawatts (MW) or gigawatts (GW)
- country: STRING - The country code (e.g., 'de' for Germany)
- data_source: STRING - Source system type for example api', 'file', 'database'
- source_system: STRING - Specific name of the source system (e.g., 'energy_charts_api', 'entsoe_api')
- ingestion_batch_id: STRING - The data ingestion batch for tracking and debugging
- ingestion_date: TIMESTAMP - UTC timestamp when the record was inserted into the bronze table


Table Name 2 : bronze_price
Description: Raw data table containing electricity pricing data from energy markets. Data is ingested at 15-minute intervals from external APIs for real-time energy price monitoring and market analysis.

Columns: 
- record_id: STRING - Unique identifier for each record (UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
- timestamp: TIMESTAMP - Converted timestamp from unix_seconds for human-readable datetime representation
- unix_seconds: BIGINT - Original Unix timestamp from source system for precise time tracking and traceability
- electricity_price: DECIMAL(10,4) - Price of electricity in EUR/MWh or local currency per unit
- electricity_unit: The local currency unit for the price.
- country: STRING - The country code (e.g., 'de' for Germany)
- data_source: STRING - Source system type for example api', 'file', 'database'
- source_system: STRING - Specific name of the source system (e.g., 'energy_charts_api', 'market_api')
- ingestion_batch_id: STRING - The data ingestion batch identifier for tracking and debugging
- ingestion_date: TIMESTAMP - UTC timestamp when the record was inserted into the bronze table


Table Name 3 : bronze_installed_power
Description: Raw data table containing installed power capacity data from various energy production facilities. Data represents the maximum power generation capacity of different electricity production plants and is typically updated monthly from external APIs for infrastructure capacity monitoring and planning.

Columns: 
- record_id: STRING - Unique identifier for each record (UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
- timestamp: TIMESTAMP - Converted to timestamp from time field for datetime representation.
- time_period: STRING - Original time period from source system in format "MM.YYYY" for traceability
- production_type: STRING - Type of electricity generation plant (e.g., 'solar', 'wind', 'nuclear', 'coal', 'gas', 'hydro')
- installed_capacity: DOUBLE - installed power generation capacity (in gigawatts, GW) 
- country: STRING - The country code (e.g., 'de' for Germany)
- data_source: STRING - Source system type (e.g., 'api', 'file', 'database')
- source_system: STRING - Specific name of the source system (e.g., 'energy_charts_api', 'capacity_registry')
- ingestion_batch_id: STRING - The data ingestion batch identifier for tracking and debugging
- ingestion_date: TIMESTAMP - UTC timestamp when the record was inserted into the bronze table



2. Silver Tables

Table Name 1 : silver_public_power
Description: Cleansed and enriched public electricity production data with data quality checks, deduplication, and business logic applied. Contains 15-minute interval electricity generation data from various energy sources with enhanced data quality flags and lineage tracking.

Columns:
- record_id: STRING NOT NULL - Unique identifier for each silver record (UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
- business_key: STRING NOT NULL - Composite business key combining timestamp, production_type, and country for deduplication (format: YYYYMMDD_HH:MM_production_type_country)
- timestamp: TIMESTAMP NOT NULL - Standardized timestamp in UTC timezone from source unix_seconds for datetime representation
- unix_seconds: BIGINT NOT NULL - Original Unix timestamp from source system for precise time tracking and traceability
- production_type: STRING NOT NULL - Normalized type of electricity generation plant (e.g., 'Solar', 'Wind offshore', 'Fossil gas', 'Hydro Run-of-River')
- electricity_generated: DOUBLE NOT NULL - Quantity of electricity produced in megawatts (MW), validated and cleansed
- country: STRING NOT NULL - ISO country code (e.g., 'de' for Germany)
- data_source: STRING NOT NULL - Source system type ('api', 'file', 'database')
- source_system: STRING NOT NULL - Specific name of the source system (e.g., 'energy_charts_api', 'entsoe_api')
- dq_check_status: STRING NOT NULL - Data quality validation status ('VALID', 'WARNING', 'ERROR', 'QUARANTINED')
- dq_check_date: TIMESTAMP NOT NULL - UTC timestamp when data quality checks were performed
- dq_score: INTEGER - Data quality score from 1-5 (5=excellent, 1=poor quality)
- is_anomaly: BOOLEAN NOT NULL - Flag indicating if the record is identified as a statistical anomaly (true/false)
- bronze_record_id: STRING NOT NULL - Reference to the original bronze table record for lineage tracking
- silver_batch_id: STRING NOT NULL - Unique identifier for the silver transformation batch processing
- silver_ingestion_date: TIMESTAMP NOT NULL - UTC timestamp when the record was processed and inserted into the silver table
- created_by: STRING NOT NULL - Process identifier that created the silver record (e.g., 'bronze_to_silver_pipeline')
- last_updated: TIMESTAMP - UTC timestamp when the record was last modified (for SCD Type 1 updates)


Table Name 2 : silver_price
Description: Cleansed and enriched electricity pricing data with data quality checks and validation. Contains electricity market price information with enhanced lineage tracking and anomaly detection.

Columns:
- record_id: STRING NOT NULL - Unique identifier for each silver record (UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
- business_key: STRING NOT NULL - Composite business key combining timestamp and country for deduplication (format: YYYYMMDD_HH:MM_country)
- timestamp: TIMESTAMP NOT NULL - Standardized timestamp in UTC timezone from source unix_seconds
- unix_seconds: BIGINT NOT NULL - Original Unix timestamp from source system for precise time tracking and traceability
- electricity_price: DECIMAL(10,4) NOT NULL - Price of electricity in EUR/MWh or local currency per unit, validated and cleansed
- electricity_unit: STRING NOT NULL - The local currency unit for the price (e.g., 'EUR/MWh')
- country: STRING NOT NULL - ISO country code (e.g., 'de' for Germany)
- data_source: STRING NOT NULL - Source system type ('api', 'file', 'database')
- source_system: STRING NOT NULL - Specific name of the source system (e.g., 'energy_charts_api', 'market_api')
- dq_check_status: STRING NOT NULL - Data quality validation status ('VALID', 'WARNING', 'ERROR', 'QUARANTINED')
- dq_check_date: TIMESTAMP NOT NULL - UTC timestamp when data quality checks were performed
- dq_score: INTEGER - Data quality score from 1-5 (5=excellent, 1=poor quality)
- is_anomaly: BOOLEAN NOT NULL - Flag indicating if the price is identified as a statistical anomaly
- bronze_record_id: STRING NOT NULL - Reference to the original bronze table record for lineage tracking
- silver_batch_id: STRING NOT NULL - Unique identifier for the silver transformation batch processing
- silver_ingestion_date: TIMESTAMP NOT NULL - UTC timestamp when the record was processed and inserted into the silver table
- created_by: STRING NOT NULL - Process identifier that created the silver record
- last_updated: TIMESTAMP - UTC timestamp when the record was last modified


Table Name 3 : silver_installed_power
Description: Cleansed and enriched installed power capacity data with data quality validation. Contains monthly maximum power generation capacity information for different electricity production facilities.

Columns:
- record_id: STRING NOT NULL - Unique identifier for each silver record (UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
- business_key: STRING NOT NULL - Composite business key combining time_period, production_type, and country (format: MM.YYYY_production_type_country)
- timestamp: TIMESTAMP NOT NULL - Converted timestamp from time_period for standardized datetime representation
- time_period: STRING NOT NULL - Original time period from source system in format "MM.YYYY" for traceability (e.g., "01.2024")
- production_type: STRING NOT NULL - Normalized type of electricity generation plant (e.g., 'Solar gross', 'Wind onshore', 'Battery Storage (Capacity)')
- installed_capacity: DOUBLE NOT NULL - Maximum power generation capacity in megawatts (MW), validated and cleansed
- country: STRING NOT NULL - ISO country code (e.g., 'de' for Germany)
- data_source: STRING NOT NULL - Source system type ('api', 'file', 'database')
- source_system: STRING NOT NULL - Specific name of the source system (e.g., 'energy_charts_api', 'capacity_registry')
- dq_check_status: STRING NOT NULL - Data quality validation status ('VALID', 'WARNING', 'ERROR', 'QUARANTINED')
- dq_check_date: TIMESTAMP NOT NULL - UTC timestamp when data quality checks were performed
- dq_score: INTEGER - Data quality score from 1-5 (5=excellent, 1=poor quality)
- is_anomaly: BOOLEAN NOT NULL - Flag indicating if the capacity value is identified as a statistical anomaly
- bronze_record_id: STRING NOT NULL - Reference to the original bronze table record for lineage tracking
- silver_batch_id: STRING NOT NULL - Unique identifier for the silver transformation batch processing
- silver_ingestion_date: TIMESTAMP NOT NULL - UTC timestamp when the record was processed and inserted into the silver table
- created_by: STRING NOT NULL - Process identifier that created the silver record
- last_updated: TIMESTAMP - UTC timestamp when the record was last modified

3. Gold Layer Tables

Table Name 1: gold_dim_production_type
Description: Dimension table containing standardized and enriched metadata for electricity production types. 
Designed for SCD (slowly changing dimension) management and auditability.

Columns:
- production_type_id: STRING NOT NULL - Surrogate key for the production type record (UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
- production_plant_name: STRING NOT NULL - Standardized name of the electricity production type (e.g., 'Solar', 'Wind', 'Hydro', 'Nuclear')
- energy_category: STRING NOT NULL - High-level energy classification: 'Renewable' (e.g., solar, wind, hydro, biomass) or 'Non-Renewable' (e.g., nuclear, coal, gas, oil)
- controllability_type: STRING NOT NULL - Operational controllability: 'Weather_Dependent' (e.g., solar, wind)'Controllable' (e.g., hydro, nuclear, coal, gas)'Mixed' (for other types)
- description: STRING - Detailed description of the production type (e.g., 'Photovoltaic and solar thermal power', 'Onshore and offshore wind power', etc.)
- country: STRING NOT NULL
- Country name or ISO code (e.g., 'de' for Germany)
- effective_date: DATE NOT NULL - UTC date when this record became effective (start of validity)
- expiry_date: DATE - UTC date when this record expired or was superseded (end of validity); NULL if current
- active_flag: BOOLEAN NOT NULL - Indicates if the production type record is currently active (true) or inactive (false)
- created_at: TIMESTAMP NOT NULL - UTC timestamp when the record was created in the dimension table
- updated_at: TIMESTAMP - UTC timestamp when the record was last updated

Table Name 2: gold_fact_power
Description:
Fact table storing detailed electricity production data at fine-grained time intervals. This table captures production quantities and prices linked to production types, supporting detailed analytics, reporting, and operational monitoring.

Columns:
record_id: STRING NOT NULL - Surrogate key for the fact record (UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
year: INTEGER NOT NULL - Calendar year extracted from the timestamp (e.g., 2025)
month: INTEGER NOT NULL - Calendar month extracted from the timestamp (1-12)
day: INTEGER NOT NULL - Calendar day extracted from the timestamp (1-31)
hour: INTEGER NOT NULL - Hour of day extracted from the timestamp (0-23)
minute: INTEGER NOT NULL - Minute of hour extracted from the timestamp (0-59)
minute_interval_30: INTEGER NOT NULL - 30-minute interval marker: 0 for minutes 0-14, 30 for minutes 15-59
timestamp_30min: TIMESTAMP NOT NULL - Normalized timestamp rounded/truncated to the start of the 30-minute interval
timestamp: TIMESTAMP NOT NULL - Original timestamp of the measurement
production_type_id: STRING NOT NULL - Foreign key referencing the production type dimension table
electricity_produced: DOUBLE NOT NULL - Quantity of electricity produced in megawatts (MW)
electricity_price: DOUBLE - Price of the electricity produced (e.g., EUR/MWh)
country: STRING NOT NULL - Country code or name (e.g., 'de' for Germany)
created_at: TIMESTAMP NOT NULL - UTC timestamp when the record was created
updated_at: TIMESTAMP - UTC timestamp when the record was last updated


Table Name 3: gold_fact_power_30min_agg
Description:
Fact table storing aggregated electricity production and price data at 30-minute intervals. Supports analytics for production trends, volatility, and price statistics, with references to the production type dimension. Designed for robust reporting and time-series analysis.

Columns:
record_id: STRING NOT NULL- Surrogate key for the fact record (UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
year: INTEGER NOT NULL - Calendar year extracted from the timestamp (e.g., 2025)
month: INTEGER NOT NULL- Calendar month extracted from the timestamp (1-12)
day: INTEGER NOT NULL - Calendar day extracted from the timestamp (1-31)
hour: INTEGER NOT NULL - Hour of day extracted from the timestamp (0-23)
minute: INTEGER NOT NULL - Minute of hour extracted from the timestamp (0-59)
minute_interval_30: INTEGER NOT NULL - 30-minute interval marker: 0 for minutes 0-29, 30 for minutes 30-59
timestamp_30min: TIMESTAMP NOT NULL - Normalized timestamp rounded/truncated to the start of the 30-minute interval
timestamp: TIMESTAMP NOT NULL Original timestamp of the measurement or aggregation
production_type_id: STRING NOT NULL - Foreign key referencing the production type dimension table
avg_electricity_produced: DOUBLE - Average electricity produced in the 30-minute interval (in MW)
total_electricity_produced: DOUBLE - Total electricity produced in the 30-minute interval (in MW)
min_electricity_produced: DOUBLE - Minimum electricity produced in the 30-minute interval (in MW)
max_electricity_produced: DOUBLE - Maximum electricity produced in the 30-minute interval (in MW)
production_volatility: DOUBLE - Standard deviation of electricity produced in the 30-minute interval (in MW)
data_points_count: INTEGER - Number of data points aggregated in the interval
avg_electricity_price: DOUBLE - Average electricity price in the 30-minute interval (e.g., EUR/MWh)
country: STRING NOT NULL - Country code or name (e.g., 'de' for Germany)
created_at: TIMESTAMP NOT NULL - UTC timestamp when the record was created
updated_at: TIMESTAMP - UTC timestamp when the record was last updated


