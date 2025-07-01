# Energy-Charts API Documentation

## Overview
**Base URL**: `https://api.energy-charts.info`  
**Version**: 1.4  
**Type**: Public REST API (no authentication required)  
**Documentation**: [Swagger UI](https://api.energy-charts.info/#/)

---

## Endpoints

### 1. Public Power (`/public_power`)
**Purpose**: Real-time electricity production by energy source  
**Frequency**: 15-minute intervals  
**Units**: Megawatts (MW)

**Request**:
```http
GET /public_power?country=de&start=2024-01-01&end=2024-01-02
```

**Response**:
```json
{
  "unix_seconds": [1704067200, 1704067800, ...],
  "production_types": [
    {"name": "Solar", "data": [1500.5, 1450.2, ...]},
    {"name": "Wind offshore", "data": [2300.8, 2410.1, ...]}
  ]
}
```

### 2. Price Data (`/price`)
**Purpose**: Electricity market pricing  
**Frequency**: Hourly intervals  
**Units**: EUR/MWh (can be negative)

**Request**:
```http
GET /price?country=de&start=2024-01-01&end=2024-01-02
```

**Response**:
```json
{
  "unix_seconds": [1704067200, 1704070800, ...],
  "price": [45.67, 52.31, -5.12, ...],
  "unit": ["E", "U", "R", ...]
}
```

### 3. Installed Power (`/installed_power`)
**Purpose**: Power generation capacity by facility type  
**Frequency**: Monthly updates  
**Units**: Megawatts (MW)  
**Coverage**: 2002-present

**Request**:
```http
GET /installed_power?country=de&time_step=monthly
```

**Response**:
```json
{
  "time": ["01.2002", "02.2002", ...],
  "production_types": [
    {"name": "Solar gross", "data": [0.0, 0.1, ...]},
    {"name": "Wind onshore", "data": [8500.5, 8600.2, ...]}
  ]
}
```

---

## Request Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `country` | ✓ | ISO country code | `de` (Germany) |
| `start` | ✓* | Start date | `2024-01-01` |
| `end` | ✓* | End date | `2024-01-02` |
| `time_step` | ✓** | Aggregation level | `monthly` |

*Required for public_power and price endpoints  
**Required for installed_power endpoint

---

## Data Notes

### Production Types
- **Renewables**: Solar, Wind (onshore/offshore), Hydro, Biomass
- **Fossil**: Coal, Gas, Oil, Nuclear
- **Storage**: Battery Storage, Pumped Hydro
- **Special**: Load, Residual load, Cross-border trading

### Data Quality
- **Missing Values**: Some data points may be null
- **Negative Prices**: Indicate electricity oversupply
- **Timezone**: All timestamps in UTC
- **Historical Gaps**: Early years may have incomplete data

---

## Integration Guide

### Best Practices
```yaml
Rate Limiting: 1-2 requests/second
Date Ranges: Limit to 1-7 days for frequent data
Error Handling: Implement retry with exponential backoff
Validation: Check response structure before processing
```

### Pipeline Usage
1. **Bronze Layer**: Store raw API responses
2. **Data Quality**: Validate response structure and data ranges
3. **Monitoring**: Track API response times and error rates
4. **Lineage**: Log API version and request metadata

### Error Codes
| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Invalid parameters |
| 404 | Endpoint not found |
| 500 | Server error | 