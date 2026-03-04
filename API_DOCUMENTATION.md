# GlideMate Backend - Complete API Reference

**Version**: 2.0.0 (Phase 3)  
**Last Updated**: March 4, 2026  
**Base URL**: `http://api.glidematev2.com` or `http://localhost:8000`

---

## 📋 API Overview

GlideMate Backend provides 44+ REST API endpoints organized into 10 main service modules:

| Module | Prefix | Endpoints | Purpose |
|--------|--------|-----------|---------|
| [Weather](#weather-api) | `/api/weather` | 10 | METAR, TAF, soundings, traffic |
| [Forecast](#forecast-api) | `/api/forecast` | 7 | KNMI/DWD forecasts, thermal, wind |
| [Parcel Analysis](#parcel-api) | `/api/parcel` | 3 | Atmospheric stability, CAPE, LI |
| [Nowcasting](#nowcast-api) | `/api/nowcast` | 5 | Real-time thermal, sunshine maps |
| [XC Planning](#xc-api) | `/api/xc` | 3 | Distance rings, safe landing spots |
| [Terrain](#terrain-api) | `/api/terrain` | 7 | Elevation, slope, ridge detection |
| [Safety](#safety-api) | `/api/safety` | 6 | NOTAMs, Föhn, thunderstorms |
| [Soaring](#soaring-api) | `/api/soaring` | 7 | Thermals, ridge lift, wave |
| [Diagnostics](#diagnostics-api) | `/api/diagnostics` | 3 | System health, metrics |
| [Elevation](#elevation-api) | `/api/elevation` | 3 | DEM data, batch elevation |

---

## 🌤️ Weather API

**Base Path**: `/api/weather`  
**METAR & TAF data** for airfield conditions  

### 1. Get METAR for Airfield
```
GET /api/weather/metar/{icao}
```
**Parameters:**
- `icao` (path, required) - ICAO code (e.g., "KORD", "EDDF")

**Response (200):**
```json
{
  "icao": "EDHI",
  "atis": "EDHI-ATIS-121.305",
  "metar": "EDHI 041250Z 32008KT 9999 FEW040 BKN100 12/10 Q1015",
  "taf": "EDHI 041500Z 3215/0518 32008KT P6000 FEW040",
  "wind_kt": 8,
  "visibility_km": 10,
  "temperature_c": 12,
  "dewpoint_c": 10,
  "altimeter_hpa": 1015,
  "ceiling_ft": 4000,
  "source": "aviation-weather",
  "updated_at": "2026-03-04T12:50:00Z"
}
```

### 2. Get TAF for Airfield
```
GET /api/weather/taf/{icao}
```
**Parameters:**
- `icao` (path, required) - ICAO code

**Response (200):**
```json
{
  "icao": "EDDF",
  "valid_from": "2026-03-04T12:00:00Z",
  "valid_to": "2026-03-05T12:00:00Z",
  "forecasts": [
    {
      "period": "0-6h",
      "wind": "270°/12kt",
      "visibility": "9999m",
      "weather": "FEW040 SCT100",
      "change": null
    }
  ]
}
```

### 3. Get METAR + TAF Combined
```
GET /api/weather/metar-taf/{icao}
```
**Response**: Combined METAR + TAF response

### 4. Get Sounding Data
```
GET /api/weather/sounding/{lat}/{lon}
```
**Parameters:**
- `lat` (path, required) - Latitude (-90 to 90)
- `lon` (path, required) - Longitude (-180 to 180)

**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "valid_time": "2026-03-04T12:00:00Z",
  "source": "NOAA-Skew-T",
  "levels": [
    {
      "pressure_hpa": 1000,
      "height_m": 100,
      "temperature_c": 15.2,
      "dewpoint_c": 9.1,
      "wind_dir": "240°",
      "wind_speed_ms": 4.1
    }
  ]
}
```

### 5. Get Atmospheric Indices
```
GET /api/weather/indices/{lat}/{lon}
```
**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "indices": {
    "cape": 450,
    "cin": -50,
    "lifted_index": 1.2,
    "k_index": 28,
    "total_totals": 48,
    "showalter_index": 0.5
  }
}
```

### 6. Get Weather Map Tiles
```
GET /api/weather/map/{overlay}/tile/{z}/{x}/{y}.png
```
**Parameters:**
- `overlay` - `radar`, `satellite`, `cloud`, `temperature`, `wind`
- `z` - Zoom level (0-18)
- `x`, `y` - Tile coordinates

**Response**: PNG map tile (256x256px)

### 7. Get Map Color Bar
```
GET /api/weather/map/{overlay}/colorbar.png
```
**Response**: Legend/colorbar image for overlay

### 8. Get Live Traffic
```
GET /api/weather/traffic/live
```
**Response (200):**
```json
{
  "aircraft": [
    {
      "callsign": "N123AB",
      "type": "PA28",
      "latitude": 50.15,
      "longitude": 8.62,
      "altitude_ft": 2500,
      "heading_deg": 210,
      "speed_kts": 95,
      "last_seen": "2026-03-04T12:45:30Z"
    }
  ],
  "count": 5
}
```

### 9. Get Aircraft Details
```
GET /api/weather/traffic/{callsign}
```
**Parameters:**
- `callsign` (path, required) - Aircraft callsign (e.g., "N123AB")

**Response (200):** Detailed aircraft position + flight plan

### 10. Get Service Status
```
GET /api/weather/status
```
**Response (200):**
```json
{
  "service": "weather",
  "status": "operational",
  "last_update": "2026-03-04T12:50:43Z",
  "data_sources": {
    "metar": "operational",
    "taf": "operational",
    "traffic": "operational"
  }
}
```

---

## 🌡️ Forecast API

**Base Path**: `/api/forecast`  
**Weather forecasts** with automatic regional model selection (KNMI/DWD)

### 1. Get Sounding Forecast
```
GET /api/forecast/sounding/{lat}/{lon}
```
**Parameters:**
- `lat` (path, required) - Latitude
- `lon` (path, required) - Longitude
- `include_bundle` (query, optional) - Include offline bundle info

**Response (200):**
```json
{
  "status": "success",
  "location": {"lat": 50.1, "lon": 8.5},
  "valid_at": "2026-03-04T12:00:00Z",
  "source": "knmi-harmonie",
  "model_used": "KNMI-HARMONIE-AROME",
  "data_age_minutes": 45,
  "levels": [
    {
      "pressure_hpa": 1000,
      "height_m": 100,
      "temperature_c": 15.2,
      "dewpoint_c": 9.1,
      "wind_direction_deg": 240,
      "wind_speed_ms": 4.1
    }
  ],
  "indices": {
    "cape": 1250,
    "lifted_index": -2.1,
    "k_index": 28.5,
    "total_totals": 48.2
  }
}
```

**Model Selection Logic:**
- **Priority Region** (47°N–57°N, 2°W–16°E): KNMI HARMONIE-AROME → fallback DWD ICON-EU
- **Outside Priority**: DWD ICON-EU → fallback KNMI
- **If both fail**: Cached data (up to 7 days old)

### 2. Get Stability Indices
```
GET /api/forecast/indices/{lat}/{lon}
```
**Response (200):** Lightweight indices (CAPE, LI, K-Index, etc.)

### 3. Get Thermal Forecast
```
GET /api/forecast/thermal/{lat}/{lon}
```
**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "valid_time": "2026-03-04T12:00:00Z",
  "thermal_strength": 3,
  "thermal_base_m": 1800,
  "thermal_top_m": 2500,
  "time_series": [
    {"hour": 12, "cape": 450, "strength": 2},
    {"hour": 13, "cape": 650, "strength": 3}
  ]
}
```

### 4. Get Wind Profile
```
GET /api/forecast/wind/{lat}/{lon}
```
**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "wind_layers": [
    {"altitude_m": 500, "direction_deg": 240, "speed_ms": 4.1},
    {"altitude_m": 1000, "direction_deg": 250, "speed_ms": 6.2},
    {"altitude_m": 2000, "direction_deg": 260, "speed_ms": 8.5}
  ]
}
```

### 5. Get Weather Summary
```
GET /api/forecast/summary/{lat}/{lon}
```
**Response (200):** Consolidated sounding + thermal + wind

### 6. Get Available Models Info
```
GET /api/forecast/models/info
```
**Response (200):**
```json
{
  "models": [
    {
      "id": "knmi-harmonie",
      "name": "KNMI HARMONIE-AROME",
      "coverage": "western-europe",
      "update_frequency_h": 6,
      "forecast_hours": 48,
      "resolution_km": 2.5
    },
    {
      "id": "dwd-icon-eu",
      "name": "DWD ICON-EU",
      "coverage": "europe",
      "update_frequency_h": 6,
      "forecast_hours": 180,
      "resolution_km": 13
    }
  ]
}
```

### 7. Get Model Coverage
```
GET /api/forecast/models/coverage/{lat}/{lon}
```
**Response (200):** Available models for location

---

## 🎯 Parcel Analysis API

**Base Path**: `/api/parcel`  
**Atmospheric stability** analysis (CAPE, LCL, LFC, EL, etc.)

### 1. Get Parcel Analysis
```
GET /api/parcel?lat={lat}&lon={lon}&altitude_m={altitude}&include_trace=true
```
**Parameters:**
- `lat` (query, required) - Latitude
- `lon` (query, required) - Longitude
- `altitude_m` (query, optional) - Parcel starting altitude (default: surface)
- `include_trace` (query, optional) - Include full sounding trace

**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "parcel_type": "surface",
  "surface_temperature": 15.2,
  "surface_dewpoint": 9.1,
  "lcl_m": 820,
  "lfc_m": 1250,
  "el_m": 4100,
  "cape": 1250,
  "cin": -25,
  "lifted_index": -2.1,
  "convection_available": true,
  "sounding_trace": [...]
}
```

### 2. Get Parcel Grid
```
GET /api/parcel/grid?bounds={minLat,minLon,maxLat,maxLon}&resolution_km=5
```
**Response (200):** Grid of CAPE/LI values (for heatmaps)

### 3. Get Health Status
```
GET /api/parcel/health
```
**Response (200):** Service health metrics

---

## 🌅 Nowcast API

**Base Path**: `/api/nowcast`  
**Real-time weather** from satellite + OGN observations

### 1. Get Thermal Nowcast
```
GET /api/nowcast/thermal?lat={lat}&lon={lon}&radius_km=50
```
**Parameters:**
- `lat` (query, required) - Center latitude
- `lon` (query, required) - Center longitude
- `radius_km` (query, optional) - Search radius (default: 50km)

**Response (200):**
```json
{
  "center": {"lat": 50.1, "lon": 8.5},
  "timestamp": "2026-03-04T12:50:00Z",
  "thermals": [
    {
      "id": "THERMAL_001",
      "latitude": 50.15,
      "longitude": 8.62,
      "strength": 4,
      "confidence": 0.85,
      "altitude_m": 1800,
      "sources": ["ogn", "satellite"]
    }
  ]
}
```

### 2. Get Sunshine Map
```
GET /api/nowcast/sunshine-map?lat={lat}&lon={lon}&resolution_km=2
```
**Response (200):** Solar radiation intensity map (for thermal prediction)

### 3. Get Thermal History
```
GET /api/nowcast/thermal-history?lat={lat}&lon={lon}&days=7
```
**Response (200):** Historical thermal frequency and strength by location/time

### 4. Get Thermal History Area
```
GET /api/nowcast/thermal-history/area?bounds={minLat,minLon,maxLat,maxLon}&days=30
```
**Response (200):** Regional thermal climatology grid

### 5. Get Best Thermal Regions
```
GET /api/nowcast/thermal-history/best-regions?country=DE&month=3
```
**Response (200):**
```json
{
  "month": "March",
  "regions": [
    {
      "name": "Schwäbische Alb",
      "center": {"lat": 48.5, "lon": 9.2},
      "avg_cape": 650,
      "thermal_frequency": 0.75,
      "best_time": "13:00-15:00"
    }
  ]
}
```

---

## ✈️ XC Planning API

**Base Path**: `/api/xc`  
**Cross-country flight planning** tools

### 1. Get XC Distance & Route
```
GET /api/xc/xc-distance?lat1={lat}&lon1={lon}&lat2={lat}&lon2={lon}&safe_landing_only=true
```
**Parameters:**
- `lat1`, `lon1` (query, required) - Start coordinates
- `lat2`, `lon2` (query, required) - End coordinates
- `safe_landing_only` (query, optional) - Filter to certified landing areas

**Response (200):**
```json
{
  "route": {
    "start": {"lat": 50.1, "lon": 8.5},
    "end": {"lat": 52.0, "lon": 8.0},
    "distance_km": 215.3,
    "duration_h": 2.5
  },
  "waypoints": [
    {"name": "Hornberg", "lat": 50.5, "lon": 8.3, "type": "thermal_hotspot"},
    {"name": "Kinzig Valley", "lat": 51.2, "lon": 8.1, "type": "ridge"}
  ],
  "safe_landing_spots": [
    {"name": "Büdesheim", "lat": 50.8, "lon": 8.4, "quality": 3}
  ]
}
```

### 2. Get Reach Rings (Reachability)
```
GET /api/xc/xc-rings?lat={lat}&lon={lon}&altitude_m=2000&wind_model=forecast
```
**Parameters:**
- `lat`, `lon` (query, required) - Starting position
- `altitude_m` (query, required) - Starting altitude
- `wind_model` (query, optional) - `current`, `forecast` (default: forecast)

**Response (200):**
```json
{
  "center": {"lat": 50.1, "lon": 8.5},
  "altitude_m": 2000,
  "rings": [
    {
      "altitude_m": 2000,
      "glide_ratio": 8,
      "reachable_area_km2": 456,
      "outer_boundary": [
        [50.2, 8.4],
        [50.1, 8.6],
        ...
      ]
    }
  ]
}
```

### 3. Get XC Health Status
```
GET /api/xc/xc-health
```
**Response (200):** API status + available landing zones count

---

## 🗻 Terrain API

**Base Path**: `/api/terrain`  
**Elevation and terrain analysis**

### 1. Get Elevation by Coordinates
```
GET /api/terrain/elevation?lat={lat}&lon={lon}&srtm_version=30
```
**Parameters:**
- `lat` (query, required) - Latitude
- `lon` (query, required) - Longitude
- `srtm_version` (query, optional) - `30`, `90` (resolution in meters, default: 30)

**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "elevation_m": 187,
  "elevation_ft": 613,
  "source": "SRTM30",
  "accuracy_m": 16
}
```

### 2. Get Batch Elevation
```
POST /api/terrain/batch
Content-Type: application/json

{
  "locations": [
    {"lat": 50.1, "lon": 8.5},
    {"lat": 50.2, "lon": 8.6}
  ]
}
```
**Response (200):**
```json
{
  "results": [
    {"location": {"lat": 50.1, "lon": 8.5}, "elevation_m": 187},
    {"location": {"lat": 50.2, "lon": 8.6}, "elevation_m": 245}
  ]
}
```

### 3. Get Elevation Health
```
GET /api/terrain/health
```
**Response (200):** DEM coverage and data status

### 4. Get Slope/Aspect
```
GET /api/terrain/slope-aspect/{lat}/{lon}
```
**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "slope_degrees": 15,
  "aspect_degrees": 230,
  "ground_type": "grass",
  "roughness": "moderate"
}
```

### 5. Get Ridge Detection
```
GET /api/terrain/ridge/{lat}/{lon}?radius_km=5
```
**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "is_ridge": true,
  "ridge_orientation_deg": 210,
  "ridge_height_m": 350,
  "wind_for_ridge": "220°-240°"
}
```

### 6. Get Valley Detection
```
GET /api/terrain/valley/{lat}/{lon}?radius_km=5
```
**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "is_valley": true,
  "valley_orientation_deg": 210,
  "valley_depth_m": 200
}
```

### 7. Get Complete Terrain Analysis
```
GET /api/terrain/complete/{lat}/{lon}
```
**Response (200):** Combined elevation + slope + aspect + ridge/valley detection

### 8. Get DEM Status
```
GET /api/terrain/dem/status
```
**Response (200):** Coverage map and data sources

### 9. Download DEM Region
```
POST /api/terrain/dem/download/{region}
```
**Parameters:**
- `region` - Region identifier (e.g., "EU", "DE", "Alps")

**Response (202):** Async download status

---

## ⚠️ Safety API

**Base Path**: `/api/safety`  
**Safety information**: NOTAMs, Föhn, Thunderstorms

### 1. Get NOTAMs by Region
```
GET /api/safety/notams?lat={lat}&lon={lon}&radius_km=100
```
**Response (200):**
```json
{
  "center": {"lat": 50.1, "lon": 8.5},
  "radius_km": 100,
  "notams": [
    {
      "notam_number": "A2847/26",
      "icao": "EDH",
      "type": "airspace",
      "priority": "high",
      "effective_start": "2026-03-04T08:00:00Z",
      "effective_end": "2026-03-05T20:00:00Z",
      "text": "MILITARY EXERCISE...",
      "min_altitude_m": 500,
      "max_altitude_m": 3500,
      "affects_gliders": true,
      "source": "FAA"
    }
  ]
}
```

### 2. Get NOTAMs for Airfield
```
GET /api/safety/notams/airfield?icao={ICAO}
```
**Response (200):** NOTAMs for specific airfield

### 3. Get NOTAMs for Route
```
GET /api/safety/notams/route?lat1={lat}&lon1={lon}&lat2={lat}&lon2={lon}&altitude_m=2000
```
**Response (200):** NOTAMs intersecting planned route

### 4. Get Föhn Status
```
GET /api/safety/foehn?lat={lat}&lon={lon}
```
**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "foehn_possible": true,
  "foehn_likely": false,
  "foehn_confirmed": false,
  "foehn_score": 0.45,
  "foehn_index": "developing",
  "affected_regions": ["Schwäbische Alb"],
  "wind_direction_crest": 240,
  "temperature_anomaly_c": 2.1,
  "collapse_risk": false,
  "soaring_assessment": "Föhn conditions developing; expect strong lift on lee slope"
}
```

### 5. Get Föhn Map
```
GET /api/safety/foehn/map
```
**Response (200):** GeoJSON all föhn regions + current status

### 6. Get Thunderstorm Risk
```
GET /api/safety/thunderstorm?lat={lat}&lon={lon}&radius_km=100
```
**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "alert_level": 2,
  "alert_message": "Moderate thunderstorm risk",
  "nearest_cell_km": 45,
  "nearest_cell_bearing": 210,
  "cell_intensity": "moderate",
  "approaches_user": true,
  "eta_minutes": 90,
  "lightning_count_50km_1h": 12,
  "cape": 1250,
  "k_index": 28,
  "convective_risk": "moderate"
}
```

### 7. Unified Safety Dashboard
```
GET /api/safety/summary?lat={lat}&lon={lon}
```
**Response (200):**
```json
{
  "overall_safety": "yellow",
  "thunderstorm": {"alert": 2, "nearest_km": 45},
  "foehn": {"foehn_likely": false},
  "notams": {"count": 3, "affects_gliders": 2},
  "visibility": {"condition": "good"},
  "icing_risk": {"risk_level": "low"}
}
```

---

## 🎈 Soaring API

**Base Path**: `/api/soaring`  
**Advanced soaring** condition analysis

### 1. Get Complete Soaring Structure
```
GET /api/soaring/soaring-structure?lat={lat}&lon={lon}&enhanced=true
```
**Response (200):**
```json
{
  "location": {"lat": 50.1, "lon": 8.5},
  "thermals": {
    "strength": 3,
    "base_m": 1800,
    "top_m": 2500
  },
  "ridge_lift": {
    "available": true,
    "optimal_altitude_m": 1200,
    "wind_direction": "240°",
    "wind_speed_ms": 7.5
  },
  "wave": {
    "available": false,
    "lee_lines": []
  },
  "convergence": {
    "available": false
  }
}
```

### 2. Get Live Thermal Hotspots
```
GET /api/soaring/thermals/live?lat={lat}&lon={lon}&radius_km=50
```
**Response (200):** Real-time thermal locations from OGN + satellite

### 3. Get Historical Thermal Data
```
GET /api/soaring/thermals/historical?month=3&region=alps
```
**Response (200):** Monthly thermal climatology

### 4. Get Ridge Soaring Conditions
```
GET /api/soaring/ridge-soaring?lat={lat}&lon={lon}
```
**Response (200):** Ridge lift potential analysis

### 5. Get Wave Conditions
```
GET /api/soaring/wave?lat={lat}&lon={lon}
```
**Response (200):** Wave lift detection and orientation

### 6. Bias Correction Status
```
GET /api/soaring/bias-correction/status
```
**Response (200):** Coverage of ML bias corrections

### 7. ML Model Status
```
GET /api/soaring/ml/status
```
**Response (200):** Thermal prediction model metrics

---

## 🔧 Diagnostics API

**Base Path**: `/api/diagnostics`  
**System health and debugging** (admin only)

### 1. Full System Diagnostics
```
GET /api/diagnostics/full
```
**Response (200):**
```json
{
  "timestamp": "2026-03-04T12:50:43Z",
  "status": "healthy",
  "components": {
    "database": {"status": "ok", "latency_ms": 12},
    "cache": {"status": "ok", "memory_mb": 1024},
    "weather_api": {"status": "operational", "latency_ms": 450},
    "celery": {"status": "ok", "workers": 4},
    "grib_processor": {"status": "idle"}
  }
}
```

### 2. Quick Health Check
```
GET /api/diagnostics/quick
```
**Response (200):** Minimal system status

### 3. Service Status
```
GET /api/diagnostics/status
```
**Response (200):** All services + last update times

---

## 📝 Elevation API

**Base Path**: `/api/elevation`  
**Digital Elevation Model** data

### 1. Single Point Elevation
```
GET /api/elevation/?lat={lat}&lon={lon}
```
**Response (200):** Elevation at single point

### 2. Batch Elevation Requests
```
POST /api/elevation/batch
Content-Type: application/json

{
  "locations": [{"lat": 50.1, "lon": 8.5}, ...]
}
```
**Response (200):** Multiple elevation points

### 3. Elevation Health
```
GET /api/elevation/health
```
**Response (200):** DEM coverage status

---

## 🔐 Authentication & Authorization

**Global Headers:**
```
Authorization: Bearer {token}
X-API-Key: {api_key}
```

**Public Endpoints** (no auth required):
- `/api/diagnostics/status`
- `/api/weather/status`
- Health checks

**Protected Endpoints** (require auth):
- All endpoints with `{admin}` tag
- Data download endpoints

---

## 🔄 Response Format

All endpoints return JSON with consistent wrapper:

**Success (200):**
```json
{
  "status": "success",
  "data": {...},
  "timestamp": "2026-03-04T12:50:43Z"
}
```

**Error (4xx/5xx):**
```json
{
  "status": "error",
  "error": "error_code",
  "message": "Human-readable error message",
  "trace_id": "abc123"
}
```

---

## ⏱️ Rate Limits

- **Public endpoints**: 1000 req/hour per IP
- **Authenticated**: 10,000 req/hour per user
- **Unlimited**: Internal services

**Headers:**
```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1646392200
```

---

## 📦 Caching Strategy

| Endpoint | TTL | Notes |
|----------|-----|-------|
| `/forecast/sounding` | 1h | Live; 7d offline |
| `/weather/metar` | 5m | Frequently updated |
| `/weather/traffic` | 5s | Real-time |
| `/terrain/elevation` | 30d | Static data |
| `/parcel/grid` | 1h | Forecast-dependent |
| `/nowcast/*` | 5m | Real-time |

---

## 🚀 Usage Examples

### Example 1: Flight Planning Checklist
```bash
# Get comprehensive safety summary
curl "http://localhost:8000/api/safety/summary?lat=50.1&lon=8.5"

# Check thermals
curl "http://localhost:8000/api/forecast/thermal/50.1/8.5"

# Get reachable areas
curl "http://localhost:8000/api/xc/xc-rings?lat=50.1&lon=8.5&altitude_m=2000"

# Check NOTAMs
curl "http://localhost:8000/api/safety/notams?lat=50.1&lon=8.5&radius_km=50"
```

### Example 2: Real-time Flight Tracking
```bash
# Get thermal hotspots
curl "http://localhost:8000/api/nowcast/thermal?lat=50.1&lon=8.5&radius_km=30"

# Get live traffic
curl "http://localhost:8000/api/weather/traffic/live"

# Update reachability
curl "http://localhost:8000/api/xc/xc-rings?lat=50.15&lon=8.62&altitude_m=1800"
```

### Example 3: Batch Elevation Request
```bash
curl -X POST "http://localhost:8000/api/elevation/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "locations": [
      {"lat": 50.1, "lon": 8.5},
      {"lat": 50.2, "lon": 8.6},
      {"lat": 50.3, "lon": 8.7}
    ]
  }'
```

---

## 📞 Support

- **Issues**: https://github.com/Strati0101/Glidemate_Backend/issues
- **Email**: l.stratmann@strtmn.de
- **Docs**: https://api.glidematev2.com/docs (Swagger)

---

**Last Updated**: March 4, 2026 | **Status**: Production Ready ✅
