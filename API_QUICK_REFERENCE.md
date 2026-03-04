# 🌐 GlideMate Backend - API Quick Reference

## 📊 API Summary

**Total Endpoints**: 44+  
**Organized Modules**: 10  
**Response Format**: JSON  
**Authentication**: Bearer Token + API Key  

---

## 🔗 All API Endpoints

### 🌤️ WEATHER API (`/api/weather`)
1. `GET /metar/{icao}` - METAR for airfield
2. `GET /taf/{icao}` - TAF forecast
3. `GET /metar-taf/{icao}` - Combined METAR+TAF
4. `GET /sounding/{lat}/{lon}` - Sounding profile
5. `GET /indices/{lat}/{lon}` - Stability indices (CAPE, LI, etc.)
6. `GET /map/{overlay}/tile/{z}/{x}/{y}.png` - Weather map tiles
7. `GET /map/{overlay}/colorbar.png` - Map legend
8. `GET /traffic/live` - Live aircraft positions
9. `GET /traffic/{callsign}` - Aircraft details
10. `GET /status` - Service health check

### 🌡️ FORECAST API (`/api/forecast`)
11. `GET /sounding/{lat}/{lon}` - Forecast sounding (KNMI/DWD with fallback)
12. `GET /indices/{lat}/{lon}` - Forecast indices
13. `GET /thermal/{lat}/{lon}` - Thermal forecast
14. `GET /wind/{lat}/{lon}` - Wind profile forecast
15. `GET /summary/{lat}/{lon}` - Complete weather summary
16. `GET /models/info` - Available weather models
17. `GET /models/coverage/{lat}/{lon}` - Model coverage for location

### 🎯 PARCEL ANALYSIS API (`/api/parcel`)
18. `GET /parcel` - Parcel analysis (CAPE, LCL, LFC, LI, CIN)
19. `GET /parcel/grid` - Grid of parcel values
20. `GET /health` - Service health

### 🌅 NOWCAST API (`/api/nowcast`)
21. `GET /nowcast/thermal` - Real-time thermal hotspots
22. `GET /nowcast/sunshine-map` - Solar radiation map
23. `GET /thermal-history` - Historical thermal data
24. `GET /thermal-history/area` - Regional thermal stats
25. `GET /thermal-history/best-regions` - Top thermal regions

### ✈️ XC PLANNING API (`/api/xc`)
26. `GET /xc-distance` - Distance calculation + waypoints
27. `GET /xc-rings` - Reachable areas (reach rings)
28. `GET /xc-health` - Service health

### 🗻 TERRAIN API (`/api/terrain`)
29. `GET /elevation` - Single elevation point
30. `POST /batch` - Batch elevation requests
31. `GET /health` - DEM health
32. `GET /slope-aspect/{lat}/{lon}` - Slope + aspect analysis
33. `GET /ridge/{lat}/{lon}` - Ridge detection
34. `GET /valley/{lat}/{lon}` - Valley detection
35. `GET /complete/{lat}/{lon}` - Full terrain analysis
36. `GET /dem/status` - DEM coverage map
37. `POST /dem/download/{region}` - Download DEM region

### ⚠️ SAFETY API (`/api/safety`)
38. `GET /notams` - NOTAMs by region
39. `GET /notams/airfield` - NOTAMs for airfield
40. `GET /notams/route` - NOTAMs along flight route
41. `GET /foehn` - Föhn status
42. `GET /foehn/map` - Föhn all regions map
43. `GET /thunderstorm` - Thunderstorm risk
44. `GET /summary` - Unified safety dashboard

### 🎈 SOARING API (`/api/soaring`)
45. `GET /soaring-structure` - Complete soaring analysis
46. `GET /thermals/live` - Live thermal hotspots
47. `GET /thermals/historical` - Thermal climatology
48. `GET /ridge-soaring` - Ridge lift potential
49. `GET /wave` - Wave conditions
50. `GET /bias-correction/status` - ML bias coverage
51. `GET /ml/status` - Model metrics

### 🔧 DIAGNOSTICS API (`/api/diagnostics`)
52. `GET /full` - Full system diagnostics
53. `GET /quick` - Quick health check
54. `GET /status` - Service status

### 📝 ELEVATION API (`/api/elevation`)
55. `GET /` - Single point elevation
56. `POST /batch` - Batch elevation
57. `GET /health` - Service health

---

## 🗺️ API Organization by Function

### For Flight Planning
- `/api/safety/summary` - Overall safety assessment
- `/api/forecast/summary/{lat}/{lon}` - Weather overview
- `/api/xc/xc-distance` - Route planning
- `/api/xc/xc-rings` - Reachable areas
- `/api/terrain/elevation` - Ground clearance

### For Real-Time Flying
- `/api/nowcast/thermal` - Current thermals
- `/api/weather/traffic/live` - Live traffic
- `/api/safety/thunderstorm` - Storm threats
- `/api/weather/metar/{icao}` - Current conditions
- `/api/xc/xc-rings` - Dynamic reachability

### For Soaring Forecasting
- `/api/forecast/thermal/{lat}/{lon}` - Thermal forecast
- `/api/forecast/sounding/{lat}/{lon}` - Atmospheric profile
- `/api/parcel/parcel` - Stability analysis
- `/api/soaring/soaring-structure` - Complete lift analysis
- `/api/nowcast/sunshine-map` - Solar radiation

### For Safety & Hazards
- `/api/safety/notams` - Airspace restrictions
- `/api/safety/foehn` - Föhn warnings
- `/api/safety/thunderstorm` - CB detection
- `/api/weather/traffic/live` - Collision avoidance
- `/api/terrain/complete/{lat}/{lon}` - Terrain hazards

### For System Monitoring
- `/api/diagnostics/full` - Complete status
- `/api/weather/status` - Weather API status
- `/api/xc/xc-health` - XC service health
- `/api/parcel/health` - Parcel service health

---

## 🔑 Common Query Parameters

| Parameter | Type | Example | Description |
|-----------|------|---------|-------------|
| `lat` | float | 50.1 | Latitude (-90 to 90) |
| `lon` | float | 8.5 | Longitude (-180 to 180) |
| `radius_km` | int | 50 | Search radius |
| `altitude_m` | int | 2000 | Altitude in meters |
| `bounds` | string | "50.0,8.0,51.0,9.0" | Bounding box |
| `resolution_km` | float | 2.5 | Grid resolution |
| `days` | int | 7 | Historical range |
| `include_bundle` | bool | true | Include offline data |
| `wind_model` | string | "forecast" | Data source |

---

## 📬 Response Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 202 | Accepted (async) |
| 400 | Bad request (invalid params) |
| 401 | Unauthorized (missing auth) |
| 403 | Forbidden (insufficient permissions) |
| 404 | Not found |
| 429 | Rate limit exceeded |
| 500 | Server error |
| 503 | Service unavailable |

---

## 🔐 Authentication

**API Key (Header):**
```
Authorization: Bearer YOUR_TOKEN_HERE
X-API-Key: YOUR_API_KEY_HERE
```

**Public Endpoints** (no auth):
- `GET /api/health`
- `GET /api/diagnostics/status`
- `GET /api/weather/traffic/live` (rate-limited)

---

## ⏱️ Response Times (avg)

| Endpoint | Time | Notes |
|----------|------|-------|
| `/metar/{icao}` | 50ms | Cached, real-time |
| `/forecast/sounding` | 300ms | NWP model data |
| `/parcel` | 100ms | Instant calculation |
| `/nowcast/thermal` | 200ms | OGN + satellite |
| `/elevation` | 20ms | DEM lookup |
- `/xc-rings` | 1000ms | Complex geometry |
| `/safety/summary` | 500ms | Multi-source |

---

## 📦 Data Formats

**Coordinates**: Decimal degrees (WGS84)
```
lat: 50.1  (°N)
lon: 8.5   (°E)
```

**Altitude**: Meters (MSL)
```
altitude_m: 2000
```

**Temperature**: Celsius
```
temperature_c: 15.2
```

**Wind**: Direction (°) + Speed (m/s or kt)
```
wind_direction: 240  (magnetic bearing)
wind_speed_ms: 8.5
wind_speed_kt: 16.5
```

**Timestamp**: ISO 8601 UTC
```
"2026-03-04T12:50:43Z"
```

---

## 🚀 Quick Start Examples

### 1. Get Current Conditions
```bash
curl "http://localhost:8000/api/weather/metar/EDDF" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### 2. Forecast Thermals
```bash
curl "http://localhost:8000/api/forecast/thermal/50.1/8.5" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### 3. Check Safety
```bash
curl "http://localhost:8000/api/safety/summary?lat=50.1&lon=8.5" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### 4. Batch Elevation
```bash
curl -X POST "http://localhost:8000/api/elevation/batch" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "locations": [
      {"lat": 50.1, "lon": 8.5},
      {"lat": 50.2, "lon": 8.6}
    ]
  }'
```

### 5. Reachable Areas
```bash
curl "http://localhost:8000/api/xc/xc-rings?lat=50.1&lon=8.5&altitude_m=2000" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

---

## 🌍 Coverage

| Service | Coverage | Resolution |
|---------|----------|------------|
| Weather (METAR/TAF) | Global | Per airfield |
| Forecast (KNMI) | Western Europe | 2.5 km |
| Forecast (DWD) | Europe | 13 km |
| Terrain (SRTM) | 60°N to 56°S | 30m or 90m |
| Traffic (OGN) | Central Europe | Real-time |
| Nowcast | Europe | Variable |

---

## 🛠️ Development

**Local Testing:**
```bash
# Start backend
python app/main.py

# Test endpoint
curl "http://localhost:8000/api/weather/status"

# View API docs (Swagger)
# http://localhost:8000/docs

# View API docs (ReDoc)
# http://localhost:8000/redoc
```

---

## 📖 Complete Documentation

Full API documentation with response models, error handling, caching strategies, and usage examples:

👉 **[API_DOCUMENTATION.md](./API_DOCUMENTATION.md)** (GitHub)

---

**Last Updated**: March 4, 2026
**Version**: 2.0.0 (Phase 3)  
**Status**: ✅ Production Ready
