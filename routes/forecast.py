"""
backend_api_forecast_routes.py
FastAPI routes for forecast data with fallback logic and bundled storage

Features:
- Region-based model selection (KNMI for NL/BE/NRW, DWD elsewhere)
- Automatic fallback logic
- model_used field in all responses
- Bundled data for offline availability
"""

import logging
from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from datetime import datetime
from typing import Optional
import json

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/forecast", tags=["forecast"])


# ════════════════════════════════════════════════════════════════
# Sounding & Atmospheric Profile Endpoints
# ════════════════════════════════════════════════════════════════

@router.get("/sounding/{lat}/{lon}")
async def get_sounding_forecast(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    include_bundle: bool = Query(False, description="Include bundled data info"),
    forecast_service = Depends(lambda: None)  # Injected in main app
):
    """
    Get atmospheric sounding forecast with automatic fallback
    
    **Region-Based Model Selection:**
    - In priority region (47°N–57°N, 2°W–16°E): Uses KNMI HARMONIE-AROME
      - Falls back to DWD ICON-EU if KNMI download fails
    - Outside priority region: Uses DWD ICON-EU
    - If both fail: Returns cached/offline data
    
    **Request:**
    ```
    GET /api/forecast/sounding/50.1/8.5
    GET /api/forecast/sounding/52.4/5.0?include_bundle=true
    ```
    
    **Response (Success - KNMI Region):**
    ```json
    {
        "status": "success",
        "location": {"lat": 50.1, "lon": 8.5},
        "valid_at": "2026-03-02T16:00:00Z",
        "source": "knmi-harmonie",
        "model_used": "KNMI-HARMONIE-AROME",
        "data_age_minutes": 45,
        "bundle_id": "knmi-harmonie-20260302-5008",
        "levels": [
            {
                "pressure_hpa": 1000,
                "height_m": 100,
                "temperature_c": 15.2,
                "dewpoint_c": 9.1,
                "wind_direction_deg": 240,
                "wind_speed_ms": 4.1
            },
            ...
        ],
        "indices": {
            "cape": 1250,
            "lifted_index": -2.1,
            "k_index": 28.5,
            "total_totals": 48.2
        }
    }
    ```
    
    **Response (Success - DWD Fallback):**
    ```json
    {
        "status": "success",
        "location": {"lat": 50.1, "lon": 8.5},
        "valid_at": "2026-03-02T12:00:00Z",
        "source": "dwd-icon-eu",
        "model_used": "DWD-ICON-EU",
        "data_age_minutes": 180,
        "bundle_id": "dwd-icon-eu-20260302-5008",
        ...
    }
    ```
    
    **Response (Cached Data):**
    ```json
    {
        "status": "success_cached",
        "location": {"lat": 50.1, "lon": 8.5},
        "valid_at": "2026-03-02T06:00:00Z",
        "source": "cache",
        "model_used": "DWD-ICON-EU",
        "data_age_minutes": 600,
        "message": "Live data unavailable; serving cached forecast",
        ...
    }
    ```
    
    **Cache:**
    - Live data: 1 hour (Redis)
    - Bundled data: 7 days (disk)
    
    **Use Case:**
    Display sounding profile on flight planning screen
    """
    try:
        if not forecast_service:
            raise HTTPException(status_code=503, detail="Forecast service not initialized")
        
        # Get forecast with fallback logic
        result = await forecast_service.get_sounding_forecast(lat, lon)
        
        return JSONResponse(content=result, status_code=200 if result['status'].startswith('success') else 500)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_sounding_forecast: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch sounding forecast")


@router.get("/indices/{lat}/{lon}")
async def get_stability_indices(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    forecast_service = Depends(lambda: None)
):
    """
    Get stability indices (lightweight version of sounding)
    
    **Request:**
    ```
    GET /api/forecast/indices/50.1/8.5
    ```
    
    **Response:**
    ```json
    {
        "status": "success",
        "location": {"lat": 50.1, "lon": 8.5},
        "model_used": "KNMI-HARMONIE-AROME",
        "data_age_minutes": 45,
        "indices": {
            "cape": 1250,
            "cape_3km": 850,
            "cin": -50,
            "lifted_index": -2.1,
            "k_index": 28.5,
            "total_totals": 48.2,
            "showalter_index": 0.5,
            "cloud_base_m": 850
        }
    }
    ```
    
    **Cache:** 1 hour  
    **Use Case:** Quick map overlay display, soaring forecast indicators
    """
    try:
        if not forecast_service:
            raise HTTPException(status_code=503, detail="Forecast service not initialized")
        
        sounding = await forecast_service.get_sounding_forecast(lat, lon)
        
        if sounding['status'] != 'success':
            raise HTTPException(status_code=500, detail=sounding.get('message'))
        
        return {
            'status': 'success',
            'location': {'lat': lat, 'lon': lon},
            'model_used': sounding['model_used'],
            'data_age_minutes': sounding.get('data_age_minutes', 0),
            'indices': sounding.get('indices', {})
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_stability_indices: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch stability indices")


# ════════════════════════════════════════════════════════════════
# Thermal Forecast Endpoints
# ════════════════════════════════════════════════════════════════

@router.get("/thermal/{lat}/{lon}")
async def get_thermal_forecast(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    height_m: int = Query(0, description="Height above ground (0-10000m)"),
    forecast_service = Depends(lambda: None)
):
    """
    Get thermal strength forecast (soaring prediction)
    
    Calculated from CAPE using fallback model logic
    
    **Request:**
    ```
    GET /api/forecast/thermal/50.1/8.5
    GET /api/forecast/thermal/50.1/8.5?height_m=1000
    ```
    
    **Response:**
    ```json
    {
        "status": "success",
        "location": {"lat": 50.1, "lon": 8.5},
        "thermal_strength": 4,
        "thermal_strength_label": "Strong",
        "cape": 1500,
        "model_used": "KNMI-HARMONIE-AROME",
        "bundle_id": "knmi-harmonie-20260302-5008",
        "valid_at": "2026-03-02T16:00:00Z",
        "data_age_minutes": 45
    }
    ```
    
    **Thermal Strength Scale:**
    - 5: Very Strong (CAPE > 2000 J/kg)
    - 4: Strong (CAPE 1500-2000)
    - 3: Moderate (CAPE 1000-1500)
    - 2: Weak (CAPE 500-1000)
    - 1: Very Weak (CAPE < 500)
    
    **Cache:** 1 hour  
    **Use Case:** Thermal forecast display on soaring flight planning screen
    """
    try:
        if not forecast_service:
            raise HTTPException(status_code=503, detail="Forecast service not initialized")
        
        result = await forecast_service.get_thermal_forecast(lat, lon, height_m)
        
        if result['status'] != 'success':
            raise HTTPException(status_code=500, detail=result.get('message'))
        
        # Add human-readable label
        thermal_labels = {
            5: "Very Strong",
            4: "Strong",
            3: "Moderate",
            2: "Weak",
            1: "Very Weak"
        }
        result['thermal_strength_label'] = thermal_labels.get(result['thermal_strength'], "Unknown")
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_thermal_forecast: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch thermal forecast")


# ════════════════════════════════════════════════════════════════
# Wind Profile Endpoints
# ════════════════════════════════════════════════════════════════

@router.get("/wind/{lat}/{lon}")
async def get_wind_profile(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    forecast_service = Depends(lambda: None)
):
    """
    Get wind profile (wind speed and direction vs altitude)
    
    **Request:**
    ```
    GET /api/forecast/wind/50.1/8.5
    ```
    
    **Response:**
    ```json
    {
        "status": "success",
        "location": {"lat": 50.1, "lon": 8.5},
        "model_used": "KNMI-HARMONIE-AROME",
        "bundle_id": "knmi-harmonie-20260302-5008",
        "valid_at": "2026-03-02T16:00:00Z",
        "wind_layers": [
            {
                "altitude_m": 0,
                "wind_speed_ms": 4.1,
                "wind_direction": 240
            },
            {
                "altitude_m": 500,
                "wind_speed_ms": 6.2,
                "wind_direction": 245
            },
            {
                "altitude_m": 1000,
                "wind_speed_ms": 8.5,
                "wind_direction": 250
            },
            ...
        ]
    }
    ```
    
    **Cache:** 1 hour  
    **Use Case:** Wind layer visualization, flight planning above cloud tops
    """
    try:
        if not forecast_service:
            raise HTTPException(status_code=503, detail="Forecast service not initialized")
        
        result = await forecast_service.get_wind_profile(lat, lon)
        
        if result['status'] != 'success':
            raise HTTPException(status_code=500, detail=result.get('message'))
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_wind_profile: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch wind profile")


# ════════════════════════════════════════════════════════════════
# Comprehensive Weather Summary
# ════════════════════════════════════════════════════════════════

@router.get("/summary/{lat}/{lon}")
async def get_weather_summary(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    forecast_service = Depends(lambda: None)
):
    """
    Get comprehensive weather summary for location
    
    Combines sounding, thermal, and wind data with automatic fallback
    
    **Request:**
    ```
    GET /api/forecast/summary/50.1/8.5
    ```
    
    **Response:**
    ```json
    {
        "status": "success",
        "location": {"lat": 50.1, "lon": 8.5},
        "valid_at": "2026-03-02T16:00:00Z",
        "model_used": "KNMI-HARMONIE-AROME",
        "bundle_id": "knmi-harmonie-20260302-5008",
        "data_freshness": {
            "is_live": true,
            "age_minutes": 45
        },
        "components": {
            "sounding": {...},
            "thermal": {...},
            "wind": {...}
        }
    }
    ```
    
    **Cache:** 1 hour (components individually cached)  
    **Use Case:** Flight briefing display, web map dashboard
    """
    try:
        if not forecast_service:
            raise HTTPException(status_code=503, detail="Forecast service not initialized")
        
        result = await forecast_service.get_weather_summary(lat, lon)
        
        if result['status'] != 'success':
            raise HTTPException(status_code=500, detail=result.get('message'))
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weather_summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch weather summary")


# ════════════════════════════════════════════════════════════════
# Model Information & Status
# ════════════════════════════════════════════════════════════════

@router.get("/models/info")
async def get_models_info():
    """
    Get information about available forecast models
    
    **Response:**
    ```json
    {
        "models": [
            {
                "id": "knmi-harmonie-arome",
                "name": "KNMI HARMONIE-AROME CY43-P3-1.0",
                "provider": "Royal Netherlands Meteorological Institute",
                "resolution_km": 5.5,
                "update_frequency_hours": 3,
                "update_times_utc": [0, 3, 6, 9, 12, 15, 18, 21],
                "coverage": {
                    "north": 57.0,
                    "south": 47.0,
                    "east": 16.0,
                    "west": -2.0
                },
                "coverage_name": "Netherlands, Belgium, NRW (Germany)",
                "priority": 11
            },
            {
                "id": "dwd-icon-eu",
                "name": "DWD ICON-EU",
                "provider": "Deutscher Wetterdienst",
                "resolution_km": 5.5,
                "update_frequency_hours": 6,
                "update_times_utc": [0, 6, 12, 18],
                "coverage": {
                    "west": -10,
                    "east": 35,
                    "south": 30,
                    "north": 70
                },
                "coverage_name": "Europe",
                "priority": 10
            }
        ],
        "priority_rules": {
            "in_knmi_region": "Use KNMI HARMONIE (priority 11), fall back to DWD if fails",
            "outside_knmi_region": "Use DWD ICON-EU (priority 10)",
            "if_both_fail": "Return cached/offline data"
        }
    }
    ```
    """
    return {
        "models": [
            {
                "id": "knmi-harmonie-arome",
                "name": "KNMI HARMONIE-AROME CY43-P3-1.0",
                "provider": "Royal Netherlands Meteorological Institute",
                "resolution_km": 5.5,
                "update_frequency_hours": 3,
                "update_times_utc": [0, 3, 6, 9, 12, 15, 18, 21],
                "coverage": {
                    "north": 57.0,
                    "south": 47.0,
                    "east": 16.0,
                    "west": -2.0
                },
                "coverage_name": "Netherlands, Belgium, NRW (Germany)",
                "priority": 11
            },
            {
                "id": "dwd-icon-eu",
                "name": "DWD ICON-EU",
                "provider": "Deutscher Wetterdienst",
                "resolution_km": 5.5,
                "update_frequency_hours": 6,
                "update_times_utc": [0, 6, 12, 18],
                "coverage": {
                    "west": -10,
                    "east": 35,
                    "south": 30,
                    "north": 70
                },
                "coverage_name": "Europe",
                "priority": 10
            }
        ],
        "priority_rules": {
            "in_knmi_region": "Use KNMI HARMONIE (priority 11), fall back to DWD if fails",
            "outside_knmi_region": "Use DWD ICON-EU (priority 10)",
            "if_both_fail": "Return cached/offline data"
        }
    }


@router.get("/models/coverage/{lat}/{lon}")
async def get_model_coverage(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
):
    """
    Get which model will be used for a specific location
    
    **Response:**
    ```json
    {
        "location": {"lat": 50.1, "lon": 8.5},
        "primary_model": "KNMI-HARMONIE-AROME",
        "fallback_model": "DWD-ICON-EU",
        "in_priority_region": true,
        "message": "This location uses KNMI HARMONIE as primary source"
    }
    ```
    """
    # Check if in priority region
    in_priority = (47.0 <= lat <= 57.0 and -2.0 <= lon <= 16.0)
    
    return {
        "location": {"lat": lat, "lon": lon},
        "primary_model": "KNMI-HARMONIE-AROME" if in_priority else "DWD-ICON-EU",
        "fallback_model": "DWD-ICON-EU" if in_priority else "None",
        "in_priority_region": in_priority,
        "message": "This location uses KNMI HARMONIE as primary source" if in_priority else "This location uses DWD ICON-EU"
    }
