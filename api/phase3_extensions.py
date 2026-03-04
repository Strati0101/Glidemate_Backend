"""
Phase 3 Extensions - API Routes for new data sources

Adds endpoints for:
- Météo-France AROME forecasts
- GeoSphere Austria forecasts and observations  
- Copernicus DEM elevation and terrain analysis
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/forecast", tags=["Phase 3 Extensions"])


# =============================================================================
# MÉTÉO-FRANCE AROME ENDPOINTS
# =============================================================================

@router.get("/meteofrance-arome/{lat}/{lon}")
async def get_meteofrance_arome_forecast(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    forecast_hour: int = Query(0, ge=0, le=48),
    db = None  # Injected by dependency
):
    """
    Météo-France AROME high-resolution forecast (1.3km).
    
    Priority Region: 43°N–52°N, 5°W–10°E (France, Alsace, Lorraine, Swiss border)
    
    Coverage outside this region will be provided by a fallback model (DWD ICON-EU).
    """
    from backend_meteofrance_integration import (
        MeteoFranceAROMEClient,
        get_meteofrance_forecast
    )
    
    # Check region
    client = MeteoFranceAROMEClient(token_manager=None)
    in_region = client.is_in_priority_region(lat, lon)
    
    if not in_region:
        return {
            "status": "out_of_region",
            "message": "Point outside Météo-France AROME priority region",
            "fallback_model": "dwd-icon-eu",
            "location": {"lat": lat, "lon": lon}
        }
    
    # Try to fetch forecast
    import asyncio
    loop = asyncio.get_event_loop()
    
    try:
        forecast = loop.run_until_complete(get_meteofrance_forecast(lat, lon, forecast_hour=forecast_hour))
        
        if forecast:
            return {
                "status": "success",
                "location": {"lat": lat, "lon": lon},
                "model_used": "meteofrance-arome",
                "forecast_hour": forecast_hour,
                "model_info": {
                    "name": "Météo-France AROME",
                    "resolution_km": 1.3,
                    "update_frequency": "1 hour",
                    "priority": 12
                },
                "data": forecast.get("data", {}),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        else:
            return {
                "status": "no_data",
                "message": "Failed to retrieve AROME forecast",
                "location": {"lat": lat, "lon": lon},
                "fallback": "Check availability later or use fallback model"
            }
    
    except Exception as e:
        logger.error(f"AROME forecast error: {e}")
        return {
            "status": "error",
            "message": str(e),
            "fallback_model": "dwd-icon-eu"
        }


# =============================================================================
# GEOSPHERE AUSTRIA ENDPOINTS
# =============================================================================

@router.get("/geosphere-austria/{lat}/{lon}")
async def get_geosphere_austria_forecast(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    forecast_hour: int = Query(0, ge=0, le=48)
):
    """
    GeoSphere Austria INCA high-resolution forecast (1km).
    
    Priority Region: 46°N–49.5°N, 9°E–18°E (Austria, South Bavaria, South Tyrol)
    
    Includes station observation data when available.
    """
    from backend_geosphere_austria_integration import (
        GeoSphereAustriaClient,
        get_geosphere_austria_forecast
    )
    
    # Check region
    client = GeoSphereAustriaClient()
    in_region = client.is_in_priority_region(lat, lon)
    
    if not in_region:
        return {
            "status": "out_of_region",
            "message": "Point outside GeoSphere Austria INCA priority region",
            "fallback_model": "dwd-icon-eu",
            "location": {"lat": lat, "lon": lon}
        }
    
    # Fetch forecast
    import asyncio
    loop = asyncio.get_event_loop()
    
    try:
        forecast = loop.run_until_complete(get_geosphere_austria_forecast(lat, lon))
        
        if forecast:
            return {
                "status": "success",
                "location": {"lat": lat, "lon": lon},
                "model_used": "geosphere-austria-inca",
                "forecast_hour": forecast_hour,
                "model_info": {
                    "name": "GeoSphere Austria INCA",
                    "resolution_km": 1.0,
                    "update_frequency": "1 hour",
                    "priority": 11
                },
                "data": forecast.get("variables", {}),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        else:
            return {
                "status": "no_data",
                "message": "Failed to retrieve INCA forecast",
                "location": {"lat": lat, "lon": lon}
            }
    
    except Exception as e:
        logger.error(f"INCA forecast error: {e}")
        return {
            "status": "error",
            "message": str(e),
            "fallback_model": "dwd-icon-eu"
        }


@router.get("/stations/austria")
async def get_austria_station_observations():
    """
    Get current observations from all Austrian TAWES stations.
    
    Updates: Every 10 minutes from source
    Coverage: Entire Austria + border regions
    
    Returns: List of station observations with temperature, wind, precip, etc.
    """
    from backend_geosphere_austria_integration import get_geosphere_austria_observations
    
    import asyncio
    loop = asyncio.get_event_loop()
    
    try:
        observations = loop.run_until_complete(get_geosphere_austria_observations())
        
        return {
            "status": "success",
            "station_count": len(observations),
            "update_frequency_minutes": 10,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "stations": observations
        }
    
    except Exception as e:
        logger.error(f"Austrian stations error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ELEVATION & TERRAIN ENDPOINTS
# =============================================================================

@router.get("/elevation/{lat}/{lon}")
async def get_elevation(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180)
):
    """
    Get elevation in meters from Copernicus DEM (30m resolution).
    
    Coverage: Europe (35°N–72°N, 25°W–45°E)
    Resolution: 30 meters
    Source: Copernicus GLO-30
    License: CC BY 4.0
    """
    from backend_dem_analysis import TerrainAnalyzer
    
    try:
        analyzer = TerrainAnalyzer()
        
        if not analyzer.load_dem():
            return {
                "status": "dem_unavailable",
                "message": "DEM not loaded in system",
                "location": {"lat": lat, "lon": lon}
            }
        
        elevation_m = analyzer.get_elevation(lat, lon)
        
        if elevation_m is None:
            return {
                "status": "outside_coverage",
                "message": "Point outside DEM coverage area",
                "location": {"lat": lat, "lon": lon},
                "coverage": {
                    "lat_min": 35,
                    "lat_max": 72,
                    "lon_min": -25,
                    "lon_max": 45
                }
            }
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon},
            "elevation_m": round(elevation_m, 1),
            "dem_info": {
                "source": "Copernicus DEM (GLO-30)",
                "resolution_m": 30,
                "license": "CC BY 4.0"
            }
        }
    
    except Exception as e:
        logger.error(f"Elevation query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/terrain/slope-aspect/{lat}/{lon}")
async def get_terrain_slope_aspect(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180)
):
    """
    Get terrain slope and aspect (direction slope faces).
    
    Used for thermal strength calculations:
    - South-facing slopes: +10-30% thermal strength
    - North-facing slopes: -20-30% thermal strength
    - Steep slopes (>20°): +5% thermal strength
    
    Aspect: 0°=N, 90°=E, 180°=S, 270°=W
    """
    from backend_dem_analysis import analyze_terrain
    
    try:
        analysis = analyze_terrain(lat, lon)
        
        if not analysis.get("dem_available"):
            return {
                "status": "dem_unavailable",
                "location": {"lat": lat, "lon": lon}
            }
        
        slope_data = analysis.get("slope_aspect", {})
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon},
            "slope_degrees": slope_data.get("slope_degrees"),
            "aspect_degrees": slope_data.get("aspect_degrees"),
            "thermal_factor": slope_data.get("thermal_factor"),
            "characteristics": {
                "is_south_facing": slope_data.get("is_south_facing"),
                "is_steep": slope_data.get("is_steep")
            },
            "elevation_m": slope_data.get("center_elevation_m")
        }
    
    except Exception as e:
        logger.error(f"Slope/aspect query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/terrain/ridge/{lat}/{lon}")
async def get_terrain_ridge(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180)
):
    """
    Detect ridges for wave flying calculations.
    
    Returns:
    - ridge_detected: Whether a significant ridge is present
    - ridge_bearing_deg: Direction of the ridge (0°=N, 90°=E, etc.)
    - ridge_height_m: Elevation gain along the ridge
    
    Used in parcel.py Froude number calculation for wave strength.
    """
    from backend_dem_analysis import analyze_terrain
    
    try:
        analysis = analyze_terrain(lat, lon)
        
        if not analysis.get("dem_available"):
            return {
                "status": "dem_unavailable",
                "location": {"lat": lat, "lon": lon}
            }
        
        ridge_data = analysis.get("ridge", {})
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon},
            "ridge_detected": ridge_data.get("ridge_detected"),
            "ridge_bearing_deg": ridge_data.get("ridge_bearing_deg"),
            "ridge_height_m": ridge_data.get("ridge_height_m"),
            "center_elevation_m": ridge_data.get("center_elevation_m"),
            "froude_adjustment_factor": analysis.get("froude_adjustment_factor")
        }
    
    except Exception as e:
        logger.error(f"Ridge detection error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/terrain/valley/{lat}/{lon}")
async def get_terrain_valley(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180)
):
    """
    Detect valley terrain for convergence zone identification.
    
    Valleys channel wind and create convergence zones.
    This enhances thermal strength by ~15%.
    
    Returns:
    - is_valley: Whether location is in a valley
    - convergence_boost: Thermal strength multiplier
    """
    from backend_dem_analysis import analyze_terrain
    
    try:
        analysis = analyze_terrain(lat, lon)
        
        if not analysis.get("dem_available"):
            return {
                "status": "dem_unavailable",
                "location": {"lat": lat, "lon": lon}
            }
        
        valley_data = analysis.get("valley", {})
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon},
            "is_valley": valley_data.get("is_valley"),
            "convergence_boost": valley_data.get("convergence_boost"),
            "surrounding_elevation_m": valley_data.get("surrounding_mean_elevation_m"),
            "center_elevation_m": valley_data.get("center_elevation_m")
        }
    
    except Exception as e:
        logger.error(f"Valley detection error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/terrain/complete/{lat}/{lon}")
async def get_complete_terrain_analysis(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    utc_hour: Optional[int] = Query(None, ge=0, le=23)
):
    """
    Get complete terrain analysis combining:
    - Ridge detection (for wave)
    - Slope/aspect (for thermals)
    - Valley zones (for convergence)
    - Shadow zones (for time-of-day)
    
    Returns combined thermal factor accounting for all terrain effects.
    """
    from backend_dem_analysis import analyze_terrain
    
    try:
        analysis = analyze_terrain(lat, lon, utc_hour=utc_hour)
        
        return {
            "status": "success" if analysis.get("dem_available") else "degraded",
            "location": {"lat": lat, "lon": lon},
            "dem_available": analysis.get("dem_available"),
            "ridge": analysis.get("ridge"),
            "slope_aspect": analysis.get("slope_aspect"),
            "valley": analysis.get("valley"),
            "shadow": analysis.get("shadow", {}),
            "combined_thermal_factor": analysis.get("combined_thermal_factor", 1.0),
            "froude_adjustment_factor": analysis.get("froude_adjustment_factor", 1.0),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    
    except Exception as e:
        logger.error(f"Complete terrain analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# DEM MANAGEMENT ENDPOINTS (Admin)
# =============================================================================

@router.get("/dem/status")
async def get_dem_status():
    """
    Get DEM download status and storage info.
    
    Admin endpoint to monitor DEM ingestion.
    """
    from backend_dem_processor import check_dem_download_status
    
    try:
        status = check_dem_download_status()
        return {
            "status": "success",
            "dem": status,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    except Exception as e:
        logger.error(f"DEM status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dem/download/{region}")
async def trigger_dem_download(region: str = "france"):
    """
    Trigger DEM download for a region (admin only).
    
    Valid regions: france, austria, alpine
    
    This is a background task - returns immediately with task ID.
    Check status with /api/forecast/dem/status
    """
    from backend_celery_tasks_phase3_extensions import download_copernicus_dem_task
    
    region_bounds = {
        "france": {"lat_min": 43, "lat_max": 52, "lon_min": -5, "lon_max": 10},
        "austria": {"lat_min": 46, "lat_max": 49, "lon_min": 9, "lon_max": 18},
        "alpine": {"lat_min": 43, "lat_max": 49, "lon_min": 4, "lon_max": 16}
    }
    
    if region not in region_bounds:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown region. Valid: {list(region_bounds.keys())}"
        )
    
    bounds = region_bounds[region]
    
    try:
        task = download_copernicus_dem_task.delay(
            region=region,
            lat_min=bounds["lat_min"],
            lat_max=bounds["lat_max"],
            lon_min=bounds["lon_min"],
            lon_max=bounds["lon_max"]
        )
        
        return {
            "status": "queued",
            "region": region,
            "task_id": task.id,
            "bounds": bounds,
            "message": "Download started, check task status"
        }
    
    except Exception as e:
        logger.error(f"DEM download trigger error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
