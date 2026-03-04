"""
FastAPI routes for parcel algorithm integration.
Endpoints for atmospheric soaring forecast products.

Deploy to: /opt/glidemate-backend/backend_api_parcel_routes.py
"""

from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/atmosphere", tags=["atmosphere"])

# Try to import parcel algorithm
try:
    from atmosphere import (
        AtmosphericLevel,
        AtmosphericProfile,
        run_parcel_analysis
    )
    from backend_data_pipeline_connector import prepare_enhancement_data
    ATMOSPHERE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Atmosphere module not available: {e}")
    ATMOSPHERE_AVAILABLE = False


def create_profile_from_forecast_data(
    lat: float,
    lon: float,
    forecast_data: Dict[str, Any]
) -> AtmosphericProfile:
    """
    Create atmospheric profile from forecast data.
    
    Args:
        lat: Latitude
        lon: Longitude
        forecast_data: Dictionary with weather model data
    
    Returns:
        AtmosphericProfile ready for analysis
    """
    # Extract levels from forecast data
    levels = []
    
    if "levels" in forecast_data:
        # Direct format with levels array
        for level_data in forecast_data["levels"]:
            level = AtmosphericLevel(
                pressure_hpa=level_data.get("pressure_hpa", 1000),
                height_m=level_data.get("height_m", 0),
                temp_c=level_data.get("temp_c", 15),
                dewpoint_c=level_data.get("dewpoint_c", 10),
                wind_u_ms=level_data.get("wind_u_ms", 0),
                wind_v_ms=level_data.get("wind_v_ms", 0),
                relative_humidity_pct=level_data.get("relative_humidity_pct", 60)
            )
            levels.append(level)
    else:
        # Extract from GRIB/NetCDF structure (typical for weather models)
        pressures = [1000, 850, 700, 500, 300]  # hPa
        heights = [0, 1400, 3000, 5500, 9000]    # meters
        
        for p, h in zip(pressures, heights):
            # Interpolate from available data
            temp = forecast_data.get(f"temp_{p}", 15 - h/500)  # Rough approximation
            td = forecast_data.get(f"dewpoint_{p}", 10 - h/500)
            u = forecast_data.get(f"wind_u_{p}", 5)
            v = forecast_data.get(f"wind_v_{p}", 0)
            rh = forecast_data.get(f"rh_{p}", 60)
            
            level = AtmosphericLevel(
                pressure_hpa=p,
                height_m=h,
                temp_c=temp,
                dewpoint_c=td,
                wind_u_ms=u,
                wind_v_ms=v,
                relative_humidity_pct=rh
            )
            levels.append(level)
    
    # Create profile
    profile = AtmosphericProfile(
        lat=lat,
        lon=lon,
        valid_time=datetime.utcnow(),
        model_source=forecast_data.get("model_source", "ICON-EU"),
        levels=levels,
        surface_temp_c=forecast_data.get("surface_temp_c", 15),
        surface_dewpoint_c=forecast_data.get("surface_dewpoint_c", 10),
        surface_pressure_hpa=forecast_data.get("surface_pressure_hpa", 1000),
        solar_radiation_wm2=forecast_data.get("solar_radiation_wm2", 500)
    )
    
    return profile


@router.get("/parcel")
async def get_parcel_analysis(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    model: str = Query("ICON-EU", description="Weather model (ICON-EU, HARMONIE-AROME, ECMWF)")
):
    """
    Get parcel analysis for a location.
    
    Returns 30+ soaring forecast products:
    - CAPE, CIN, Lifted Index, K-Index
    - Thermal strength (0-5 rating)
    - LCL, LFC, EL heights
    - XC distance potential
    - Wave feasibility
    - Wind shear analysis
    
    Example:
        GET /api/atmosphere/parcel/47.5/10.5?model=ICON-EU
    """
    
    if not ATMOSPHERE_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Atmosphere module not available. Check server logs."
        )
    
    try:
        # Create mock forecast data for now
        # In production, fetch from actual weather model
        forecast_data = {
            "model_source": model,
            "surface_temp_c": 15,
            "surface_dewpoint_c": 10,
            "surface_pressure_hpa": 1000,
            "solar_radiation_wm2": 500,
            "levels": [
                {
                    "pressure_hpa": 1000,
                    "height_m": 0,
                    "temp_c": 15,
                    "dewpoint_c": 10,
                    "wind_u_ms": 5,
                    "wind_v_ms": 0,
                    "relative_humidity_pct": 60
                },
                {
                    "pressure_hpa": 850,
                    "height_m": 1400,
                    "temp_c": 10,
                    "dewpoint_c": 5,
                    "wind_u_ms": 8,
                    "wind_v_ms": 0,
                    "relative_humidity_pct": 55
                },
                {
                    "pressure_hpa": 700,
                    "height_m": 3000,
                    "temp_c": 0,
                    "dewpoint_c": -5,
                    "wind_u_ms": 10,
                    "wind_v_ms": -5,
                    "relative_humidity_pct": 45
                },
                {
                    "pressure_hpa": 500,
                    "height_m": 5500,
                    "temp_c": -20,
                    "dewpoint_c": -30,
                    "wind_u_ms": 15,
                    "wind_v_ms": -10,
                    "relative_humidity_pct": 30
                },
                {
                    "pressure_hpa": 300,
                    "height_m": 9000,
                    "temp_c": -40,
                    "dewpoint_c": -50,
                    "wind_u_ms": 20,
                    "wind_v_ms": -15,
                    "relative_humidity_pct": 20
                }
            ]
        }
        
        # Create profile
        profile = create_profile_from_forecast_data(lat, lon, forecast_data)
        
        # Fetch real forecast data for enhancement modules
        enhancement_data = prepare_enhancement_data(lat, lon, model_source="ICON-EU")
        
        # Run analysis with enhancement data
        result = run_parcel_analysis(profile, forecast_data=enhancement_data)
        
        # Format output
        return {
            "location": {
                "lat": lat,
                "lon": lon,
                "model": model
            },
            "timestamp": datetime.utcnow().isoformat(),
            "heights": {
                "lcl_m": round(result.lcl_height_m, 1),
                "lfc_m": round(result.lfc_height_m, 1),
                "el_m": round(result.el_height_m, 1),
                "thermal_top_m": round(result.thermal_top_m, 1),
                "freezing_level_m": round(result.freezing_level_m, 1)
            },
            "convection": {
                "cape_jkg": round(result.cape_jkg, 1),
                "cin_jkg": round(result.cin_jkg, 1),
                "lifted_index": round(result.lifted_index, 2),
                "k_index": round(result.k_index, 1),
                "total_totals": round(result.total_totals, 1),
                "showalter_index": round(result.showalter_index, 2)
            },
            "soaring": {
                "thermal_strength": result.thermal_strength,
                "thermal_strength_label": result.thermal_strength_label,
                "trigger_temp_c": round(result.trigger_temp_c, 1),
                "blue_thermal_day": result.blue_thermal_day,
                "od_risk": result.od_risk,
                "xc_distance_km": round(result.xc_distance_km, 1),
                "xc_bearing_deg": round(result.xc_best_bearing_deg, 1),
                "fai_triangle_possible": result.fai_triangle_possible
            },
            "wave": {
                "possible": result.wave_possible,
                "froude_number": round(result.froude_number, 3),
                "amplitude_m": round(result.wave_amplitude_m, 1),
                "window_base_m": round(result.wave_window_base_m, 1),
                "window_top_m": round(result.wave_window_top_m, 1)
            },
            "wind_shear": [
                {
                    "from_m": layer["from_m"],
                    "to_m": layer["to_m"],
                    "shear_kt_per_1000ft": round(layer["shear_kt_per_1000ft"], 2),
                    "significant": layer["significant"]
                }
                for layer in result.wind_shear_layers
            ],
            # Enhancement modules - Soaring Forecast Package 2
            "convergence": result.convergence if hasattr(result, 'convergence') else None,
            "blue_thermal": result.blue_thermal if hasattr(result, 'blue_thermal') else None,
            "thermal_day_curve": result.thermal_day_curve if hasattr(result, 'thermal_day_curve') else None
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid profile data: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Parcel analysis error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}"
        )


@router.get("/parcel/grid")
async def get_parcel_grid(
    lat_min: float = Query(45.0, ge=-90, le=90, description="Min latitude"),
    lat_max: float = Query(50.0, ge=-90, le=90, description="Max latitude"),
    lon_min: float = Query(5.0, ge=-180, le=180, description="Min longitude"),
    lon_max: float = Query(15.0, ge=-180, le=180, description="Max longitude"),
    grid_spacing: float = Query(0.5, gt=0, description="Grid spacing in degrees"),
    model: str = Query("ICON-EU", description="Weather model")
):
    """
    Get parcel analysis grid for map display.
    
    Returns GeoJSON feature collection with parcel products.
    
    Example:
        GET /api/atmosphere/parcel/grid?lat_min=45&lat_max=50&lon_min=5&lon_max=15&grid_spacing=1
    """
    
    if not ATMOSPHERE_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Atmosphere module not available"
        )
    
    try:
        features = []
        lat = lat_min
        
        while lat <= lat_max:
            lon = lon_min
            while lon <= lon_max:
                try:
                    # Create profile for this grid point
                    forecast_data = {
                        "model_source": model,
                        "surface_temp_c": 15 - lat/10,
                        "surface_dewpoint_c": 10 - lat/10,
                        "surface_pressure_hpa": 1000,
                        "solar_radiation_wm2": 500,
                        "levels": [
                            {
                                "pressure_hpa": 1000,
                                "height_m": 0,
                                "temp_c": 15 - lat/10,
                                "dewpoint_c": 10 - lat/10,
                                "wind_u_ms": 5,
                                "wind_v_ms": 0,
                                "relative_humidity_pct": 60
                            },
                            {
                                "pressure_hpa": 850,
                                "height_m": 1400,
                                "temp_c": 10 - lat/10,
                                "dewpoint_c": 5 - lat/10,
                                "wind_u_ms": 8,
                                "wind_v_ms": 0,
                                "relative_humidity_pct": 55
                            },
                            {
                                "pressure_hpa": 700,
                                "height_m": 3000,
                                "temp_c": 0 - lat/10,
                                "dewpoint_c": -5 - lat/10,
                                "wind_u_ms": 10,
                                "wind_v_ms": -5,
                                "relative_humidity_pct": 45
                            },
                            {
                                "pressure_hpa": 500,
                                "height_m": 5500,
                                "temp_c": -20 - lat/10,
                                "dewpoint_c": -30 - lat/10,
                                "wind_u_ms": 15,
                                "wind_v_ms": -10,
                                "relative_humidity_pct": 30
                            },
                            {
                                "pressure_hpa": 300,
                                "height_m": 9000,
                                "temp_c": -40 - lat/10,
                                "dewpoint_c": -50 - lat/10,
                                "wind_u_ms": 20,
                                "wind_v_ms": -15,
                                "relative_humidity_pct": 20
                            }
                        ]
                    }
                    
                    profile = create_profile_from_forecast_data(lat, lon, forecast_data)
                    enhancement_data = prepare_enhancement_data(lat, lon, model_source="ICON-EU")
                    result = run_parcel_analysis(profile, forecast_data=enhancement_data)
                    
                    # Create GeoJSON feature
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [lon, lat]
                        },
                        "properties": {
                            "cape": round(result.cape_jkg),
                            "thermal_strength": result.thermal_strength,
                            "thermal_label": result.thermal_strength_label,
                            "xc_km": round(result.xc_distance_km),
                            "wave": result.wave_possible,
                            "li": round(result.lifted_index, 1)
                        }
                    }
                    features.append(feature)
                    
                except Exception as e:
                    logger.warning(f"Grid point ({lat}, {lon}) failed: {e}")
                    continue
                
                lon += grid_spacing
            lat += grid_spacing
        
        return {
            "type": "FeatureCollection",
            "features": features,
            "accuracy": f"{grid_spacing}° spacing",
            "timestamp": datetime.utcnow().isoformat(),
            "point_count": len(features)
        }
        
    except Exception as e:
        logger.error(f"Grid generation error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Grid generation failed: {str(e)}"
        )


@router.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "ok",
        "atmosphere_module": "available" if ATMOSPHERE_AVAILABLE else "not_available",
        "timestamp": datetime.utcnow().isoformat()
    }
