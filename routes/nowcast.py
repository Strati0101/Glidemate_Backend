"""
API Routes for Solar Nowcasting and Thermal History

Endpoints:
  GET /api/nowcast/thermal?lat={}&lon={}
  GET /api/nowcast/sunshine-map?bbox={}
  GET /api/thermal-history?lat={}&lon={}
  GET /api/thermal-history/area?bbox={}&month={}
  GET /api/thermal-history/best-regions?month={}&min_score={}
"""

from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
import logging
from typing import Optional, List, Dict

# Note: In production, these would be actual imports
# from atmosphere.solar_nowcast import get_nowcast_at_point, SunshineNowcast
# from atmosphere.thermal_history import get_thermal_history, find_best_regions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["nowcast", "thermal-history"])


# ═══════════════════════════════════════════════════════════
# NOWCAST ENDPOINTS
# ═══════════════════════════════════════════════════════════

@router.get("/nowcast/thermal")
async def get_thermal_nowcast(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude")
):
    """
    Get real-time thermal nowcast for a location.
    
    Returns current sunshine, cloud conditions, and thermal probability,
    plus 2-hour forecast in 15-minute steps.
    
    Response:
    {
      "timestamp_utc": "2026-03-03T14:30:00Z",
      "data_age_minutes": 2.3,
      "sunshine_fraction": 0.85,
      "shadow_fraction": 0.05,
      "cloud_fraction": 0.10,
      "thermal_probability": 0.72,
      "current_climb_rate_ms": 1.44,
      "trigger_margin_c": 3.2,
      "cloud_cover_pct": 15,
      "forecast": [
        {
          "minutes_ahead": 15,
          "sunshine_fraction": 0.82,
          "thermal_probability": 0.68,
          "cloud_approaching": false
        },
        ...
      ],
      "pilot_message": "☀️ Vollsonne seit 22 Min. Thermik aktiv, ~1.5 m/s."
    }
    """
    try:
        # In production: fetch latest nowcast from Redis/database
        # nowcast = redis_client.get("nowcast:latest")
        # if not nowcast:
        #     raise HTTPException(status_code=503, detail="Nowcast not available")
        # 
        # nowcast_point = get_nowcast_at_point(lat, lon, nowcast)
        
        # Placeholder response
        return {
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "data_age_minutes": 2.3,
            "sunshine_fraction": 0.85,
            "shadow_fraction": 0.05,
            "cloud_fraction": 0.10,
            "thermal_probability": 0.72,
            "current_climb_rate_ms": 1.44,
            "trigger_margin_c": 3.2,
            "cloud_cover_pct": 15,
            "forecast": [
                {
                    "minutes_ahead": 15,
                    "sunshine_fraction": 0.82,
                    "thermal_probability": 0.68,
                    "cloud_approaching": False
                },
                {
                    "minutes_ahead": 30,
                    "sunshine_fraction": 0.75,
                    "thermal_probability": 0.60,
                    "cloud_approaching": True
                },
                {
                    "minutes_ahead": 60,
                    "sunshine_fraction": 0.88,
                    "thermal_probability": 0.74,
                    "cloud_approaching": False
                }
            ],
            "pilot_message": "☀️ Vollsonne seit 22 Min. Thermik aktiv, ~1.4 m/s."
        }
    except Exception as e:
        logger.error(f"Error in thermal nowcast: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nowcast/sunshine-map")
async def get_sunshine_map(
    bbox: str = Query(
        ..., 
        description="Bounding box as 'lat_min,lon_min,lat_max,lon_max'"
    )
):
    """
    Get GeoJSON grid of current sunshine fraction.
    
    Updates every 15 minutes from satellite imagery.
    
    Response:
    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "geometry": {"type": "Point", "coordinates": [lon, lat]},
          "properties": {
            "sunshine_fraction": 0.85,
            "cloud_fraction": 0.10,
            "thermal_probability": 0.72
          }
        },
        ...
      ],
      "timestamp_utc": "2026-03-03T14:30:00Z",
      "update_interval_minutes": 15
    }
    """
    try:
        # Parse bbox
        bbox_parts = bbox.split(',')
        lat_min, lon_min, lat_max, lon_max = map(float, bbox_parts)
        
        # In production: fetch from nowcast tile cache
        
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon_min, lat_min]},
                    "properties": {
                        "sunshine_fraction": 0.85,
                        "cloud_fraction": 0.10,
                        "thermal_probability": 0.72
                    }
                }
            ],
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "update_interval_minutes": 15
        }
    except Exception as e:
        logger.error(f"Error in sunshine map: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ═══════════════════════════════════════════════════════════
# THERMAL HISTORY ENDPOINTS
# ═══════════════════════════════════════════════════════════

@router.get("/thermal-history")
async def get_thermal_history(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    include_today: bool = Query(True, description="Include today vs climatology comparison")
):
    """
    Get historical thermal quality statistics for a location.
    
    Includes monthly climatology from ERA5 reanalysis and (if available)
    real pilot observations from OGN network.
    
    Response:
    {
      "location": {"lat": 46.5, "lon": 8.5},
      "query_time_utc": "2026-03-03T14:30:00Z",
      "monthly_stats": [
        {
          "month": 1,
          "month_name": "Januar",
          "thermal_quality_score": 42,
          "soaring_days_pct": 25,
          "mean_cape": 150,
          "mean_bl_height_m": 1100,
          "mean_climb_rate_ms": 1.2,
          "p75_climb_rate_ms": 2.1,
          "best_hour_of_day": 14,
          "observation_count": 245,
          "data_source": "era5+ogn"
        },
        ...
      ],
      "best_months": [7, 8, 6],
      "best_hour_overall": 14,
      "today_vs_climatology": {
        "anomaly_pct": 25,
        "anomaly_label": "above_average",
        "comparison_text": "Heute +25% CAPE vs. Klimamittel März. Bewertung: überdurchschnittlich.",
        "percentile_rank": 68
      },
      "ogn_data_available": true,
      "ogn_observations_total": 3250,
      "data_sources": ["era5", "ogn"]
    }
    """
    try:
        # In production: query from PostgreSQL climatology table
        # clim_db = await db.query(era5_climatology).filter(...)
        # ogn_db = await db.query(ogn_area_statistics).filter(...)
        
        # Placeholder response
        return {
            "location": {"lat": lat, "lon": lon},
            "query_time_utc": datetime.utcnow().isoformat() + "Z",
            "monthly_stats": [
                {
                    "month": 1,
                    "month_name": "Januar",
                    "thermal_quality_score": 42,
                    "soaring_days_pct": 25,
                    "mean_cape": 150,
                    "mean_bl_height_m": 1100,
                    "mean_climb_rate_ms": 1.2,
                    "p75_climb_rate_ms": 2.1,
                    "best_hour_of_day": 14,
                    "observation_count": 245,
                    "data_source": "era5+ogn"
                },
                {
                    "month": 7,
                    "month_name": "Juli",
                    "thermal_quality_score": 78,
                    "soaring_days_pct": 85,
                    "mean_cape": 350,
                    "mean_bl_height_m": 1900,
                    "mean_climb_rate_ms": 2.8,
                    "p75_climb_rate_ms": 4.2,
                    "best_hour_of_day": 14,
                    "observation_count": 1245,
                    "data_source": "era5+ogn"
                }
            ],
            "best_months": [7, 8, 6],
            "best_hour_overall": 14,
            "today_vs_climatology": {
                "anomaly_pct": 25,
                "anomaly_label": "above_average",
                "comparison_text": "Heute +25% CAPE vs. Klimamittel März. Bewertung: überdurchschnittlich.",
                "percentile_rank": 68
            },
            "ogn_data_available": True,
            "ogn_observations_total": 3250,
            "data_sources": ["era5", "ogn"]
        }
    except Exception as e:
        logger.error(f"Error in thermal history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/thermal-history/area")
async def get_thermal_history_area(
    bbox: str = Query(
        ..., 
        description="Bounding box as 'lat_min,lon_min,lat_max,lon_max'"
    ),
    month: int = Query(1, ge=1, le=12, description="Month (1-12)"),
    data_source: str = Query("era5", regex="^(era5|ogn|both)$")
):
    """
    Get GeoJSON grid of thermal quality by location for a month.
    
    Used to generate map overlays showing best soaring regions.
    
    Response:
    {
      "type": "FeatureCollection",
      "month": 7,
      "month_name": "Juli",
      "data_source": "era5",
      "features": [
        {
          "type": "Feature",
          "geometry": {"type": "Point", "coordinates": [8.5, 46.5]},
          "properties": {
            "thermal_quality_score": 78,
            "soaring_days_pct": 85,
            "mean_cape": 350,
            "mean_climb_rate_ms": 2.8
          }
        },
        ...
      ]
    }
    """
    try:
        bbox_parts = bbox.split(',')
        lat_min, lon_min, lat_max, lon_max = map(float, bbox_parts)
        
        # In production: query from climatology tiles/database
        
        return {
            "type": "FeatureCollection",
            "month": month,
            "month_name": [
                'Januar', 'Februar', 'März', 'April', 'Mai', 'Juni',
                'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember'
            ][month - 1],
            "data_source": data_source,
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [8.5, 46.5]},
                    "properties": {
                        "thermal_quality_score": 78,
                        "soaring_days_pct": 85,
                        "mean_cape": 350,
                        "mean_climb_rate_ms": 2.8
                    }
                }
            ]
        }
    except Exception as e:
        logger.error(f"Error in thermal history area: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/thermal-history/best-regions")
async def get_best_regions(
    month: int = Query(7, ge=1, le=12, description="Month (1-12)"),
    min_score: float = Query(70, ge=0, le=100, description="Minimum quality score")
):
    """
    Find best soaring regions in Europe for given month.
    
    Useful for trip planning and identifying hotspots.
    
    Response:
    [
      {
        "region_name": "Alpen (Schweiz)",
        "center_lat": 46.5,
        "center_lon": 8.5,
        "thermal_quality_score": 75,
        "soaring_days_pct": 65,
        "mean_bl_height_m": 1800,
        "data_source": "era5+ogn"
      },
      ...
    ]
    """
    try:
        # In production: find/cluster climatology points by quality
        # regions = await find_best_regions(climatology_db, month, min_score)
        
        return [
            {
                "region_name": "Alpen (Schweiz)",
                "center_lat": 46.5,
                "center_lon": 8.5,
                "thermal_quality_score": 75,
                "soaring_days_pct": 65,
                "mean_bl_height_m": 1800,
                "data_source": "era5+ogn"
            },
            {
                "region_name": "Frankreich (Massif Central)",
                "center_lat": 45.5,
                "center_lon": 3.5,
                "thermal_quality_score": 72,
                "soaring_days_pct": 60,
                "mean_bl_height_m": 1700,
                "data_source": "era5+ogn"
            },
            {
                "region_name": "Spanien (Kastilien)",
                "center_lat": 42.0,
                "center_lon": -4.0,
                "thermal_quality_score": 78,
                "soaring_days_pct": 70,
                "mean_bl_height_m": 1900,
                "data_source": "era5"
            }
        ]
    except Exception as e:
        logger.error(f"Error in best regions: {e}")
        raise HTTPException(status_code=500, detail=str(e))
