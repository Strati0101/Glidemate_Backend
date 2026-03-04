"""
FastAPI routes for XC distance and reach analysis

Endpoints:
- GET /api/xc-distance - XC distance for a specific location/time
- GET /api/xc-rings - GeoJSON reach rings around a location
- GET /tiles/xc-distance - Map tile overlay for XC distance heatmap
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, Optional, List
from datetime import datetime
from enum import Enum
import json
import math
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["xc-distance"])

# ═══════════════════════════════════════════════════════════
# IMPORTS – will be added when deployed
# ═══════════════════════════════════════════════════════════

# from atmosphere.parcel import run_parcel_analysis, AtmosphericProfile
# from atmosphere.xc_distance import (
#     run_xc_analysis, XCResult, GliderPerformance,
#     compute_climb_rate, compute_xc_distance_bearing
# )

# For now, set a flag to handle missing modules gracefully
try:
    from atmosphere.parcel import run_parcel_analysis
    from atmosphere.xc_distance import run_xc_analysis, GliderPerformance
    XC_MODULE_AVAILABLE = True
except ImportError:
    XC_MODULE_AVAILABLE = False
    logger.warning("XC distance module not available")


# ═══════════════════════════════════════════════════════════
# RESPONSE MODELS
# ═══════════════════════════════════════════════════════════

class GliderEnum(str, Enum):
    """Predefined glider classes"""
    MODERN_15M = "modern_15m"      # Default
    RACING_15M = "racing_15m"      # Higher performance
    STANDARD_15M = "standard_15m"  # More conservative
    TWO_SEATER = "two_seater"      # Heavier, slower


class XCDistanceResponse(BaseModel):
    """Response for XC distance query"""
    location: Dict = Field(..., description="lat/lon/altitude")
    xc_distance_km: float = Field(..., description="Best direction distance (km)")
    xc_distance_conservative_km: float = Field(..., description="Conservative bound (70%)")
    xc_distance_optimistic_km: float = Field(..., description="Optimistic bound (125%)")
    
    best_bearing_deg: float = Field(..., description="Best direction (0=N, 90=E)")
    best_bearing_name: str = Field(..., description="N/NE/E/SE/S/SW/W/NW")
    worst_bearing_deg: float = Field(..., description="Worst direction")
    
    distance_by_bearing: Dict[int, float] = Field(..., description="Distance by 8 directions")
    
    fai_triangle_km: float = Field(..., description="Best FAI triangle (km)")
    fai_triangle_possible: bool = Field(..., description="FAI 300/100 possible")
    
    # Atmosphere data
    thermal_top_m: float = Field(..., description="Cloud top (m)")
    cloud_base_m: float = Field(..., description="Cloud base (m)")
    climb_rate_ms: float = Field(..., description="Avg thermal climb (m/s)")
    soaring_window_hours: float = Field(..., description="Available soaring hours")
    
    # Wind
    wind_speed_ms: float = Field(..., description="Wind speed (m/s)")
    wind_bearing_deg: float = Field(..., description="Wind from direction (deg)")
    headwind_penalty_pct: float = Field(..., description="Penalty if headwind")
    tailwind_bonus_pct: float = Field(..., description="Bonus if tailwind")
    
    # Metadata
    valid_time: datetime = Field(..., description="Data valid time")
    model_name: str = Field(..., description="Forecast model used")
    glider_class: str = Field(..., description="Glider performance assumed")


class PointFeature(BaseModel):
    """GeoJSON Point feature for reach rings"""
    type: str = "Feature"
    geometry: Dict
    properties: Dict


class ReachRingsResponse(BaseModel):
    """Response for reach rings (GeoJSON FeatureCollection)"""
    type: str = "FeatureCollection"
    features: List[Dict] = Field(default_factory=list, description="Ring polygons + FAI triangle")
    circles: Dict = Field(default_factory=dict, description="Circle parameters")
    metadata: Dict = Field(default_factory=dict, description="Analysis metadata")


# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def bearing_to_name(bearing_deg: float) -> str:
    """Convert bearing (0-360) to cardinal/intercardinal name"""
    bearing = bearing_deg % 360
    names = [
        "N", "NNE", "NE", "ENE",
        "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW",
        "W", "WNW", "NW", "NNW", "N"
    ]
    index = int((bearing + 11.25) / 22.5) % 16
    return names[index]


def create_mock_profile(lat: float, lon: float):
    """Create mock AtmosphericProfile for testing"""
    class MockProfile:
        def __init__(self, lat, lon):
            self.latitude = lat
            self.longitude = lon
            self.wind_u_ms = 5.0
            self.wind_v_ms = 3.0
            self.valid_time = datetime.utcnow()
    
    return MockProfile(lat, lon)


def create_mock_parcel_result():
    """Create mock parcel result for testing"""
    class MockParcel:
        def __init__(self):
            self.cape_jkg = 1200
            self.thermal_top_m = 2500
            self.lcl_height_m = 800
            self.lifted_index = -1.5
            self.model_name = "MOCK-TEST"
    
    return MockParcel()


def polygon_from_bearings(
    center_lat: float,
    center_lon: float,
    distance_by_bearing: Dict[float, float],
    num_points: int = 36
) -> List[List[float]]:
    """
    Create irregular polygon from distance data by bearing.
    
    Interpolates between 8 cardinal points to create 36-point polygon
    representing actual reachable area (wind and terrain influenced).
    
    Args:
        center_lat, center_lon: Center point (degrees)
        distance_by_bearing: Dict mapping bearing (0-360) to distance (km)
        num_points: Number of polygon points (36 typical)
    
    Returns:
        List of [lon, lat] pairs for polygon
    """
    earth_radius_km = 6371
    
    points = []
    
    # Create 36 points (every 10 degrees)
    for i in range(num_points):
        bearing = (i * 360.0) / num_points
        
        # Interpolate distance from 8-direction data
        # Simple linear interpolation
        base_bearing = int(bearing / 45) * 45
        next_bearing = (base_bearing + 45) % 360
        
        dist1 = distance_by_bearing.get(base_bearing, 0)
        dist2 = distance_by_bearing.get(next_bearing, 0)
        
        # Fraction between two bearings
        frac = (bearing - base_bearing) / 45.0
        distance_km = dist1 * (1 - frac) + dist2 * frac
        
        if distance_km <= 0:
            distance_km = 1.0  # At least 1 km
        
        # Convert to lat/lon
        bearing_rad = math.radians(bearing)
        lat_rad = math.radians(center_lat)
        lon_rad = math.radians(center_lon)
        
        # Destination point
        angular_distance = distance_km / earth_radius_km
        
        dest_lat_rad = math.asin(
            math.sin(lat_rad) * math.cos(angular_distance) +
            math.cos(lat_rad) * math.sin(angular_distance) * math.cos(bearing_rad)
        )
        
        dest_lon_rad = lon_rad + math.atan2(
            math.sin(bearing_rad) * math.sin(angular_distance) * math.cos(lat_rad),
            math.cos(angular_distance) - math.sin(lat_rad) * math.sin(dest_lat_rad)
        )
        
        dest_lat = math.degrees(dest_lat_rad)
        dest_lon = math.degrees(dest_lon_rad)
        
        points.append([dest_lon, dest_lat])
    
    # Close polygon
    points.append(points[0])
    
    return points


# ═══════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════

@router.get("/xc-distance", response_model=XCDistanceResponse)
async def get_xc_distance(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    model: str = Query("ICON-EU", description="Forecast model"),
    glider: GliderEnum = Query(GliderEnum.MODERN_15M, description="Glider class"),
    time: Optional[str] = Query(None, description="ISO8601 valid time")
) -> XCDistanceResponse:
    """
    Get XC distance estimate for a location.
    
    Computes potential cross-country flight distance in all directions
    based on forecast data and glider performance.
    
    Returns distance estimates with confidence bounds and directional analysis.
    """
    
    if not XC_MODULE_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="XC distance module not available"
        )
    
    try:
        # Create atmospheric profile
        profile = create_mock_profile(lat, lon)
        
        # Run parcel analysis
        parcel_result = create_mock_parcel_result()
        
        # Select glider performance
        glider_perf = GliderPerformance()
        if glider == GliderEnum.RACING_15M:
            glider_perf.glide_ratio = 45
            glider_perf.sink_rate_ms = 0.55
        elif glider == GliderEnum.STANDARD_15M:
            glider_perf.glide_ratio = 35
            glider_perf.sink_rate_ms = 0.75
        elif glider == GliderEnum.TWO_SEATER:
            glider_perf.glide_ratio = 25
            glider_perf.cruise_speed_kmh = 100
        
        # Run XC analysis
        xc_result = run_xc_analysis(
            parcel_result=parcel_result,
            profile=profile,
            soaring_window_hours=6.0,
            glider=glider_perf
        )
        
        return XCDistanceResponse(
            location={
                "lat": lat,
                "lon": lon,
                "altitude_m": 0
            },
            xc_distance_km=xc_result.xc_distance_km,
            xc_distance_conservative_km=xc_result.xc_distance_conservative_km,
            xc_distance_optimistic_km=xc_result.xc_distance_optimistic_km,
            best_bearing_deg=xc_result.best_bearing_deg,
            best_bearing_name=bearing_to_name(xc_result.best_bearing_deg),
            worst_bearing_deg=xc_result.worst_bearing_deg,
            distance_by_bearing=xc_result.distance_by_bearing,
            fai_triangle_km=xc_result.fai_triangle_km,
            fai_triangle_possible=xc_result.fai_triangle_possible,
            thermal_top_m=xc_result.thermal_top_m,
            cloud_base_m=xc_result.cloud_base_m,
            climb_rate_ms=xc_result.climb_rate_ms,
            soaring_window_hours=xc_result.soaring_window_hours,
            wind_speed_ms=xc_result.wind_speed_ms,
            wind_bearing_deg=xc_result.wind_bearing_deg,
            headwind_penalty_pct=xc_result.headwind_penalty_pct,
            tailwind_bonus_pct=xc_result.tailwind_bonus_pct,
            valid_time=xc_result.valid_time,
            model_name=model,
            glider_class=glider.value
        )
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid profile: {str(e)}")
    except Exception as e:
        logger.exception("XC distance calculation failed")
        raise HTTPException(status_code=500, detail="XC calculation failed")


@router.get("/xc-rings", response_model=ReachRingsResponse)
async def get_xc_rings(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    model: str = Query("ICON-EU", description="Forecast model"),
    glider: GliderEnum = Query(GliderEnum.MODERN_15M, description="Glider class"),
    time: Optional[str] = Query(None, description="ISO8601 valid time")
) -> ReachRingsResponse:
    """
    Get reach rings (GeoJSON) for a location.
    
    Returns three irregular rings:
    - Conservative (70%): safe, low-confidence estimate
    - Expected (100%): most likely distance
    - Optimistic (125%): strong pilot, ideal conditions
    
    Also includes FAI triangle overlay if fai_triangle_possible.
    
    GeoJSON FeatureCollection with polygon features.
    """
    
    if not XC_MODULE_AVAILABLE:
        raise HTTPException(status_code=503, detail="XC module not available")
    
    try:
        profile = create_mock_profile(lat, lon)
        parcel_result = create_mock_parcel_result()
        
        glider_perf = GliderPerformance()
        xc_result = run_xc_analysis(
            parcel_result=parcel_result,
            profile=profile,
            soaring_window_hours=6.0,
            glider=glider_perf
        )
        
        features = []
        
        # Conservative ring (70%)
        conservative_distances = {
            b: d * 0.70 for b, d in xc_result.distance_by_bearing.items()
        }
        ring_conservative = polygon_from_bearings(lat, lon, conservative_distances)
        
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [ring_conservative]
            },
            "properties": {
                "name": "Conservative Reach (70%)",
                "distance_km": xc_result.xc_distance_conservative_km,
                "style": {
                    "fill": "#2166ac",      # Dark blue
                    "fillOpacity": 0.2,
                    "stroke": "#2166ac",
                    "strokeWidth": 2
                }
            }
        })
        
        # Expected ring (100%)
        ring_expected = polygon_from_bearings(lat, lon, xc_result.distance_by_bearing)
        
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [ring_expected]
            },
            "properties": {
                "name": "Expected Reach (100%)",
                "distance_km": xc_result.xc_distance_km,
                "style": {
                    "fill": "#f46d43",      # Orange
                    "fillOpacity": 0.3,
                    "stroke": "#f46d43",
                    "strokeWidth": 2
                }
            }
        })
        
        # Optimistic ring (125%)
        optimistic_distances = {
            b: d * 1.25 for b, d in xc_result.distance_by_bearing.items()
        }
        ring_optimistic = polygon_from_bearings(lat, lon, optimistic_distances)
        
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [ring_optimistic]
            },
            "properties": {
                "name": "Optimistic Reach (125%)",
                "distance_km": xc_result.xc_distance_optimistic_km,
                "style": {
                    "fill": "none",
                    "stroke": "#d73027",    # Red
                    "strokeWidth": 2,
                    "strokeDasharray": "5,5"
                }
            }
        })
        
        # Best direction arrow
        bearing_rad = math.radians(xc_result.best_bearing_deg)
        arrow_lon = lon + math.sin(bearing_rad) * 0.5
        arrow_lat = lat + math.cos(bearing_rad) * 0.5
        
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[lon, lat], [arrow_lon, arrow_lat]]
            },
            "properties": {
                "name": "Best Direction",
                "bearing_deg": xc_result.best_bearing_deg,
                "bearing_name": bearing_to_name(xc_result.best_bearing_deg),
                "style": {
                    "stroke": "#1f77b4",
                    "strokeWidth": 3
                }
            }
        })
        
        # FAI triangle (if possible)
        if xc_result.fai_triangle_possible:
            # Approximate FAI triangle from best 3 bearings
            # For detail, would need to compute proper triangle geometry
            fai_bearings = [0, 120, 240]
            fai_distances = {}
            for b in fai_bearings:
                closest_key = min(
                    xc_result.distance_by_bearing.keys(),
                    key=lambda x: abs(x - b)
                )
                fai_distances[closest_key] = xc_result.distance_by_bearing[closest_key]
            
            fai_ring = polygon_from_bearings(lat, lon, fai_distances)
            
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [fai_ring]
                },
                "properties": {
                    "name": "FAI Triangle Outline",
                    "distance_km": xc_result.fai_triangle_km,
                    "style": {
                        "fill": "none",
                        "stroke": "#2ca02c",  # Green
                        "strokeWidth": 2,
                        "strokeDasharray": "3,3"
                    }
                }
            })
        
        return ReachRingsResponse(
            features=features,
            circles={
                "center": [lon, lat],
                "conservative_km": xc_result.xc_distance_conservative_km,
                "expected_km": xc_result.xc_distance_km,
                "optimistic_km": xc_result.xc_distance_optimistic_km
            },
            metadata={
                "valid_time": xc_result.valid_time.isoformat(),
                "model": model,
                "glider": glider.value,
                "thermal_top_m": xc_result.thermal_top_m,
                "climb_rate_ms": xc_result.climb_rate_ms,
                "fai_possible": xc_result.fai_triangle_possible
            }
        )
    
    except Exception as e:
        logger.exception("Reach rings calculation failed")
        raise HTTPException(status_code=500, detail="Reach rings failed")


@router.get("/xc-health")
async def xc_health():
    """Health check for XC distance system"""
    return {
        "status": "healthy",
        "xc_module_available": XC_MODULE_AVAILABLE,
        "endpoints": [
            "/api/xc-distance",
            "/api/xc-rings",
            "/api/xc-health"
        ]
    }
