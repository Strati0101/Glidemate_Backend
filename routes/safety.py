"""
Safety Module FastAPI Integration

API endpoints for:
- NOTAM queries (FAA + OpenAIP)
- Föhn detection
- Thunderstorm warnings
- Unified safety dashboard
"""

import logging
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import os

# Import safety modules
from safety.notam import init_notam_manager, NOTAMManager
from safety.foehn import FoehnDetector, FOEHN_REGIONS, init_foehn_detector
from safety.thunderstorm import (
    AlertLevelCalculator, ConvectiveRiskAssessor,
    init_thunderstorm_system
)
from safety.alerts import init_alert_system, SafetyAlertEngine

logger = logging.getLogger("safety.api")

# Initialize systems
notam_manager: Optional[NOTAMManager] = None
foehn_detector: Optional[FoehnDetector] = None
thunderstorm_system: Optional[Dict] = None
alert_engine: Optional[SafetyAlertEngine] = None


# ========================
# Pydantic Models
# ========================
class NOTAMResponse(BaseModel):
    """NOTAM response"""
    notam_number: str
    icao: Optional[str]
    notam_type: str
    priority: str
    effective_start: str
    effective_end: str
    full_text: str
    min_altitude_m: Optional[int]
    max_altitude_m: Optional[int]
    affects_gliders: bool
    source: str


class FoehnResponse(BaseModel):
    """Föhn detection response"""
    foehn_possible: bool
    foehn_likely: bool
    foehn_confirmed: bool
    foehn_score: float
    foehn_index: str
    affected_regions: List[Dict]
    dp_hpa: Optional[float]
    wind_dir_crest: Optional[float]
    t_anomaly_c: Optional[float]
    rh_valley: Optional[float]
    collapse_risk: bool
    collapse_window_hours: Optional[int]
    soaring_assessment: str
    forecast_6h: List[float]


class ThunderstormResponse(BaseModel):
    """Thunderstorm risk response"""
    alert_level: int
    alert_message: str
    nearest_cell_km: Optional[float]
    nearest_cell_bearing_deg: Optional[float]
    nearest_cell_intensity: Optional[str]
    cell_moving_toward_user: bool
    eta_minutes: Optional[int]
    lightning_count_50km_1h: int
    convective_risk: str
    storm_cells: List[Dict]
    cape_jkg: Optional[float]
    k_index: Optional[float]
    total_totals: Optional[float]


class SafetySummaryResponse(BaseModel):
    """Unified safety dashboard"""
    overall_safety: str  # 'green', 'yellow', 'orange', 'red'
    thunderstorm: Dict
    foehn: Dict
    notams: Dict
    airspace: Dict
    cb_warning: Optional[Dict] = None  # Cb development warning
    fog_visibility: Optional[Dict] = None  # Fog and visibility
    icing_risk: Optional[Dict] = None  # Icing conditions
    surface_conditions: Optional[Dict] = None  # Snow/soil moisture
    pilot_action: str
    timestamp: str


# ========================
# NOTAM Endpoints
# ========================
async def _init_notam_manager():
    """Initialize NOTAM manager on first use"""
    global notam_manager
    if notam_manager is None:
        from safety.notam import init_notam_manager
        notam_manager = init_notam_manager()
    return notam_manager


async def get_notams_for_area(
    bbox: str = Query(
        ...,
        description="Bounding box: minLon,minLat,maxLon,maxLat"
    ),
    hours: int = Query(24, description="Hours ahead to check")
) -> List[NOTAMResponse]:
    """
    GET /api/notams?bbox={minLon,minLat,maxLon,maxLat}&hours=24
    
    Get all NOTAMs affecting area in GeoJSON format
    """
    try:
        manager = await _init_notam_manager()
        
        # Parse bbox
        parts = bbox.split(",")
        if len(parts) != 4:
            raise ValueError("bbox must be 4 comma-separated values")
        
        min_lon, min_lat, max_lon, max_lat = map(float, parts)
        
        # Fetch NOTAMs
        notams = await manager.fetch_notams_for_area(
            min_lon, min_lat, max_lon, max_lat, hours
        )
        
        return [
            NOTAMResponse(
                notam_number=n.notam_number,
                icao=n.icao,
                notam_type=n.notam_type,
                priority=n.priority,
                effective_start=n.effective_start.isoformat(),
                effective_end=n.effective_end.isoformat(),
                full_text=n.full_text,
                min_altitude_m=n.min_altitude_m,
                max_altitude_m=n.max_altitude_m,
                affects_gliders=n.affects_gliders,
                source=n.source,
            )
            for n in notams
        ]
    
    except Exception as e:
        logger.error(f"NOTAM fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def get_notams_for_airfield(
    icao: str = Query(..., description="ICAO code")
) -> List[NOTAMResponse]:
    """
    GET /api/notams/airfield?icao={ICAO}
    
    Get all active NOTAMs for specific airfield
    """
    try:
        manager = await _init_notam_manager()
        notams = await manager.faa_client.fetch_notams_by_icao(icao)
        
        return [
            NOTAMResponse(
                notam_number=n.notam_number,
                icao=n.icao,
                notam_type=n.notam_type,
                priority=n.priority,
                effective_start=n.effective_start.isoformat(),
                effective_end=n.effective_end.isoformat(),
                full_text=n.full_text,
                min_altitude_m=n.min_altitude_m,
                max_altitude_m=n.max_altitude_m,
                affects_gliders=n.affects_gliders,
                source=n.source,
            )
            for n in notams
        ]
    
    except Exception as e:
        logger.error(f"Airfield NOTAM error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def get_notams_for_route(
    lat1: float = Query(...),
    lon1: float = Query(...),
    lat2: float = Query(...),
    lon2: float = Query(...),
    altitude_m: int = Query(2000),
) -> List[NOTAMResponse]:
    """
    GET /api/notams/route?lat1={lat}&lon1={lon}&lat2={lat}&lon2={lon}&altitude_m=2000
    
    Get NOTAMs intersecting planned route
    """
    try:
        manager = await _init_notam_manager()
        notams = await manager.fetch_notams_for_route(lat1, lon1, lat2, lon2, altitude_m)
        
        return [
            NOTAMResponse(
                notam_number=n.notam_number,
                icao=n.icao,
                notam_type=n.notam_type,
                priority=n.priority,
                effective_start=n.effective_start.isoformat(),
                effective_end=n.effective_end.isoformat(),
                full_text=n.full_text,
                min_altitude_m=n.min_altitude_m,
                max_altitude_m=n.max_altitude_m,
                affects_gliders=n.affects_gliders,
                source=n.source,
            )
            for n in notams
        ]
    
    except Exception as e:
        logger.error(f"Route NOTAM error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================
# Föhn Endpoints
# ========================
async def get_foehn_status(
    lat: float = Query(...),
    lon: float = Query(...),
) -> FoehnResponse:
    """
    GET /api/foehn?lat={lat}&lon={lon}
    
    Get föhn status for location (finds nearest föhn region)
    """
    try:
        global foehn_detector
        if foehn_detector is None:
            foehn_detector = init_foehn_detector()
        
        # TODO: Get actual NWP data from database
        # For now, return template response
        
        return FoehnResponse(
            foehn_possible=False,
            foehn_likely=False,
            foehn_confirmed=False,
            foehn_score=0.0,
            foehn_index="none",
            affected_regions=[],
            dp_hpa=None,
            wind_dir_crest=None,
            t_anomaly_c=None,
            rh_valley=None,
            collapse_risk=False,
            collapse_window_hours=None,
            soaring_assessment="No föhn conditions detected.",
            forecast_6h=[0.0] * 6,
        )
    
    except Exception as e:
        logger.error(f"Föhn status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def get_foehn_map() -> JSONResponse:
    """
    GET /api/foehn/map
    
    Get föhn status as GeoJSON for all regions
    """
    try:
        features = []
        
        for region_id, region in FOEHN_REGIONS.items():
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [region["lon"], region["lat"]]
                },
                "properties": {
                    "id": region_id,
                    "name": region["name"],
                    "country": region["country"],
                    # TODO: Add actual föhn status
                    "foehn_status": "none",
                }
            }
            features.append(feature)
        
        return JSONResponse({
            "type": "FeatureCollection",
            "features": features
        })
    
    except Exception as e:
        logger.error(f"Föhn map error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================
# Thunderstorm Endpoints
# ========================
async def get_thunderstorm_status(
    lat: float = Query(...),
    lon: float = Query(...),
) -> ThunderstormResponse:
    """
    GET /api/thunderstorm/status?lat={lat}&lon={lon}
    
    Get thunderstorm risk and alert level
    """
    try:
        global thunderstorm_system
        if thunderstorm_system is None:
            thunderstorm_system = init_thunderstorm_system()
        
        # TODO: Get actual lightning and storm cell data
        risk = AlertLevelCalculator.calculate_alert(
            lat, lon, [], [], "none"
        )
        
        return ThunderstormResponse(
            alert_level=risk.alert_level,
            alert_message=risk.alert_message,
            nearest_cell_km=risk.nearest_cell_km,
            nearest_cell_bearing_deg=risk.nearest_cell_bearing_deg,
            nearest_cell_intensity=risk.nearest_cell_intensity,
            cell_moving_toward_user=risk.cell_moving_toward,
            eta_minutes=risk.eta_minutes,
            lightning_count_50km_1h=risk.lightning_count_50km_1h,
            convective_risk=risk.convective_risk,
            storm_cells=[],
            cape_jkg=risk.cape_jkg,
            k_index=risk.k_index,
            total_totals=risk.total_totals,
        )
    
    except Exception as e:
        logger.error(f"Thunderstorm status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def get_lightning_live(
    bbox: str = Query(
        ...,
        description="Bounding box: minLon,minLat,maxLon,maxLat"
    ),
    minutes: int = Query(30, description="Time window in minutes"),
) -> JSONResponse:
    """
    GET /api/lightning/live?bbox={}&minutes=30
    
    Get recent lightning strikes as GeoJSON
    """
    try:
        # TODO: Get from Redis or database
        
        return JSONResponse({
            "type": "FeatureCollection",
            "features": []  # Will be populated from live data
        })
    
    except Exception as e:
        logger.error(f"Lightning data error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def get_storm_cells(
    bbox: str = Query(...),
) -> JSONResponse:
    """
    GET /api/storm-cells?bbox={}
    
    Get active storm cell polygons
    """
    try:
        # TODO: Get from storm tracking system
        
        return JSONResponse({
            "type": "FeatureCollection",
            "features": []  # Will be populated from storm data
        })
    
    except Exception as e:
        logger.error(f"Storm cells error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================
# Unified Safety Dashboard
# ========================
async def get_safety_summary(
    lat: float = Query(...),
    lon: float = Query(...),
) -> SafetySummaryResponse:
    """
    GET /api/safety/summary?lat={lat}&lon={lon}
    
    Complete safety overview for pilot including:
    - Thunderstorm & Cb development
    - Föhn conditions
    - Fog & visibility
    - Icing conditions
    - Surface conditions (snow, soil moisture)
    - NOTAMs
    """
    try:
        # Get all safety data
        tstorm = await get_thunderstorm_status(lat, lon)
        foehn = await get_foehn_status(lat, lon)
        
        # Get parcel analysis with safety weather modules
        from atmosphere.parcel import AtmosphericProfile, run_parcel_analysis
        try:
            # Create minimal profile for safety analysis
            # In production, would fetch from ERA5/ICON-EU data
            profile = AtmosphericProfile(
                lat=lat,
                lon=lon,
                valid_time=datetime.utcnow(),
                model_source="ICON-EU",
                levels=[],  # Would be populated from model data
                surface_temp_c=15,
                surface_dewpoint_c=10,
                surface_pressure_hpa=1013,
                solar_radiation_wm2=500
            )
            parcel_result = run_parcel_analysis(profile)
            
            cb_warning = parcel_result.cb_development
            fog_data = parcel_result.fog_visibility
            icing_data = parcel_result.icing
            surface_data = parcel_result.snow_soil
        except Exception as e:
            logger.warning(f"Weather model analysis failed: {e}")
            cb_warning = None
            fog_data = None
            icing_data = None
            surface_data = None
        
        # Get safety status
        global alert_engine
        if alert_engine is None:
            alert_system = init_alert_system()
            alert_engine = alert_system["engine"]
        
        summary = alert_engine.get_overall_safety_status(
            thunderstorm_risk=tstorm.dict(),
            foehn_info=foehn.dict(),
            notam_count_high=0,
        )
        
        return SafetySummaryResponse(
            overall_safety=summary["overall_safety"],
            thunderstorm={
                "alert_level": tstorm.alert_level,
                "nearest_km": tstorm.nearest_cell_km,
                "eta_min": tstorm.eta_minutes,
            },
            foehn={
                "confirmed": foehn.foehn_confirmed,
                "collapse_risk": foehn.collapse_risk,
                "wind_speed_kmh": 0,
            },
            notams={
                "count_high_priority": 0,
                "nearest_notam": None,
            },
            airspace={
                "violations_risk": False,
                "restricted_nearby": False,
            },
            cb_warning=cb_warning,
            fog_visibility=fog_data,
            icing_risk=icing_data,
            surface_conditions=surface_data,
            pilot_action=summary["pilot_action"],
            timestamp=datetime.utcnow().isoformat(),
        )
    
    except Exception as e:
        logger.error(f"Safety summary error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================
# Push Notification Endpoint
# ========================
async def send_push_notification(
    device_token: str = Query(...),
    alert_level: int = Query(...),
    message: str = Query(...),
) -> JSONResponse:
    """
    POST /api/push/notify
    
    Send push notification to device
    (for internal use by alert system)
    """
    try:
        global alert_engine
        if alert_engine is None:
            alert_system = init_alert_system()
            alert_engine = alert_system["engine"]
        
        # TODO: Send via alert system
        
        return JSONResponse({
            "success": True,
            "message": "Notification queued",
        })
    
    except Exception as e:
        logger.error(f"Push notification error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================
# Router Setup
# ========================
def create_safety_router() -> APIRouter:
    """Create FastAPI router for safety module"""
    
    router = APIRouter(prefix="/api/safety", tags=["Safety"])
    
    # NOTAM endpoints
    router.add_api_route(
        "/notams",
        get_notams_for_area,
        methods=["GET"],
        summary="Get NOTAMs for area"
    )
    router.add_api_route(
        "/notams/airfield",
        get_notams_for_airfield,
        methods=["GET"],
        summary="Get NOTAMs for airfield"
    )
    router.add_api_route(
        "/notams/route",
        get_notams_for_route,
        methods=["GET"],
        summary="Get NOTAMs for planned route"
    )
    
    # Föhn endpoints
    router.add_api_route(
        "/foehn",
        get_foehn_status,
        methods=["GET"],
        summary="Get föhn status"
    )
    router.add_api_route(
        "/foehn/map",
        get_foehn_map,
        methods=["GET"],
        summary="Get föhn status map"
    )
    
    # Thunderstorm endpoints
    router.add_api_route(
        "/thunderstorm/status",
        get_thunderstorm_status,
        methods=["GET"],
        summary="Get thunderstorm risk"
    )
    router.add_api_route(
        "/lightning/live",
        get_lightning_live,
        methods=["GET"],
        summary="Get live lightning data"
    )
    router.add_api_route(
        "/storm-cells",
        get_storm_cells,
        methods=["GET"],
        summary="Get storm cell positions"
    )
    
    # Unified dashboard
    router.add_api_route(
        "/summary",
        get_safety_summary,
        methods=["GET"],
        summary="Get unified safety summary"
    )
    
    # Push notifications
    router.add_api_route(
        "/push/notify",
        send_push_notification,
        methods=["POST"],
        summary="Send push notification"
    )
    
    logger.info("Safety API router created with 8 endpoints")
    return router
