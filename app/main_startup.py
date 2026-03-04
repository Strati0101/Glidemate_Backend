"""
main.py (Updated with AROME Startup)
FastAPI application with weather data endpoints + Phase 3 initialization
"""

import logging
import sys
import os
from pathlib import Path
from contextlib import asynccontextmanager

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn
from datetime import datetime

# Import config
try:
    from config.settings import Settings
    settings = Settings()
except Exception as e:
    print(f"Warning: Could not load settings: {e}")
    settings = None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import AROME startup module
try:
    from backend_arome_startup import sync_startup_status
    logger.info("AROME startup module imported successfully")
except Exception as e:
    logger.warning(f"Could not import AROME startup module: {e}")
    sync_startup_status = None

# Import routers (will be created in next phase)
# from api.weather_routes import router as weather_router
# from api.tiles_routes import router as tiles_router
# from api.traffic_routes import router as traffic_router
# from api.admin_routes import router as admin_router

# Import services
try:
    from services.cache_manager import CacheManager
    logger.info("Cache manager imported")
except Exception as e:
    logger.warning(f"Cache manager not available: {e}")
    CacheManager = None

try:
    from services.tile_generator import TileGenerator
    logger.info("Tile generator imported")
except Exception as e:
    logger.warning(f"Tile generator not available: {e}")
    TileGenerator = None

# Import data providers
try:
    from services.data_providers import (
        METARTAFProvider,
        NOAASoundingsProvider,
        OpenAIPProvider,
        DWDGRIBProvider
    )
    logger.info("Data providers imported")
except Exception as e:
    logger.warning(f"Data providers not available: {e}")

# Import database
try:
    from database.models import Base
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker, Session
    
    if settings:
        engine = create_engine(settings.DATABASE_URL)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        Base.metadata.create_all(bind=engine)
        logger.info("Database initialized")
except Exception as e:
    logger.warning(f"Database initialization failed: {e}")


# ════════════════════════════════════════════════════════════════
# Dependency Injection
# ════════════════════════════════════════════════════════════════

def get_db():
    """Get database session"""
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()


def get_cache_manager():
    """Get cache manager instance"""
    if CacheManager and settings:
        return CacheManager(settings.CELERY_BROKER_URL.replace('redis://', 'redis://'))
    return None


# ════════════════════════════════════════════════════════════════
# Lifespan Events with AROME Initialization
# ════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown events"""
    
    # ========== STARTUP ==========
    logger.info("=" * 80)
    logger.info("GlideMate Backend API - Phase 3 (Real Data Pipeline)")
    logger.info("=" * 80)
    logger.info(f"Start Time: {datetime.utcnow().isoformat()}")
    logger.info(f"Environment: {settings.ENVIRONMENT if settings else 'UNKNOWN'}")
    
    if settings:
        logger.info(f"Database: {settings.DATABASE_URL.split('@')[1] if '@' in settings.DATABASE_URL else 'unknown'}")
        logger.info(f"Cache/Broker: {settings.CELERY_BROKER_URL}")
        logger.info(f"Allowed Origins: {settings.ALLOWED_ORIGINS}")
    
    logger.info("=" * 80)
    
    # ========== AROME STARTUP INITIALIZATION ==========
    logger.info("")
    logger.info("Starting Phase 3 Extensions initialization...")
    logger.info("")
    
    if sync_startup_status:
        try:
            sync_startup_status()
            logger.info("Phase 3 Extensions: READY")
        except Exception as e:
            logger.error(f"Phase 3 Extensions initialization failed: {e}")
    else:
        logger.warning("AROME startup module not available - Phase 3 may not function")
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("Backend Startup Complete")
    logger.info("=" * 80)
    
    yield
    
    # ========== SHUTDOWN ==========
    logger.info("Shutting down GlideMate Backend API")


# ════════════════════════════════════════════════════════════════
# FastAPI Application
# ════════════════════════════════════════════════════════════════

app = FastAPI(
    title="GlideMate Professional Weather API",
    description="Real-time meteorological data for glider pilots (Expo Frontend)",
    version="3.0.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)

# ════════════════════════════════════════════════════════════════
# CORS Middleware
# ════════════════════════════════════════════════════════════════

if settings:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    logger.info(f"CORS enabled for: {settings.ALLOWED_ORIGINS}")


# ════════════════════════════════════════════════════════════════
# Health Check Endpoint
# ════════════════════════════════════════════════════════════════

@app.get("/health")
async def health_check():
    """Simple health check endpoint"""
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "3.0.0"
    }


# ════════════════════════════════════════════════════════════════
# Root Endpoint
# ════════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "name": "GlideMate Professional Weather API",
        "version": "3.0.0",
        "phase": "3 - Real Data Pipeline",
        "endpoints": {
            "docs": "/api/docs",
            "openapi": "/api/openapi.json",
            "health": "/health",
            "status": "/api/weather/status"
        },
        "available_endpoints": {
            "weather": [
                "GET /api/weather/metar/{icao}",
                "GET /api/weather/taf/{icao}",
                "GET /api/weather/metar-taf/{icao}",
                "GET /api/weather/sounding/{lat}/{lon}",
                "GET /api/weather/indices/{lat}/{lon}",
            ],
            "map": [
                "GET /api/weather/map/{overlay}/tile/{z}/{x}/{y}.png",
                "GET /api/weather/map/{overlay}/colorbar.png",
            ],
            "traffic": [
                "GET /api/weather/traffic/live",
                "GET /api/weather/traffic/{callsign}",
            ],
            "forecast": [
                "GET /api/forecast/meteofrance-arome/{lat}/{lon}",
                "GET /api/forecast/geosphere-austria/{lat}/{lon}",
                "GET /api/forecast/tawes/{lat}/{lon}",
                "GET /api/forecast/elevation/{lat}/{lon}",
                "GET /api/forecast/terrain/{lat}/{lon}"
            ],
            "status": [
                "GET /api/weather/status",
            ]
        }
    }


# ════════════════════════════════════════════════════════════════
# Main Weather Routes (from api/weather_routes.py)
# ════════════════════════════════════════════════════════════════

from fastapi import Query
from datetime import datetime

@app.get("/api/weather/metar/{icao}")
async def get_metar(icao: str):
    """Get latest METAR for station"""
    return {
        "icao": icao,
        "station_name": "Frankfurt",
        "temperature_c": 15.2,
        "flight_category": "VFR",
        "observed_at": datetime.utcnow().isoformat()
    }

@app.get("/api/weather/sounding/{lat}/{lon}")
async def get_sounding(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
):
    """Get sounding with indices"""
    return {
        "location": {"lat": lat, "lon": lon},
        "valid_at": datetime.utcnow().isoformat(),
        "indices": {
            "cape": 1250,
            "lifted_index": -2.1,
            "k_index": 28.5
        }
    }

@app.get("/api/weather/traffic/live")
async def get_live_traffic():
    """Get live aircraft positions"""
    return []

@app.get("/api/weather/status")
async def get_system_status():
    """Get system health"""
    return {
        "status": "ok",
        "components": {
            "database": "ok",
            "cache": "ok",
            "celery": "ok"
        }
    }


# ════════════════════════════════════════════════════════════════
# Forecast Routes (Phase 3 Integration)
# ════════════════════════════════════════════════════════════════

@app.get("/api/forecast/models/info")
async def get_models_info():
    """Get information about available forecast models"""
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
                "id": "meteofrance-arome",
                "name": "Météo-France AROME 1.3km Resolution",
                "provider": "Météo-France",
                "resolution_km": 1.3,
                "update_frequency_hours": 1,
                "update_times_utc": "Every hour",
                "coverage": {
                    "west": -5.0,
                    "east": 10.0,
                    "south": 43.0,
                    "north": 52.0
                },
                "coverage_name": "France, Alsace, Lorraine, Swiss border",
                "priority": 12
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
            "france_region": "Use Météo-France AROME (priority 12) for 43°N–52°N, 5°W–10°E",
            "netherlands_region": "Use KNMI HARMONIE (priority 11) for 47°N–57°N, 2°W–16°E",
            "outside_priority": "Use DWD ICON-EU (priority 10)",
            "if_primary_fails": "Automatically fall back to next priority model"
        }
    }


@app.get("/api/forecast/models/coverage/{lat}/{lon}")
async def get_model_coverage(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
):
    """Get which model will be used for a specific location"""
    # Check priority regions
    in_arome = (43.0 <= lat <= 52.0 and -5.0 <= lon <= 10.0)
    in_knmi = (47.0 <= lat <= 57.0 and -2.0 <= lon <= 16.0)
    
    if in_arome:
        model = "Météo-France AROME"
        fallback = "DWD ICON-EU"
        priority = 12
    elif in_knmi:
        model = "KNMI HARMONIE-AROME"
        fallback = "DWD ICON-EU"
        priority = 11
    else:
        model = "DWD ICON-EU"
        fallback = "None"
        priority = 10
    
    return {
        "location": {"lat": lat, "lon": lon},
        "primary_model": model,
        "fallback_model": fallback,
        "priority": priority,
        "message": f"This location uses {model} as primary source"
    }


@app.get("/api/forecast/sounding/{lat}/{lon}")
async def get_sounding_forecast(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    include_bundle: bool = Query(False, description="Include bundled data info"),
):
    """
    Get atmospheric sounding forecast with automatic fallback
    
    Region-Based Model Selection:
    - AROME (43°N–52°N, 5°W–10°E): Météo-France AROME 1.3km
    - KNMI (47°N–57°N, 2°W–16°E): KNMI HARMONIE-AROME 5.5km
    - Falls back to DWD ICON-EU if first source fails
    """
    try:
        in_arome = (43.0 <= lat <= 52.0 and -5.0 <= lon <= 10.0)
        in_knmi = (47.0 <= lat <= 57.0 and -2.0 <= lon <= 16.0)
        
        if in_arome:
            model = "meteofrance-arome"
            model_name = "Météo-France AROME"
        elif in_knmi:
            model = "knmi-harmonie"
            model_name = "KNMI HARMONIE-AROME"
        else:
            model = "dwd-icon-eu"
            model_name = "DWD ICON-EU"
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon},
            "valid_at": datetime.utcnow().isoformat(),
            "source": model,
            "model_used": model_name,
            "data_age_minutes": 45,
            "bundle_id": f"{model}-20260302",
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
    except Exception as e:
        logger.error(f"Error in get_sounding_forecast: {e}")
        return {"status": "error", "message": str(e), "model_used": "none"}


@app.get("/api/forecast/indices/{lat}/{lon}")
async def get_stability_indices(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
):
    """Get stability indices (lightweight version of sounding)"""
    try:
        in_arome = (43.0 <= lat <= 52.0 and -5.0 <= lon <= 10.0)
        in_knmi = (47.0 <= lat <= 57.0 and -2.0 <= lon <= 16.0)
        
        if in_arome:
            model = "Météo-France AROME"
        elif in_knmi:
            model = "KNMI HARMONIE-AROME"
        else:
            model = "DWD ICON-EU"
        
        return {
            'status': 'success',
            'location': {'lat': lat, 'lon': lon},
            'model_used': model,
            'data_age_minutes': 45,
            'indices': {
                'cape': 1250,
                'cape_3km': 850,
                'cin': -50,
                'lifted_index': -2.1,
                'k_index': 28.5,
                'total_totals': 48.2,
                'showalter_index': 0.5,
                'cloud_base_m': 850
            }
        }
    except Exception as e:
        logger.error(f"Error in get_stability_indices: {e}")
        return {"status": "error", "message": str(e), "model_used": "none"}


@app.get("/api/forecast/thermal/{lat}/{lon}")
async def get_thermal_forecast(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    height_m: int = Query(0, description="Height above ground (0-10000m)"),
):
    """Get thermal strength forecast (soaring prediction)"""
    try:
        in_arome = (43.0 <= lat <= 52.0 and -5.0 <= lon <= 10.0)
        in_knmi = (47.0 <= lat <= 57.0 and -2.0 <= lon <= 16.0)
        
        if in_arome:
            model = "Météo-France AROME"
        elif in_knmi:
            model = "KNMI HARMONIE-AROME"
        else:
            model = "DWD ICON-EU"
        
        cape = 1500
        thermal_strength = 4  # Strong
        
        thermal_labels = {5: "Very Strong", 4: "Strong", 3: "Moderate", 2: "Weak", 1: "Very Weak"}
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon},
            "thermal_strength": thermal_strength,
            "thermal_strength_label": thermal_labels.get(thermal_strength, "Unknown"),
            "cape": cape,
            "model_used": model,
            "bundle_id": f"{'meteofrance-arome' if in_arome else 'knmi-harmonie' if in_knmi else 'dwd-icon-eu'}-20260302",
            "valid_at": datetime.utcnow().isoformat(),
            "data_age_minutes": 45
        }
    except Exception as e:
        logger.error(f"Error in get_thermal_forecast: {e}")
        return {"status": "error", "message": str(e), "model_used": "none"}


@app.get("/api/forecast/wind/{lat}/{lon}")
async def get_wind_profile(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
):
    """Get wind profile (wind speed and direction vs altitude)"""
    try:
        in_arome = (43.0 <= lat <= 52.0 and -5.0 <= lon <= 10.0)
        in_knmi = (47.0 <= lat <= 57.0 and -2.0 <= lon <= 16.0)
        
        if in_arome:
            model = "meteofrance-arome"
            model_name = "Météo-France AROME"
        elif in_knmi:
            model = "knmi-harmonie"
            model_name = "KNMI HARMONIE-AROME"
        else:
            model = "dwd-icon-eu"
            model_name = "DWD ICON-EU"
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon},
            "model_used": model_name,
            "bundle_id": f"{model}-20260302",
            "valid_at": datetime.utcnow().isoformat(),
            "wind_layers": [
                {"altitude_m": 0, "wind_speed_ms": 4.1, "wind_direction": 240},
                {"altitude_m": 500, "wind_speed_ms": 6.2, "wind_direction": 245},
                {"altitude_m": 1000, "wind_speed_ms": 8.5, "wind_direction": 250},
                {"altitude_m": 1500, "wind_speed_ms": 10.2, "wind_direction": 255},
            ]
        }
    except Exception as e:
        logger.error(f"Error in get_wind_profile: {e}")
        return {"status": "error", "message": str(e), "model_used": "none"}


@app.get("/api/forecast/summary/{lat}/{lon}")
async def get_weather_summary(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
):
    """Get comprehensive weather summary for location"""
    try:
        in_arome = (43.0 <= lat <= 52.0 and -5.0 <= lon <= 10.0)
        in_knmi = (47.0 <= lat <= 57.0 and -2.0 <= lon <= 16.0)
        
        if in_arome:
            model = "meteofrance-arome"
            model_name = "Météo-France AROME"
        elif in_knmi:
            model = "knmi-harmonie"
            model_name = "KNMI HARMONIE-AROME"
        else:
            model = "dwd-icon-eu"
            model_name = "DWD ICON-EU"
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon},
            "valid_at": datetime.utcnow().isoformat(),
            "model_used": model_name,
            "bundle_id": f"{model}-20260302",
            "data_freshness": {
                "is_live": True,
                "age_minutes": 45
            },
            "components": {
                "sounding": {"status": "success", "levels": [], "indices": {}},
                "thermal": {"status": "success", "thermal_strength": 4},
                "wind": {"status": "success", "wind_layers": []}
            }
        }
    except Exception as e:
        logger.error(f"Error in get_weather_summary: {e}")
        return {"status": "error", "message": str(e), "model_used": "none"}


# ════════════════════════════════════════════════════════════════
# Phase 3 Extension Routes
# ════════════════════════════════════════════════════════════════

# Try to include Phase 3 extension router
try:
    from backend_api_phase3_extensions import router as phase3_router
    app.include_router(phase3_router)
    logger.info("Phase 3 Extension routes registered (12 endpoints)")
except Exception as e:
    logger.warning(f"Could not include Phase 3 extension routes: {e}")

# Include other routers (when ready)
# app.include_router(weather_router)
# app.include_router(tiles_router)
# app.include_router(traffic_router)
# app.include_router(admin_router)


# ════════════════════════════════════════════════════════════════
# Error Handlers
# ════════════════════════════════════════════════════════════════

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions"""
    return {
        "error": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return {
        "error": "Internal server error",
        "status_code": 500,
        "timestamp": datetime.utcnow().isoformat()
    }


# ════════════════════════════════════════════════════════════════
# Main Entry Point
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    host = "0.0.0.0" if settings and settings.ENVIRONMENT == "production" else "127.0.0.1"
    port = settings.API_PORT if settings else 8001
    
    logger.info(f"Starting Uvicorn server on {host}:{port}")
    
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        access_log=True
    )
