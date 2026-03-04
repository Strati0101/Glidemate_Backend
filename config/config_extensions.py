"""
Phase 3 Extensions - Configuration Updates

Adds to backend_config.py:
- Celery beat schedule for new data sources
- Updated model priority rules by region
- DEM configuration
- API key configuration

This file contains additions to be merged into backend_config.py
"""

# =============================================================================
# CELERY BEAT SCHEDULE ADDITIONS
# =============================================================================

# Add these to CELERY_BEAT_SCHEDULE dictionary in backend_config.py

CELERY_BEAT_SCHEDULE_EXTENSIONS = {
    # CRITICAL: Météo-France Token Renewal - Every 55 minutes
    # This keeps credentials fresh and prevents "awaiting credentials" errors
    "meteofrance-token-renewal": {
        "task": "renew_meteofrance_token",
        "schedule": {
            "__type__": "interval",
            "seconds": 3300  # Every 55 minutes (before 60 min token expiry)
        },
        "options": {"queue": "weather_high_priority", "priority": 15},  # Highest priority
        "kwargs": {}
    },
    
    # Météo-France AROME - Priority 12 (highest resolution)
    "meteofrance-arome-ingestion": {
        "task": "ingest_meteofrance_arome",
        "schedule": {
            "__type__": "crontab",
            "minute": 0,  # Every hour on the hour
            "hour": "*"
        },
        "options": {"queue": "weather_high_priority", "priority": 12},
        "kwargs": {}
    },
    
    # GeoSphere Austria Stations - Priority 9 (frequent updates)
    "geosphere-austria-stations": {
        "task": "ingest_geosphere_austria_stations",
        "schedule": {
            "__type__": "interval",
            "seconds": 3600  # Every 1 hour (Celery beat, but source updates every 10 min)
        },
        "options": {"queue": "weather_high_priority", "priority": 9},
        "kwargs": {}
    },
    
    # GeoSphere Austria INCA - Priority 11
    "geosphere-austria-inca": {
        "task": "ingest_geosphere_austria_inca",
        "schedule": {
            "__type__": "crontab",
            "minute": 30,  # Every hour at :30
            "hour": "*"
        },
        "options": {"queue": "weather_high_priority", "priority": 11},
        "kwargs": {}
    },
    
    # Copernicus DEM Download - One-time, low priority
    # Run once daily at 3 AM UTC (low traffic time)
    "copernicus-dem-download": {
        "task": "download_copernicus_dem",
        "schedule": {
            "__type__": "crontab",
            "minute": 0,
            "hour": 3,
            "day_of_week": "0"  # Sunday only
        },
        "options": {"queue": "background_tasks", "priority": 1},
        "kwargs": {
            "region": "france",  # Download France region (80 tiles)
            "lat_min": 43,
            "lat_max": 52,
            "lon_min": -5,
            "lon_max": 10
        }
    },
    
    # Terrain analysis caching - Low priority, run once daily
    "cache-terrain-regions": {
        "task": "cache_terrain_regions",
        "schedule": {
            "__type__": "crontab",
            "minute": 0,
            "hour": 2,
            "day_of_week": "0"  # Sunday at 2 AM UTC
        },
        "options": {"queue": "background_tasks", "priority": 2},
        "kwargs": {}
    }
}


# =============================================================================
# MODEL PRIORITY RULES - UPDATED
# =============================================================================

# Add these regional priority rules to replace old model priority table

MODEL_PRIORITY_RULES = {
    # France region (43°N–52°N, 5°W–10°E) - Highest resolution available
    "france": {
        "name": "France & Alsace-Lorraine",
        "bounds": {
            "lat_min": 43.0,
            "lat_max": 52.0,
            "lon_min": -5.0,
            "lon_max": 10.0
        },
        "priority_chain": [
            {
                "model": "meteofrance-arome",
                "resolution_km": 1.3,
                "update_frequency_h": 1,
                "priority": 12,
                "coverage": "France + border regions",
                "max_forecast_h": 48
            },
            {
                "model": "knmi-harmonie-arome",
                "resolution_km": 5.5,
                "update_frequency_h": 1,
                "priority": 11,
                "coverage": "NL/BE/NRW",
                "max_forecast_h": 48
            },
            {
                "model": "dwd-icon-eu",
                "resolution_km": 7.0,
                "update_frequency_h": 6,
                "priority": 10,
                "coverage": "Europe-wide",
                "max_forecast_h": 180
            }
        ]
    },
    
    # Austria region (46°N–49.5°N, 9°E–18°E) - 1km Alpine model
    "austria": {
        "name": "Austria & South Bavaria",
        "bounds": {
            "lat_min": 46.0,
            "lat_max": 49.5,
            "lon_min": 9.0,
            "lon_max": 18.0
        },
        "priority_chain": [
            {
                "model": "geosphere-austria-inca",
                "resolution_km": 1.0,
                "update_frequency_h": 1,
                "priority": 11,
                "coverage": "Austria + Alpine regions",
                "max_forecast_h": 48
            },
            {
                "model": "dwd-icon-eu",
                "resolution_km": 7.0,
                "update_frequency_h": 6,
                "priority": 10,
                "coverage": "Europe-wide",
                "max_forecast_h": 180
            }
        ]
    },
    
    # Default/global - Use standard cascade
    "global": {
        "name": "Global (default)",
        "priority_chain": [
            {
                "model": "dwd-icon-eu",
                "resolution_km": 7.0,
                "update_frequency_h": 6,
                "priority": 10,
                "coverage": "Europe",
                "max_forecast_h": 180
            },
            {
                "model": "knmi-harmonie-arome",
                "resolution_km": 5.5,
                "update_frequency_h": 1,
                "priority": 9,
                "coverage": "NL/BE/NRW",
                "max_forecast_h": 48
            },
            {
                "model": "ecmwf-ifs",
                "resolution_km": 25.0,
                "update_frequency_h": 12,
                "priority": 5,
                "coverage": "Global",
                "max_forecast_h": 240
            }
        ]
    }
}


def get_model_priority_for_location(lat: float, lon: float) -> dict:
    """
    Determine which model to use for a location based on priority rules.
    
    Args:
        lat: Latitude
        lon: Longitude
    
    Returns:
        Dictionary with priority_chain, best_model, and region info
    """
    # Check France region first
    france = MODEL_PRIORITY_RULES["france"]
    if (france["bounds"]["lat_min"] <= lat <= france["bounds"]["lat_max"] and
        france["bounds"]["lon_min"] <= lon <= france["bounds"]["lon_max"]):
        return {
            "region": "france",
            "priority_chain": france["priority_chain"],
            "primary_model": france["priority_chain"][0]["model"],
            "note": "Météo-France AROME high-resolution model"
        }
    
    # Check Austria region
    austria = MODEL_PRIORITY_RULES["austria"]
    if (austria["bounds"]["lat_min"] <= lat <= austria["bounds"]["lat_max"] and
        austria["bounds"]["lon_min"] <= lon <= austria["bounds"]["lon_max"]):
        return {
            "region": "austria",
            "priority_chain": austria["priority_chain"],
            "primary_model": austria["priority_chain"][0]["model"],
            "note": "GeoSphere Austria INCA 1km model"
        }
    
    # Default global
    global_rules = MODEL_PRIORITY_RULES["global"]
    return {
        "region": "global",
        "priority_chain": global_rules["priority_chain"],
        "primary_model": global_rules["priority_chain"][0]["model"],
        "note": "Using standard cascade"
    }


# =============================================================================
# DEM CONFIGURATION
# =============================================================================

DEM_CONFIG = {
    "enabled": True,
    "source": {
        "name": "Copernicus DEM (GLO-30)",
        "resolution_m": 30,
        "coverage": {
            "lat_min": 35,
            "lat_max": 72,
            "lon_min": -25,
            "lon_max": 45,
            "name": "Europe + North Africa"
        },
        "license": "CC BY 4.0",
        "s3_bucket": "copernicus-dem-30m",
        "s3_region": "us-west-2"
    },
    "storage": {
        "tiles_directory": "/opt/glidemate-backend/data/dem/tiles",
        "vrt_file": "/opt/glidemate-backend/data/dem/europe_30m.vrt",
        "max_tile_size_mb": 100,
        "cache_in_ram": False  # Only load when needed (saves memory)
    },
    "features": {
        "ridge_detection": True,          # For wave flying
        "slope_aspect": True,              # For thermal strength
        "valley_detection": True,          # For convergence zones
        "shadow_zones": True,              # For time-of-day corrections
        "froude_enhancement": True         # For Froude number calculations
    }
}


# =============================================================================
# API KEY CONFIGURATION
# =============================================================================

# Add to .env file:
#
# METEOFRANCE_CLIENT_CREDENTIALS=<base64-encoded-credentials>
#   Value: enhrazBKWGM2Zlc3VVNSZ1paVlZYVVNwcmg4YTo2MjRfTVh5dFVobTVoWFFCV3J3Y0FoZmRESHNh
#   (Base64 encoded from: {client_id}:{client_secret})
#
# GEOSPHERE_AUSTRIA_ENABLED=true
#   (No API key needed - CC BY 4.0 open license)
#
# COPERNICUS_DEM_ENABLED=true
#   (No API key needed - AWS S3 anonymous access)

API_KEYS_PHASE3 = {
    "meteofrance": {
        "env_var": "METEOFRANCE_CLIENT_CREDENTIALS",
        "required": True,
        "description": "Base64-encoded Météo-France API credentials",
        "endpoint": "https://portail-api.meteofrance.fr/token",
        "token_lifetime_sec": 3600,
        "renewal_buffer_sec": 300
    },
    "geosphere_austria": {
        "required": False,
        "description": "No API key needed (CC BY 4.0 open license)",
        "endpoint": "https://dataset.api.hub.geosphere.at/v1"
    },
    "copernicus_dem": {
        "required": False,
        "description": "No API key needed (AWS S3 anonymous access)",
        "s3_bucket": "copernicus-dem-30m",
        "s3_region": "us-west-2"
    }
}


# =============================================================================
# INTEGRATION WITH EXISTING PARCEL.PY
# =============================================================================

# In parcel.py, these factors should be applied:
#
# 1. RIDGE HEIGHT FOR FROUDE NUMBER:
#    Fr = U_perp / (N * H_ridge)
#    where H_ridge is from TerrainContext.ridge_height_m
#
# 2. THERMAL STRENGTH MODIFICATION:
#    thermal_strength *= TerrainContext.thermal_factor
#    (Accounts for slope, aspect, sunshine)
#
# 3. VALLEY CONVERGENCE BOOST:
#    If TerrainContext.is_valley:
#        thermal_strength *= 1.15
#
# 4. SHADOW ZONE REDUCTION:
#    If in shadow (north-facing, time of day):
#        thermal_strength *= 0.7
#    If in sun (south-facing):
#        thermal_strength *= 1.2

TERRAIN_CORRECTION_FACTORS = {
    "ridge_height_usage": {
        "description": "Ridge height in Froude number calculation",
        "formula": "Fr = U_perp / (N * H_ridge)",
        "data_source": "TerrainContext.ridge_height_m"
    },
    "thermal_slope_aspect": {
        "description": "Thermal factor from slope and aspect",
        "south_facing": 1.1,      # +10%
        "north_facing": 0.7,      # -30%
        "flat_terrain": 1.0,
        "formula": "thermal_strength *= slope_aspect_factor"
    },
    "valley_convergence": {
        "description": "Wind convergence in valleys",
        "boost_factor": 1.15,     # +15%
        "formula": "if is_valley: thermal_strength *= 1.15"
    },
    "shadow_zones": {
        "description": "Sun elevation effect",
        "well_lit": 1.2,          # +20%
        "shaded": 0.7,            # -30%
        "formula": "thermal_strength *= shadow_factor"
    }
}


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

LOGGING_EXTENSIONS = {
    "meteofrance_arome": {
        "level": "INFO",
        "handlers": ["console", "file"],
        "propagate": False
    },
    "geosphere_austria": {
        "level": "INFO",
        "handlers": ["console", "file"],
        "propagate": False
    },
    "copernicus_dem": {
        "level": "DEBUG",
        "handlers": ["console", "file"],
        "propagate": False
    },
    "terrain_analysis": {
        "level": "DEBUG",
        "handlers": ["console", "file"],
        "propagate": False
    }
}
