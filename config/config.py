"""
Configuration module for GlideMate Backend
Environment variables, database, cache, and Celery settings
"""

import os
from pydantic_settings import BaseSettings
from functools import lru_cache
from celery.schedules import crontab


class Settings(BaseSettings):
    """Application settings from environment variables"""
    
    # App
    app_name: str = "GlideMate Professional Weather API"
    app_version: str = "2.0.0"
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    # Database
    database_url: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/glidemate")
    
    # Redis / Cache
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    cache_ttl_metar: int = 30 * 60  # 30 minutes
    cache_ttl_taf: int = 60 * 60  # 1 hour
    cache_ttl_radar: int = 5 * 60  # 5 minutes
    cache_ttl_model_tiles: int = 60 * 60  # 1 hour
    cache_ttl_sounding: int = 6 * 60 * 60  # 6 hours
    cache_ttl_indices: int = 60 * 60  # 1 hour
    cache_ttl_airspace: int = 24 * 60 * 60  # 24 hours
    cache_ttl_traffic: int = 30  # 30 seconds
    cache_ttl_satellite: int = 15 * 60  # 15 minutes
    
    # Celery
    celery_broker_url: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
    celery_result_backend: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/2")
    
    # Data Sources
    dwd_icon_url: str = "https://opendata.dwd.de/weather/nwp/icon-eu/grib/"
    dwd_radolan_url: str = "https://opendata.dwd.de/weather/radar/radolan/rw/"
    dwd_aviation_url: str = "https://opendata.dwd.de/weather/weather_reports/aviation/"
    noaa_soundings_url: str = "https://rucsoundings.noaa.gov/get_soundings.cgi"
    noaa_gfs_url: str = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    openaip_url: str = "https://api.core.openaip.net/"
    ogn_stream_host: str = os.getenv("OGN_STREAM_HOST", "aprs.glidernet.org")
    ogn_stream_port: int = int(os.getenv("OGN_STREAM_PORT", "10152"))
    
    # KNMI (Royal Netherlands Meteorological Institute)
    knmi_api_key: str = os.getenv("KNMI_API_KEY", "")
    knmi_api_url: str = "https://api.dataplatform.knmi.nl/open-data/v1"
    knmi_harmonie_dataset: str = "harmonie-arome-cy43-p3-1-0"
    knmi_insitu_dataset: str = "10-minute-in-situ-meteorological-observations-1-0"
    
    # Priority region for KNMI (NL, BE, NRW): 47°N–57°N, 2°W–16°E
    knmi_priority_north: float = 57.0
    knmi_priority_south: float = 47.0
    knmi_priority_east: float = 16.0
    knmi_priority_west: float = -2.0
    
    # File Storage
    data_dir: str = os.getenv("DATA_DIR", "/opt/glidemate-data")
    grib2_dir: str = os.path.join(data_dir, "grib2")
    tiles_dir: str = os.path.join(data_dir, "tiles")
    
    # Geographic Bounds (Europe)
    min_lat: float = 25.0
    max_lat: float = 72.0
    min_lon: float = -25.0
    max_lon: float = 45.0
    
    # Forecast Horizon
    forecast_hours: int = 120
    model_run_interval: int = 6  # hours
    
    # CORS
    cors_origins: list = [
        "http://localhost:8080",
        "http://localhost:8081",
        "http://localhost:8082",
        "http://localhost:8083",
        "http://localhost:3000",
        "http://192.168.1.155:8080",
        "http://192.168.1.155:8081",
        "exp://localhost",
    ]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get singleton settings instance"""
    return Settings()


# Celery Configuration
def get_celery_config(settings: Settings = None) -> dict:
    """Generate Celery configuration"""
    if settings is None:
        settings = get_settings()
    
    return {
        'broker_url': settings.celery_broker_url,
        'result_backend': settings.celery_result_backend,
        'task_serializer': 'json',
        'accept_content': ['json'],
        'result_serializer': 'json',
        'timezone': 'UTC',
        'enable_utc': True,
        'task_track_started': True,
        'task_time_limit': 30 * 60,  # 30 minutes hard limit
        'task_soft_time_limit': 25 * 60,  # 25 minutes soft limit
        'broker_connection_retry_on_startup': True,
        'beat_schedule': {
            'dwd-icon-ingestion': {
                'task': 'celery_tasks.data_ingestion.ingest_dwd_icon',
                'schedule': 6 * 3600,  # Every 6 hours
                'options': {'queue': 'data_ingestion', 'priority': 10}
            },
            'dwd-radolan-ingestion': {
                'task': 'celery_tasks.data_ingestion.ingest_dwd_radolan',
                'schedule': 5 * 60,  # Every 5 minutes
                'options': {'queue': 'data_ingestion', 'priority': 9}
            },
            'knmi-harmonie-ingestion': {
                'task': 'celery_tasks.data_ingestion.ingest_knmi_harmonie',
                'schedule': 3 * 3600,  # Every 3 hours (HARMONIE updates more frequently)
                'options': {'queue': 'data_ingestion', 'priority': 11}  # Higher than DWD for NL region
            },
            'knmi-insitu-ingestion': {
                'task': 'celery_tasks.data_ingestion.ingest_knmi_insitu',
                'schedule': 10 * 60,  # Every 10 minutes (in-situ high frequency)
                'options': {'queue': 'data_ingestion', 'priority': 8}
            },
            'dwd-metar-taf-ingestion': {
                'task': 'celery_tasks.data_ingestion.ingest_dwd_metar_taf',
                'schedule': 30 * 60,  # Every 30 minutes
                'options': {'queue': 'data_ingestion', 'priority': 8}
            },
            'noaa-soundings-ingestion': {
                'task': 'celery_tasks.data_ingestion.ingest_noaa_soundings',
                'schedule': 12 * 3600,  # Every 12 hours
                'options': {'queue': 'data_ingestion', 'priority': 7}
            },
            'noaa-gfs-ingestion': {
                'task': 'celery_tasks.data_ingestion.ingest_noaa_gfs',
                'schedule': 6 * 3600,  # Every 6 hours
                'options': {'queue': 'data_ingestion', 'priority': 6}
            },
            'openaip-ingestion': {
                'task': 'celery_tasks.data_ingestion.ingest_openaip',
                'schedule': 24 * 3600,  # Daily
                'options': {'queue': 'data_ingestion', 'priority': 5}
            },
            'eumetsat-cloud-ingest': {
                'task': 'celery_tasks.data_ingestion.ingest_eumetsat_cloud',
                'schedule': 15 * 60,  # Every 15 minutes
                'options': {'queue': 'data_ingestion', 'priority': 9}
            },
            'eumetsat-imagery-ingest': {
                'task': 'celery_tasks.data_ingestion.ingest_eumetsat_imagery',
                'schedule': 30 * 60,  # Every 30 minutes
                'options': {'queue': 'data_ingestion', 'priority': 8}
            },
            'blitzortung-listener': {
                'task': 'celery_tasks.safety.listen_blitzortung_task',
                'schedule': 60,  # Continuous polling every minute
                'options': {'queue': 'safety', 'priority': 10}
            },
            'ogn-traffic-stream': {
                'task': 'celery_tasks.live_data.ingest_ogn_aprs',
                'schedule': 60,  # Continuous polling every minute
                'options': {'queue': 'live_data', 'priority': 10}
            },
            'ogn-stats-weekly': {
                'task': 'celery_tasks.analytics.build_ogn_statistics',
                'schedule': crontab(day_of_week=6, hour=4, minute=0),  # Sundays 4:00 UTC
                'options': {'queue': 'analytics', 'priority': 8}
            },
        }
    }
