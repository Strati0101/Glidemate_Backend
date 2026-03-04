"""
celery_app.py
Central Celery application configuration and task routing
"""

import os
import logging
from celery import Celery

logger = logging.getLogger(__name__)

# Get config from environment
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/1')

# Create Celery app
app = Celery(
    'glidemate-backend',
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

# Load configuration
celery_config = {
    'broker_url': CELERY_BROKER_URL,
    'result_backend': CELERY_RESULT_BACKEND,
    'task_serializer': 'json',
    'accept_content': ['json'],
    'result_serializer': 'json',
    'timezone': 'UTC',
    'enable_utc': True,
    'task_track_started': True,
    'task_time_limit': 30 * 60,  # 30 minutes hard limit
    'task_soft_time_limit': 25 * 60,  # 25 minutes soft limit
    'broker_connection_retry_on_startup': True,
    'task_routes': {
        'celery_tasks.data_ingestion.*': {'queue': 'data_ingestion'},
        'celery_tasks.tile_generation.*': {'queue': 'tile_generation'},
        'ingest_meteofrance_arome': {'queue': 'weather_high_priority', 'priority': 12},
        'renew_meteofrance_token': {'queue': 'weather_high_priority', 'priority': 15},
        'ingest_geosphere_austria_inca': {'queue': 'weather_high_priority', 'priority': 11},
        'ingest_geosphere_austria_stations': {'queue': 'weather_high_priority', 'priority': 9},
    },
    'beat_schedule': {
        # CRITICAL: Token renewal every 55 minutes (highest priority)
        'meteofrance-token-renewal': {
            'task': 'renew_meteofrance_token',
            'schedule': 3300,  # 55 minutes (before 60 min token expiry)
            'options': {'priority': 15}
        },
        
        # Phase 3 data sources
        'meteofrance-arome-ingestion': {
            'task': 'ingest_meteofrance_arome',
            'schedule': 3600,  # Every hour
            'options': {'priority': 12}
        },
        
        'geosphere-austria-inca': {
            'task': 'ingest_geosphere_austria_inca',
            'schedule': 1800,  # Every 30 minutes
            'options': {'priority': 11}
        },
        
        'geosphere-austria-stations': {
            'task': 'ingest_geosphere_austria_stations',
            'schedule': 3600,  # Every hour
            'options': {'priority': 9}
        },
        
        # ICON-D2 (German model EU)
        'icon-d2-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_icon_d2',
            'schedule': 6 * 3600,  # Every 6 hours
            'options': {'priority': 10}
        },
        
        # GFS (Global Forecast System)
        'gfs-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_gfs',
            'schedule': 6 * 3600,  # Every 6 hours
            'options': {'priority': 10}
        },
        
        # KNMI (Royal Netherlands)
        'knmi-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_knmi',
            'schedule': 3 * 3600,  # Every 3 hours
            'options': {'priority': 10}
        },
        
        # EUMETSAT (Satellites)
        'eumetsat-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_eumetsat',
            'schedule': 30 * 60,  # Every 30 minutes
            'options': {'priority': 10}
        },
        
        # CDS (Climate Data Store)
        'cds-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_cds',
            'schedule': 24 * 3600,  # Daily
            'options': {'priority': 5}
        },
        
        # Existing DWD sources
        'dwd-icon-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_dwd_icon',
            'schedule': 6 * 3600,  # Every 6 hours
            'options': {'priority': 10}
        },
        'dwd-radolan-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_dwd_radolan',
            'schedule': 5 * 60,  # Every 5 minutes
            'options': {'priority': 9}
        },
        'dwd-metar-taf-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_dwd_metar_taf',
            'schedule': 30 * 60,  # Every 30 minutes
            'options': {'priority': 8}
        },
        'dwd-cdc-stations': {
            'task': 'celery_tasks.data_ingestion.ingest_dwd_cdc_stations',
            'schedule': 60 * 60,  # Every hour
            'options': {'priority': 8}
        },
        'dwd-radiosonde': {
            'task': 'celery_tasks.data_ingestion.ingest_dwd_radiosonde',
            'schedule': 12 * 3600,  # Every 12 hours
            'options': {'priority': 7}
        },
        'dwd-radar-composite': {
            'task': 'celery_tasks.data_ingestion.ingest_dwd_radar_composite',
            'schedule': 5 * 60,  # Every 5 minutes
            'options': {'priority': 9}
        },
        'noaa-soundings-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_noaa_soundings',
            'schedule': 12 * 3600,  # Every 12 hours
            'options': {'priority': 7}
        },
        'noaa-gfs-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_noaa_gfs',
            'schedule': 6 * 3600,  # Every 6 hours
            'options': {'priority': 6}
        },
        'openaip-ingest': {
            'task': 'celery_tasks.data_ingestion.ingest_openaip',
            'schedule': 24 * 3600,  # Daily
            'options': {'priority': 5}
        },
        
        # ML Pipeline tasks
        'detect-thermal-circles': {
            'task': 'tasks.detect_thermal_circles',
            'schedule': 2 * 60,  # Every 2 minutes
            'options': {'priority': 10}
        },
        
        'update-ogn-biases': {
            'task': 'tasks.update_ogn_biases',
            'schedule': 86400,  # Daily at 02:00 UTC (crontab)
            'options': {'priority': 7}
        },
        
        'update-sounding-biases': {
            'task': 'tasks.update_sounding_biases',
            'schedule': 86400,  # Daily at 02:00 UTC (crontab)
            'options': {'priority': 7}
        },
        
        'train-thermal-models': {
            'task': 'tasks.train_thermal_models',
            'schedule': 604800,  # Weekly on Sunday at 03:00 UTC (crontab)
            'options': {'priority': 6}
        },
    }
}

app.config_from_object(celery_config)

# Auto-discover tasks from celery_tasks modules and Phase 3 extensions
app.autodiscover_tasks(['celery_tasks', 'backend_celery_tasks_phase3_extensions'])

logger.info("Celery app initialized with broker: %s", CELERY_BROKER_URL)
logger.info("Task routes configured for Phase 3 extensions:")
logger.info("  - renew_meteofrance_token: Priority 15 (HIGHEST, every 55 minutes)")
logger.info("  - ingest_meteofrance_arome: Priority 12 (every 60 minutes)")
logger.info("  - ingest_geosphere_austria_inca: Priority 11 (every 30 minutes)")


@app.task(bind=True)
def debug_task(self):
    """Test task to verify Celery is working"""
    logger.info('Celery task request: %s', self.request)
    return 'Celery is working!'
