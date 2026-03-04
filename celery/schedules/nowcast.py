#!/usr/bin/env python3
"""
Celery Beat Schedule for Soaring Forecast Package 3
Defines scheduled tasks for solar nowcasting, climatology updates, and maintenance
"""
from celery.schedules import crontab

# Beat schedule configuration for Soaring Forecast Package 3
NOWCAST_BEAT_SCHEDULE = {
    'solar-nowcast-15min': {
        'task': 'backend_celery_nowcast_tasks.compute_solar_nowcast',
        'schedule': crontab(minute='*/15'),  # Every 15 minutes
        'options': {'queue': 'default'},
    },
    'ogn-stats-weekly': {
        'task': 'backend_celery_nowcast_tasks.build_ogn_statistics',
        'schedule': crontab(hour=4, minute=0, day_of_week=6),  # Sunday 04:00 UTC
        'options': {'queue': 'default'},
    },
    'era5-update-monthly': {
        'task': 'backend_celery_nowcast_tasks.update_era5_climatology',
        'schedule': crontab(hour=5, minute=0, day_of_month=1),  # 1st of month 05:00 UTC
        'options': {'queue': 'default'},
    },
    'tiles-regen-monthly': {
        'task': 'backend_celery_nowcast_tasks.regenerate_history_tiles',
        'schedule': crontab(hour=6, minute=0, day_of_month=1),  # 1st of month 06:00 UTC
        'options': {'queue': 'default'},
    },
    'cleanup-hourly': {
        'task': 'backend_celery_nowcast_tasks.cleanup_old_nowcasts',
        'schedule': crontab(minute=0),  # Every hour
        'options': {'queue': 'default'},
    },
}
