"""
Celery Background Tasks for Soaring Module

Scheduled tasks:
- 02:00 UTC daily: Update OGN & sounding biases
- 03:00 UTC Sunday: Train ML models
- Every 15 min: Poll EUMETSAT satellite data
- 06:00 & 14:00 UTC: Update IGRA2 radiosonde data
- 1st Sunday monthly: Download ERA5 reanalysis
"""

import logging
from datetime import datetime, timedelta
from celery import Celery, Task
from celery.schedules import crontab

logger = logging.getLogger(__name__)

# Initialize Celery app
# Note: Assumes CELERY_BROKER_URL and CELERY_RESULT_BACKEND in config
celery = Celery('glidemate')

# Load config from file
celery.config_from_object('celeryconfig')


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 1: UPDATE OGN BIASES (Daily, 02:00 UTC)
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(name='tasks.update_ogn_biases')
def update_ogn_biases():
    """
    Recalculate OGN-based bias corrections daily.
    
    Analyses all observations from last 30 days, computes grid cell biases.
    Runs at 02:00 UTC daily after overnight OGN activity.
    """
    
    logger.info("Starting OGN bias update...")
    
    try:
        from ml.bias_correction import update_all_ogn_biases
        from database import get_db
        
        db = get_db()
        success = update_all_ogn_biases(db)
        
        if success:
            logger.info("✓ OGN biases updated successfully")
            return {"status": "success", "timestamp": datetime.utcnow().isoformat()}
        else:
            logger.warning("OGN bias update failed")
            return {"status": "failed", "timestamp": datetime.utcnow().isoformat()}
    
    except Exception as e:
        logger.exception(f"OGN bias task failed: {e}")
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 2: UPDATE SOUNDING BIASES (Daily, 02:00 UTC)
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(name='tasks.update_sounding_biases')
def update_sounding_biases():
    """
    Recalculate model vs IGRA2 sounding biases.
    
    Compares model profiles with radiosonde data per pressure level.
    Runs daily at 02:00 UTC after IGRA2 data arrival.
    """
    
    logger.info("Starting sounding bias update...")
    
    try:
        from ml.bias_correction import update_all_sounding_biases
        from database import get_db
        
        db = get_db()
        success = update_all_sounding_biases(db)
        
        if success:
            logger.info("✓ Sounding biases updated successfully")
            return {"status": "success", "timestamp": datetime.utcnow().isoformat()}
        else:
            logger.warning("Sounding bias update failed")
            return {"status": "failed"}
    
    except Exception as e:
        logger.exception(f"Sounding bias task failed: {e}")
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 3: TRAIN ML MODELS (Weekly Sunday 03:00 UTC)
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(name='tasks.train_thermal_models')
def train_thermal_models():
    """
    Train/retrain XGBoost thermal models.
    
    Only if >=10,000 observations since last training.
    Trains 3 models: climb_rate, thermal_top, thermal_radius.
    Runs every Sunday at 03:00 UTC.
    """
    
    logger.info("Starting ML model training...")
    
    try:
        from ml.thermal_model import train_thermal_models
        from database import get_db
        
        db = get_db()
        metrics = train_thermal_models(db)
        
        if metrics:
            msg = f"✓ Models trained: {len(metrics)} models"
            logger.info(msg)
            
            return {
                "status": "success",
                "models_trained": len(metrics),
                "metrics": {k: v.__dict__ for k, v in metrics.items()},
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            logger.info("Training skipped (insufficient observations or recently trained)")
            return {"status": "skipped", "reason": "insufficient_data"}
    
    except Exception as e:
        logger.exception(f"Model training task failed: {e}")
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 3.5: DETECT THERMAL CIRCLES (Every 2 minutes)
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(name='tasks.detect_thermal_circles')
def detect_thermal_circles():
    """
    Detect thermal circling from OGN APRS aircraft data.
    
    Analyzes aircraft tracks for circling patterns.
    Stores observations in thermal_observations table.
    Runs every 2 minutes during daylight hours.
    """
    
    logger.info("Starting OGN thermal circle detection...")
    
    try:
        from ogn.thermal_detector import CirclingDetector, ThermalObservation as ThermalObs
        from backend_database_models import ThermalObservation
        from database import get_db
        
        db = get_db()
        detector = CirclingDetector(window_size=30)
        
        # Fetch recent OGN traffic positions (last 30 seconds)
        # From traffic_cache or OGN stream
        traffic_data = db.execute("""
            SELECT callsign, lat, lon, altitude_m, vertical_speed_ms, 
                   track_degrees, last_update
            FROM traffic_cache
            WHERE last_update > NOW() - INTERVAL '30 seconds'
            ORDER BY callsign, last_update
        """).fetchall()
        
        if not traffic_data:
            logger.debug("No recent traffic data")
            return {"status": "no_data"}
        
        # Group positions by callsign and analyze for circling
        from collections import defaultdict
        tracks = defaultdict(list)
        
        for row in traffic_data:
            callsign, lat, lon, alt_m, vs, track, timestamp = row
            from ogn.thermal_detector import Position
            pos = Position(
                lat=lat, lon=lon, alt_m=alt_m,
                bearing_deg=track, time=timestamp, callsign=callsign
            )
            tracks[callsign].append(pos)
        
        # Detect thermals from each aircraft track
        observations = []
        for callsign, positions in tracks.items():
            if len(positions) >= 10:
                thermal_obs = detector.analyze_track(positions)
                if thermal_obs:
                    observations.append(thermal_obs)
        
        # Store in database
        count = 0
        for obs in observations:
            db_obs = ThermalObservation(
                lat=obs.lat,
                lon=obs.lon,
                altitude_m=obs.alt_base_m,
                alt_m=obs.alt_base_m,
                climb_rate_ms=obs.climb_rate_ms,
                alt_top_m=obs.alt_top_m,
                thermal_radius_m=obs.radius_m,
                duration_s=obs.duration_s,
                timestamp=obs.timestamp,
                callsign=obs.callsign,
                land_cover_class=obs.land_cover_class,
                elevation_m=obs.elevation_m,
                slope_deg=obs.slope_deg,
                aspect_deg=obs.aspect_deg,
            )
            db.add(db_obs)
            count += 1
        
        db.commit()
        
        logger.info(f"Detected and stored {count} thermals")
        return {
            "status": "success",
            "observations_stored": count,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        logger.exception(f"Thermal circle detection failed: {e}")
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 4: POLL EUMETSAT SATELLITE DATA (Every 15 minutes)
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(name='tasks.poll_eumetsat')
def poll_eumetsat():
    """
    Check for latest EUMETSAT satellite products.
    
    Downloads SEVIRI, cloud mask, rainfall data every 15 minutes.
    Stores in /data/satellite/
    """
    
    try:
        from atmosphere.data_sources import EumsatDataSource
        
        eumetsat = EumsatDataSource()
        result = eumetsat.download_latest_product()
        
        if result:
            logger.debug(f"EUMETSAT satellite data downloaded: {result}")
            return {"status": "success", "product": result}
        else:
            logger.debug("No new EUMETSAT product available")
            return {"status": "no_update"}
    
    except Exception as e:
        logger.warning(f"EUMETSAT poll failed: {e}")
        return {"status": "error", "error": str(e)[:100]}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 5: UPDATE IGRA2 RADIOSONDES (Twice daily: 06:00 & 14:00 UTC)
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(name='tasks.update_igra2_stations')
def update_igra2_stations():
    """
    Download latest IGRA2 radiosonde data.
    
    Updates 20 key European stations.
    Runs at 06:00 & 14:00 UTC (after 00:00 & 12:00 UTC ascents).
    """
    
    logger.info("Updating IGRA2 radiosonde data...")
    
    try:
        from atmosphere.data_sources import IGRA2DataSource
        from database import get_db
        
        igra2 = IGRA2DataSource()
        db = get_db()
        
        # Download all European stations
        stations_updated = igra2.download_all_european_stations()
        
        logger.info(f"✓ IGRA2 updated: {stations_updated} stations")
        
        return {
            "status": "success",
            "stations_updated": stations_updated,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        logger.warning(f"IGRA2 update failed: {e}")
        return {"status": "error", "error": str(e)[:100]}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 6: DOWNLOAD ERA5 REANALYSIS (First Sunday monthly, 04:00 UTC)
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(name='tasks.monthly_era5_download')
def monthly_era5_download():
    """
    Download monthly ERA5 reanalysis data.
    
    Downloads pressure levels + single levels for entire month.
    Runs on 1st Sunday of month at 04:00 UTC.
    """
    
    logger.info("Starting monthly ERA5 download...")
    
    try:
        from atmosphere.data_sources import ERA5DataSource
        
        era5 = ERA5DataSource()
        
        # Get current month
        now = datetime.utcnow()
        previous_month = now.replace(day=1) - timedelta(days=1)
        
        # Download previous month's data
        success = era5.initialize_download(
            year=previous_month.year,
            months=[previous_month.month]
        )
        
        if success:
            logger.info(f"✓ ERA5 {previous_month.strftime('%B %Y')} downloaded")
            return {
                "status": "success",
                "year": previous_month.year,
                "month": previous_month.month
            }
        else:
            logger.warning("ERA5 download failed")
            return {"status": "failed"}
    
    except Exception as e:
        logger.exception(f"ERA5 download task failed: {e}")
        return {"status": "error", "error": str(e)[:100]}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 7: CLEANUP OLD OBSERVATIONS (Daily 23:00 UTC)
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(name='tasks.cleanup_old_observations')
def cleanup_old_observations(days_to_keep: int = 90):
    """
    Delete old thermal observations to manage database size.
    
    Keeps last N days, deletes beyond that.
    Runs daily at 23:00 UTC.
    """
    
    logger.info(f"Cleaning observations older than {days_to_keep} days...")
    
    try:
        from database import get_db
        
        db = get_db()
        
        cutoff = datetime.utcnow() - timedelta(days=days_to_keep)
        
        result = db.execute("""
            DELETE FROM thermal_observations
            WHERE timestamp < %s
        """, (cutoff,))
        
        deleted_count = result.rowcount
        
        logger.info(f"✓ Deleted {deleted_count} old observations")
        
        return {
            "status": "success",
            "deleted_count": deleted_count,
            "kept_days": days_to_keep
        }
    
    except Exception as e:
        logger.exception(f"Cleanup task failed: {e}")
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# CELERY BEAT SCHEDULE
# ═══════════════════════════════════════════════════════════════════════════════

def get_celery_schedule():
    """
    Returns the Celery Beat schedule configuration.
    
    Place this in celeryconfig.py:
    
    beat_schedule = get_celery_schedule()
    """
    
    from celery.schedules import crontab
    
    return {
        # OGN Bias Updates (Daily)
        'update-ogn-biases': {
            'task': 'tasks.update_ogn_biases',
            'schedule': crontab(hour=2, minute=0),
        },
        'update-sounding-biases': {
            'task': 'tasks.update_sounding_biases',
            'schedule': crontab(hour=2, minute=10),  # 10 min after OGN
        },
        
        # ML Training (Weekly)
        'train-thermal-models': {
            'task': 'tasks.train_thermal_models',
            'schedule': crontab(day_of_week=6, hour=3, minute=0),  # Sunday 03:00
        },
        
        # Data Source Updates
        'poll-eumetsat': {
            'task': 'tasks.poll_eumetsat',
            'schedule': 15 * 60,  # Every 15 minutes
        },
        'update-igra2': {
            'task': 'tasks.update_igra2_stations',
            'schedule': crontab(hour=6, minute=0) | crontab(hour=14, minute=0),
        },
        'monthly-era5-download': {
            'task': 'tasks.monthly_era5_download',
            'schedule': crontab(day_of_week=6, hour=4, minute=0, day_of_month='1-7'),
        },
        
        # Maintenance
        'cleanup-old-observations': {
            'task': 'tasks.cleanup_old_observations',
            'schedule': crontab(hour=23, minute=0),
        }
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLING & MONITORING
# ═══════════════════════════════════════════════════════════════════════════════

class TaskBase(Task):
    """Base task with error handling"""
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Called when task fails"""
        logger.error(f"Task {self.name} failed: {exc}")
        # Could send alert here
    
    def on_success(self, result, task_id, args, kwargs):
        """Called when task succeeds"""
        logger.debug(f"Task {self.name} succeeded")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test schedule generation
    schedule = get_celery_schedule()
    
    print("Celery Beat Schedule:")
    print("=" * 60)
    for task_name, task_config in schedule.items():
        print(f"  {task_name:30} → {task_config['task']}")
        print(f"    {'':30}   schedule: {task_config['schedule']}")
    
    print(f"\nTotal tasks: {len(schedule)}")
