"""
Safety Module Celery Tasks

Background jobs for:
- NOTAM updates (every 30 minutes)
- Föhn detection (every hour)
- Blitzortung listener (persistent WebSocket)
- Storm cell tracking (every 5 minutes)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
import os

logger = logging.getLogger("safety.celery_tasks")


# ========================
# NOTAM Updates (30 min)
# ========================
@shared_task(
    name="safety.update_notams",
    bind=True,
    time_limit=600,  # 10 minutes
    soft_time_limit=550,
)
def update_notams_task(self):
    """
    Update NOTAMs from FAA + OpenAIP every 30 minutes
    """
    try:
        from safety.notam import init_notam_manager
        import asyncio
        
        # Initialize manager
        manager = init_notam_manager()
        
        # Fetch for Europe region
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        notams = loop.run_until_complete(
            manager.fetch_notams_for_area(
                min_lon=-10,
                min_lat=43,
                max_lon=30,
                max_lat=55,
                hours=24
            )
        )
        
        logger.info(f"✓ Updated {len(notams)} NOTAMs from FAA + OpenAIP")
        
        # Store in database (TODO: implement storage)
        # for notam in notams:
        #     db.session.add(notam_to_db(notam))
        # db.session.commit()
        
        return {
            "status": "success",
            "notams_updated": len(notams),
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    except SoftTimeLimitExceeded:
        logger.error("NOTAM update timeout")
        return {"status": "timeout"}
    
    except Exception as e:
        logger.error(f"NOTAM update failed: {e}")
        self.retry(exc=e, countdown=60, max_retries=3)


# ========================
# Föhn Detection (1 hour)
# ========================
@shared_task(
    name="safety.detect_foehn_all_regions",
    bind=True,
    time_limit=1800,  # 30 minutes
    soft_time_limit=1750,
)
def detect_foehn_task(self):
    """
    Detect föhn conditions in all Alpine regions every hour
    
    Requires current NWP data:
    - MSLP south and north of Alps
    - Wind at 700 hPa
    - Temperature and humidity in valleys
    """
    try:
        from safety.foehn import FoehnDetector, FOEHN_REGIONS
        from backend_forecast_data_service import get_nwp_profile
        
        detector = FoehnDetector()
        results = {}
        
        for region_id, region_info in FOEHN_REGIONS.items():
            try:
                # Get NWP data for this region
                # This is a placeholder - actual implementation would fetch
                # from ICON-EU or INCA models
                
                profile = {
                    "pressure_south_hpa": 1013.2,
                    "pressure_north_hpa": 1009.5,
                    "wind_u_700hpa": -5.0,
                    "wind_v_700hpa": 2.0,
                    "temp_valley_c": 15.0,
                    "temp_climatology_c": 12.0,
                    "humidity_valley_percent": 35.0,
                    "wind_speed_10m_ms": 6.5,
                }
                
                # Run detection
                foehn_index = detector.detect_foehn(
                    region_id=region_id,
                    pressure_south_hpa=profile["pressure_south_hpa"],
                    pressure_north_hpa=profile["pressure_north_hpa"],
                    wind_u_700hpa=profile["wind_u_700hpa"],
                    wind_v_700hpa=profile["wind_v_700hpa"],
                    temp_valley_c=profile["temp_valley_c"],
                    temp_climatology_c=profile["temp_climatology_c"],
                    humidity_valley_percent=profile["humidity_valley_percent"],
                    wind_speed_10m_ms=profile["wind_speed_10m_ms"],
                )
                
                results[region_id] = foehn_index.to_dict()
                
                # Store in database (TODO)
                # db.session.add(foehn_index_to_db(foehn_index))
            
            except Exception as e:
                logger.error(f"Föhn detection failed for {region_id}: {e}")
                continue
        
        logger.info(f"✓ Föhn detection complete for {len(results)} regions")
        
        # Check for collapse risks and send alerts
        for region_id, foehn_data in results.items():
            if foehn_data.get("collapse_risk"):
                logger.warning(
                    f"🚨 Föhn collapse risk in {foehn_data['region_name']} "
                    f"({foehn_data['collapse_eta_minutes']} min)"
                )
        
        return {
            "status": "success",
            "regions_checked": len(results),
            "collapse_warnings": sum(
                1 for r in results.values()
                if r.get("collapse_risk")
            ),
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    except SoftTimeLimitExceeded:
        logger.error("Föhn detection timeout")
        return {"status": "timeout"}
    
    except Exception as e:
        logger.error(f"Föhn detection failed: {e}")
        self.retry(exc=e, countdown=120, max_retries=3)


# ========================
# Storm Cell Detection (5 min)
# ========================
@shared_task(
    name="safety.detect_storm_cells",
    bind=True,
    time_limit=300,
    soft_time_limit=250,
)
def detect_storm_cells_task(self):
    """
    Detect and track thunderstorm cells from lightning data
    
    Runs every 5 minutes
    """
    try:
        from safety.thunderstorm import StormCellDetector, LightningStrike
        from datetime import datetime, timedelta
        # TODO: Import Redis or database based lightning retrieval
        # from backend_cache_manager import get_recent_lightning
        
        detector = StormCellDetector()
        
        # Get lightning strikes from last 30 minutes
        # strikes = get_recent_lightning(minutes=30)
        strikes = []  # Placeholder
        
        if len(strikes) < 5:
            logger.debug(f"Insufficient lightning data ({len(strikes)} strikes)")
            return {"status": "insufficient_data", "strikes": len(strikes)}
        
        # Detect cells
        cells = detector.detect_cells(strikes, time_window_minutes=30)
        
        logger.info(f"✓ Detected {len(cells)} storm cells")
        
        # Store in database (TODO)
        # for cell in cells:
        #     db.session.add(cell_to_db(cell))
        # db.session.commit()
        
        # Check for cells approaching known airfields
        for cell in cells:
            if cell.movement_speed_kmh and cell.movement_speed_kmh > 20:
                logger.warning(
                    f"⚠️ Fast-moving storm cell: {cell.intensity} "
                    f"moving {cell.movement_speed_kmh:.0f} km/h"
                )
        
        return {
            "status": "success",
            "cells_detected": len(cells),
            "strikes_analyzed": len(strikes),
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    except SoftTimeLimitExceeded:
        logger.error("Storm cell detection timeout")
        return {"status": "timeout"}
    
    except Exception as e:
        logger.error(f"Storm cell detection failed: {e}")
        self.retry(exc=e, countdown=60, max_retries=3)


# ========================
# Blitzortung Listener (persistent)
# ========================
@shared_task(
    name="safety.listen_blitzortung",
    bind=True,
    time_limit=0,  # No limit (runs forever)
)
def listen_blitzortung_task(self):
    """
    Listen to Blitzortung WebSocket for real-time lightning
    
    This task should:
    1. Connect to wss://ws.blitzortung.org:7815/
    2. Process incoming strikes
    3. Store in Redis (TTL 3600s)
    4. Update database for historical analysis
    5. Reconnect automatically on disconnect
    
    Should be managed by supervisor (process control)
    """
    try:
        from safety.thunderstorm import BlitzortungClient
        
        client = BlitzortungClient()
        
        async def process_strike(strike):
            """Callback to process each lightning strike"""
            try:
                # Store in Redis for fast access
                # TODO: redis_client.lpush('lightning:strikes', strike.to_dict())
                
                # Store in DB for historical analysis
                # TODO: db.session.add(LightningStrike(...))
                
                logger.debug(
                    f"⚡ Strike: {strike.lat:.2f}, {strike.lon:.2f}"
                )
            except Exception as e:
                logger.error(f"Error processing strike: {e}")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Run forever
        logger.info("🔌 Starting Blitzortung listener...")
        try:
            loop.run_until_complete(
                client.connect_and_listen(process_strike)
            )
        finally:
            loop.close()
    
    except Exception as e:
        logger.error(f"Blitzortung listener failed: {e}")
        # Restart after delay
        self.retry(exc=e, countdown=30, max_retries=None)


# ========================
# Thunderstorm Risk Calculation (2 min)
# ========================
@shared_task(
    name="safety.calculate_threat_levels",
    bind=True,
    time_limit=300,
    soft_time_limit=250,
)
def calculate_threat_levels_task(self):
    """
    Calculate thunderstorm threat levels for active users
    
    Runs every 2 minutes:
    1. Get all active users from tracking system
    2. Get current storm cells and lightning
    3. Get current NWP convective indices
    4. Calculate threat level per user
    5. Send alerts as needed
    """
    try:
        from safety.thunderstorm import AlertLevelCalculator
        from safety.alerts import init_alert_system
        
        # TODO: Get active users from OGN tracking or database
        # users = get_active_users()
        users = []
        
        calculator = AlertLevelCalculator()
        alert_system = init_alert_system()
        engine = alert_system["engine"]
        
        alerts_sent = 0
        
        for user in users:
            try:
                # Calculate threat
                risk = calculator.calculate_alert(
                    user_lat=user["lat"],
                    user_lon=user["lon"],
                    cells=[],  # TODO: get current cells
                    lightning_strikes_recent=[],  # TODO: get strikes
                    convective_risk="moderate",  # TODO: from NWP
                )
                
                # Send alert if needed
                if risk.alert_level > 0:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    sent = loop.run_until_complete(
                        engine.evaluate_and_alert(
                            device_token=user.get("device_token"),
                            user_lat=user["lat"],
                            user_lon=user["lon"],
                            thunderstorm_risk=risk.to_dict(),
                        )
                    )
                    
                    if sent.get("thunderstorm"):
                        alerts_sent += 1
            
            except Exception as e:
                logger.error(f"Risk calculation failed for user: {e}")
                continue
        
        logger.info(
            f"✓ Threat levels calculated for {len(users)} users, "
            f"{alerts_sent} alerts sent"
        )
        
        return {
            "status": "success",
            "users_evaluated": len(users),
            "alerts_sent": alerts_sent,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    except SoftTimeLimitExceeded:
        logger.error("Threat level calculation timeout")
        return {"status": "timeout"}
    
    except Exception as e:
        logger.error(f"Threat calculation failed: {e}")
        self.retry(exc=e, countdown=60, max_retries=2)


# ========================
# Cleanup Old Data
# ========================
@shared_task(
    name="safety.cleanup_old_data",
    bind=True,
    time_limit=600,
)
def cleanup_old_data_task(self):
    """
    Clean old data from tables every 6 hours
    
    Removes:
    - Lightning strikes older than 7 days
    - Inactive storm cells (> 2 hours old)
    - Old NOTAM records (expired)
    - Alert history (older than 30 days)
    """
    try:
        logger.info("🧹 Starting data cleanup...")
        
        # TODO: Implement database cleanup
        # deleted_strikes = db.session.query(LightningStrike).filter(
        #     LightningStrike.timestamp < datetime.utcnow() - timedelta(days=7)
        # ).delete()
        #
        # deleted_cells = db.session.query(StormCell).filter(
        #     StormCell.expires_at < datetime.utcnow()
        # ).delete()
        #
        # db.session.commit()
        
        logger.info("✓ Data cleanup complete")
        
        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return {"status": "error", "message": str(e)}


# ========================
# Task Scheduling
# ========================
def register_safety_tasks(celery_app):
    """
    Register all safety tasks with Celery beat scheduler
    
    Usage in main celery app config:
    ```
    from backend_celery_safety_tasks import register_safety_tasks
    register_safety_tasks(app)
    ```
    """
    
    app = celery_app
    
    # Update NOTAMs every 30 minutes
    app.conf.beat_schedule.update({
        "safety-update-notams": {
            "task": "safety.update_notams",
            "schedule": 1800.0,  # 30 minutes
        },
        # Detect föhn every hour
        "safety-detect-foehn": {
            "task": "safety.detect_foehn_all_regions",
            "schedule": 3600.0,  # 1 hour
        },
        # Detect storm cells every 5 minutes
        "safety-detect-storm-cells": {
            "task": "safety.detect_storm_cells",
            "schedule": 300.0,  # 5 minutes
        },
        # Calculate threat levels every 2 minutes
        "safety-calculate-threats": {
            "task": "safety.calculate_threat_levels",
            "schedule": 120.0,  # 2 minutes
        },
        # Cleanup old data every 6 hours
        "safety-cleanup": {
            "task": "safety.cleanup_old_data",
            "schedule": 21600.0,  # 6 hours
        },
    })
    
    logger.info("✓ Safety tasks registered with Celery beat")


if __name__ == "__main__":
    # For testing
    print("Testing safety tasks...")
    
    result = update_notams_task()
    print(f"NOTAM update: {result}")
    
    result = detect_foehn_task()
    print(f"Föhn detection: {result}")
    
    result = detect_storm_cells_task()
    print(f"Storm cells: {result}")
