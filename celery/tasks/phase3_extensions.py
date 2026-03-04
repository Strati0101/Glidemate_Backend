"""
Celery Tasks for Phase 3 Extensions
- Météo-France AROME (1.3km)
- GeoSphere Austria INCA (1km)
- Copernicus DEM processing

These tasks integrate with the existing Celery beat schedule.
"""

import logging
from celery import shared_task
from datetime import datetime
import asyncio
from sqlalchemy.orm import Session

from backend_database_models import (
    MeteoFranceAromeForecast,
    GeoSphereAustriaForecast,
    GeoSphereAustriaObservation,
    TerrainContext
)
from backend_meteofrance_integration import get_meteofrance_forecast, MeteoFranceAROMEClient
from backend_geosphere_austria_integration import (
    get_geosphere_austria_forecast,
    get_geosphere_austria_observations,
    GeoSphereAustriaClient
)
from backend_dem_processor import download_dem_europe_region, check_dem_download_status
from backend_dem_analysis import analyze_terrain, cache_terrain_analysis

logger = logging.getLogger(__name__)


# =============================================================================
# MÉTÉO-FRANCE AROME TASKS
# =============================================================================

@shared_task(
    name='ingest_meteofrance_arome',
    bind=True,
    autoretry_for=(Exception,),
    max_retries=3,
    default_retry_delay=300  # Retry after 5 minutes
)
def ingest_meteofrance_arome(self):
    """
    Download Météo-France AROME forecast data.
    
    Schedule: Every 1 hour (priority 12)
    
    Coverage: 43°N–52°N, 5°W–10°E (France, Alsace, Lorraine, Swiss border)
    Resolution: 1.3km
    """
    try:
        from backend_database import SessionLocal
        import redis
        
        logger.info("Starting Météo-France AROME ingestion task")
        
        # Initialize clients
        redis_client = redis.from_url("redis://localhost:6379/0")
        
        # Priority grid points for ingestion
        priority_points = [
            # France
            (48.8, 2.4),  # Paris
            (43.3, -0.6),  # Bordeaux
            (45.5, -73.6),  # Generic France point
            # Alsace & Lorraine
            (48.6, 7.8),  # Strasbourg
            (49.1, 6.2),  # Metz
            # Alps
            (45.9, 6.6),  # Chamonix
            (47.0, 7.5),  # Jura
            # Swiss border
            (46.5, 8.3),  # Valais
        ]
        
        db = SessionLocal()
        ingested_count = 0
        
        for lat, lon in priority_points:
            try:
                # Async fetch wrapped in sync context
                loop = asyncio.get_event_loop()
                forecast = loop.run_until_complete(
                    get_meteofrance_forecast(lat, lon, redis_client=redis_client)
                )
                
                if not forecast:
                    logger.warning(f"No forecast for {lat}/{lon}")
                    continue
                
                # Store in database
                now = datetime.utcnow()
                
                # Create records for multiple forecast hours
                for forecast_hour in [0, 6, 12, 24, 36, 48]:
                    if forecast_hour > 48:
                        break
                    
                    record = MeteoFranceAromeForecast(
                        lat=lat,
                        lon=lon,
                        issued_at=now,
                        valid_at=datetime.utcfromtimestamp(
                            datetime.utcnow().timestamp() + forecast_hour * 3600
                        ),
                        forecast_hour=forecast_hour,
                        received_at=now,
                        # Extract values from forecast (depends on structure)
                        temperature_2m_c=forecast.get("data", {}).get("temperature_2m"),
                        cape_j_kg=forecast.get("data", {}).get("cape"),
                        # ... other fields from forecast dict
                    )
                    
                    db.merge(record)
                    ingested_count += 1
            
            except Exception as e:
                logger.error(f"Failed to ingest AROME for {lat}/{lon}: {e}")
                continue
        
        db.commit()
        db.close()
        
        logger.info(f"AROME ingestion complete: {ingested_count} records")
        return {"status": "success", "records_ingested": ingested_count}
    
    except Exception as exc:
        logger.error(f"AROME ingestion task failed: {exc}")
        raise self.retry(exc=exc)


# =============================================================================
# GEOSPHERE AUSTRIA TASKS
# =============================================================================

@shared_task(
    name='ingest_geosphere_austria_stations',
    bind=True,
    autoretry_for=(Exception,),
    max_retries=3,
    default_retry_delay=300
)
def ingest_geosphere_austria_stations(self):
    """
    Download current observations from Austrian TAWES weather stations.
    
    Schedule: Every 10 minutes (or 1 hour for Celery beat)
    Update Frequency: 10 minutes from source
    Coverage: Austria + border regions
    """
    try:
        from backend_database import SessionLocal
        
        logger.info("Starting GeoSphere Austria station observations ingestion")
        
        db = SessionLocal()
        
        # Async fetch
        loop = asyncio.get_event_loop()
        stations = loop.run_until_complete(get_geosphere_austria_observations())
        
        if not stations:
            logger.warning("No Austrian station observations received")
            return {"status": "no_data", "records": 0}
        
        ingested_count = 0
        for station in stations:
            try:
                record = GeoSphereAustriaObservation(
                    station_id=station.get("id"),
                    station_name=station.get("name"),
                    lat=station.get("lat"),
                    lon=station.get("lon"),
                    elevation_m=station.get("elevation_m"),
                    
                    observed_at=datetime.fromisoformat(
                        station.get("timestamp", "").replace("Z", "+00:00")
                    ) if station.get("timestamp") else datetime.utcnow(),
                    received_at=datetime.utcnow(),
                    
                    temperature_c=station.get("temperature_c"),
                    dewpoint_c=station.get("dewpoint_c"),
                    relative_humidity_pct=station.get("relative_humidity_pct"),
                    
                    wind_direction_deg=station.get("wind_direction_deg"),
                    wind_speed_ms=station.get("wind_speed_ms"),
                    
                    pressure_hpa=station.get("pressure_hpa"),
                    precipitation_mm=station.get("precipitation_mm"),
                    sunshine_hours=station.get("sunshine_hours")
                )
                
                db.merge(record)
                ingested_count += 1
            
            except Exception as e:
                logger.warning(f"Failed to parse station {station.get('id')}: {e}")
                continue
        
        db.commit()
        db.close()
        
        logger.info(f"Austrian stations ingestion complete: {ingested_count} records")
        return {"status": "success", "records_ingested": ingested_count}
    
    except Exception as exc:
        logger.error(f"Austrian stations ingestion failed: {exc}")
        raise self.retry(exc=exc)


@shared_task(
    name='ingest_geosphere_austria_inca',
    bind=True,
    autoretry_for=(Exception,),
    max_retries=3,
    default_retry_delay=300
)
def ingest_geosphere_austria_inca(self):
    """
    Download GeoSphere Austria INCA gridded forecast.
    
    Schedule: Every 1 hour (priority 11)
    
    Coverage: 46°N–49.5°N, 9°E–18°E (Austria, South Bavaria, South Tyrol)
    Resolution: 1km
    Max Forecast: 48 hours
    """
    try:
        from backend_database import SessionLocal
        
        logger.info("Starting GeoSphere Austria INCA ingestion")
        
        db = SessionLocal()
        
        # Async INCA fetch
        loop = asyncio.get_event_loop()
        
        # Priority grid points for Austria region
        priority_points = [
            (48.2, 16.4),  # Vienna
            (47.3, 11.4),  # Salzburg
            (47.3, 13.0),  # Hallstatt region
            (46.6, 14.3),  # Alpine region
            (47.5, 9.5),   # Vorarlberg
        ]
        
        ingested_count = 0
        
        for lat, lon in priority_points:
            try:
                forecast = loop.run_until_complete(get_geosphere_austria_forecast(lat, lon))
                
                if not forecast:
                    logger.warning(f"No INCA forecast for {lat}/{lon}")
                    continue
                
                now = datetime.utcnow()
                
                # Create records for multiple forecast hours
                for forecast_hour in [0, 3, 6, 12, 24, 36, 48]:
                    record = GeoSphereAustriaForecast(
                        lat=lat,
                        lon=lon,
                        issued_at=now,
                        valid_at=datetime.utcfromtimestamp(
                            now.timestamp() + forecast_hour * 3600
                        ),
                        forecast_hour=forecast_hour,
                        received_at=now,
                        temperature_2m_c=forecast.get("variables", {}).get("temperature_2m"),
                        precipitation_mm=forecast.get("variables", {}).get("precipitation"),
                        # ... other fields
                    )
                    
                    db.merge(record)
                    ingested_count += 1
            
            except Exception as e:
                logger.error(f"Failed to ingest INCA for {lat}/{lon}: {e}")
                continue
        
        db.commit()
        db.close()
        
        logger.info(f"INCA ingestion complete: {ingested_count} records")
        return {"status": "success", "records_ingested": ingested_count}
    
    except Exception as exc:
        logger.error(f"INCA ingestion task failed: {exc}")
        raise self.retry(exc=exc)


# =============================================================================
# COPERNICUS DEM TASKS
# =============================================================================

@shared_task(
    name='download_copernicus_dem',
    bind=True,
    autoretry_for=(Exception,),
    max_retries=2,
    default_retry_delay=3600  # Retry after 1 hour if failed
)
def download_copernicus_dem_task(
    self,
    region: str = "france",
    lat_min: int = 43,
    lat_max: int = 52,
    lon_min: int = -5,
    lon_max: int = 10
):
    """
    Download Copernicus DEM tiles for a region.
    
    ONE-TIME download per region. After successful completion,
    this task should NOT be run again for the same region.
    
    Common regions:
    - france: 43°N–52°N, 5°W–10°E
    - austria: 46°N–49.5°N, 9°E–18°E
    - alpine: 43°N–49.5°N, 4°E–16°E
    
    Schedule: One-time or daily low-priority task
    """
    try:
        logger.info(f"Starting Copernicus DEM download for region: {region}")
        
        # Check if already downloaded
        status = check_dem_download_status()
        
        if status.get("percent_complete", 0) > 0:
            logger.info(f"DEM partially downloaded: {status['percent_complete']}%")
        
        # Download region
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(
            download_dem_europe_region(
                lat_min=lat_min,
                lat_max=lat_max,
                lon_min=lon_min,
                lon_max=lon_max
            )
        )
        
        logger.info(f"DEM download result: {result}")
        
        # Create VRT if all tiles downloaded
        if result.get("status", {}).get("percent_complete") == 100:
            from backend_dem_processor import create_dem_vrt
            vrt_success = create_dem_vrt()
            logger.info(f"VRT creation: {'Success' if vrt_success else 'Failed'}")
        
        return {"status": "success", "region": region, "result": result}
    
    except Exception as exc:
        logger.error(f"DEM download task failed: {exc}")
        raise self.retry(exc=exc)


@shared_task(
    name='cache_terrain_analysis',
    bind=True,
    autoretry_for=(Exception,),
    max_retries=2,
    default_retry_delay=300
)
def cache_terrain_analysis_task(
    self,
    lat: float,
    lon: float,
    force_recompute: bool = False
):
    """
    Cache terrain analysis from DEM for a location.
    
    Stores ridge height, slope, aspect, and thermal factors in database.
    Called periodically for important soaring sites.
    
    Or triggered on-demand when user queries a location for the first time.
    """
    try:
        from backend_database import SessionLocal
        
        db = SessionLocal()
        
        # Check if already cached
        existing = db.query(TerrainContext).filter_by(
            latitude=lat,
            longitude=lon
        ).first()
        
        if existing and not force_recompute:
            logger.debug(f"Terrain already cached for {lat}/{lon}")
            return {"status": "cached", "lat": lat, "lon": lon}
        
        logger.info(f"Computing terrain analysis for {lat}/{lon}")
        
        # Compute terrain features
        analysis = analyze_terrain(lat, lon)
        
        if analysis.get("dem_available"):
            # Cache in database
            cache_terrain_analysis(db, lat, lon)
            logger.info(f"Terrain cached for {lat}/{lon}")
        else:
            logger.warning(f"DEM not available for {lat}/{lon}")
        
        db.close()
        
        return {
            "status": "success",
            "lat": lat,
            "lon": lon,
            "thermal_factor": analysis.get("combined_thermal_factor")
        }
    
    except Exception as exc:
        logger.error(f"Terrain analysis caching failed: {exc}")
        raise self.retry(exc=exc)


@shared_task(
    name='cache_terrain_regions',
    bind=True
)
def cache_terrain_regions_task(self, regions: list = None):
    """
    Batch cache terrain analysis for important soaring regions.
    
    Default regions:
    - Alpine (Chamonix, Interlaken, etc.)
    - French (Puy de Dôme, etc.)
    - Austrian (Hohe Tauern)
    """
    if regions is None:
        # Default important soaring sites
        regions = [
            # Alps
            {"lat": 45.9, "lon": 6.6, "name": "Chamonix"},
            {"lat": 46.5, "lon": 8.3, "name": "Valais"},
            {"lat": 47.0, "lon": 11.5, "name": "Zillertal"},
            
            # French thermals
            {"lat": 45.3, "lon": 3.0, "name": "Puy de Dôme"},
            {"lat": 44.5, "lon": 1.4, "name": "Pyrenees"},
            
            # Austria
            {"lat": 47.3, "lon": 12.0, "name": "Salzkammergut"},
            {"lat": 47.0, "lon": 14.5, "name": "Hohe Tauern"},
        ]
    
    results = []
    
    for region in regions:
        try:
            result = cache_terrain_analysis_task.delay(
                region.get("lat"),
                region.get("lon")
            )
            results.append({
                "name": region.get("name"),
                "task_id": result.id
            })
        except Exception as e:
            logger.error(f"Failed to queue terrain analysis for {region.get('name')}: {e}")
    
    logger.info(f"Queued terrain analysis for {len(results)} regions")
    return {"status": "queued", "regions": results}


# =============================================================================
# TOKEN RENEWAL TASKS
# =============================================================================

@shared_task(
    name='renew_meteofrance_token',
    bind=True,
    autoretry_for=(Exception,),
    max_retries=2,
    default_retry_delay=600  # Retry after 10 minutes if failed
)
def renew_meteofrance_token(self):
    """
    Automatically renew Météo-France AROME JWT token.
    
    Schedule: Every 55 minutes (before 60 minute expiry)
    
    This ensures credentials are always fresh and never expire,
    preventing "awaiting credentials" errors.
    
    Token lifetime: 3600 seconds (60 minutes)
    Renewal buffer: 300 seconds (5 minutes)
    Renewal interval: 3300 seconds (55 minutes)
    """
    try:
        import os
        import redis
        from backend_meteofrance_integration import MeteoFranceTokenManager
        from datetime import datetime
        
        logger.info("=" * 70)
        logger.info("AUTOMATIC METEOFRANCE TOKEN RENEWAL")
        logger.info("=" * 70)
        
        # Get credentials from environment
        credentials = os.getenv("METEOFRANCE_CLIENT_CREDENTIALS", "").strip()
        
        if not credentials:
            logger.error("[✗] AROME credentials: NOT FOUND in .env")
            return {
                "status": "error",
                "message": "Credentials not in .env",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        if credentials == "dGVzdF9jbGllbnQ6dGVzdF9zZWNyZXQ=":
            logger.warning("[!] AROME credentials: DEMO/TEST (not production)")
            return {
                "status": "demo_credentials",
                "message": "Using demo credentials",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        logger.info(f"[✓] AROME credentials: FOUND ({len(credentials)} chars)")
        
        # Initialize Redis and token manager
        redis_client = redis.from_url("redis://localhost:6379/0")
        
        token_manager = MeteoFranceTokenManager(
            redis_client=redis_client,
            client_credentials=credentials
        )
        
        # Renew token
        logger.info("Fetching fresh token from Météo-France API...")
        
        import asyncio
        loop = asyncio.get_event_loop()
        token = loop.run_until_complete(token_manager.get_valid_token())
        
        if token:
            logger.info("[✓] AROME token: RENEWED successfully")
            logger.info(f"    Valid until: {(datetime.utcnow().timestamp() + 3600)} (3600s from now)")
            logger.info(f"    Stored in Redis: meteofrance:token")
            
            # Log next renewal time
            next_renewal = datetime.utcnow()
            from datetime import timedelta
            next_renewal = next_renewal + timedelta(minutes=55)
            logger.info(f"    Next renewal: {next_renewal.isoformat()}")
        else:
            logger.error("[✗] AROME token: FETCH FAILED")
            return {
                "status": "error",
                "message": "Failed to fetch token",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        logger.info("=" * 70)
        logger.info("TOKEN RENEWAL: SUCCESS")
        logger.info("=" * 70)
        
        return {
            "status": "success",
            "message": "Token renewed successfully",
            "timestamp": datetime.utcnow().isoformat(),
            "next_renewal": (datetime.utcnow() + timedelta(minutes=55)).isoformat()
        }
    
    except Exception as exc:
        logger.error(f"Token renewal failed: {exc}", exc_info=True)
        logger.error("=" * 70)
        logger.error("TOKEN RENEWAL: FAILED")
        logger.error("=" * 70)
        
        # Retry this task
        raise self.retry(exc=exc)
