"""
celery_tasks/data_ingestion_phase3.py
Updated data ingestion tasks with real API integration
Replace backend_celery_data_ingestion.py with this version
"""

import logging
import asyncio
import json
from datetime import datetime
from celery import shared_task
from celery_app import app

# Import data providers
from services.data_providers import (
    METARTAFProvider,
    NOAASoundingsProvider,
    OpenAIPProvider,
)

# Import database models
from database.models import (
    MetarReport, TafReport, SoundingProfile, Airspace
)

# Import algorithms
from algorithms.weather_algorithms import MeteorologicalAlgorithms

# Import settings
from config.settings import Settings

# Import DWD integration
from backend_dwd_integration import (
    DWDIconProvider,
    DWDRadolanProvider,
    ingest_dwd_icon_real,
    ingest_dwd_radolan_real
)

# Import KNMI integration
from backend_knmi_integration import (
    ingest_knmi_harmonie_real,
    ingest_knmi_insitu_real
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = Settings()


# ════════════════════════════════════════════════════════════════
# METAR / TAF Ingestion
# ════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=3)
def ingest_dwd_metar_taf(self):
    """
    Fetch METAR and TAF from AVWX API
    Runs every 30 minutes
    """
    try:
        logger.info("Starting METAR/TAF ingestion...")
        
        # Initialize provider
        provider = METARTAFProvider(api_key=settings.AVWX_API_KEY)
        
        # List of major European stations (expand as needed)
        stations = [
            'EDDF', 'EDDM', 'EDDS', 'EDDH',  # Germany
            'EGLL', 'EGSS', 'EGCC',            # UK
            'LFPG', 'LFPO', 'NZAA',            # France
            'LIRF', 'LIPZ', 'LIRN',            # Italy
            'LEMD', 'LEIB',                    # Spain
            'LOWW', 'LOWK',                    # Austria
            'UUWW', 'UK78',                    # Russia/Ukraine
            'LZIB', 'LKPR',                    # Czech/Slovakia
            'OPKC', 'KSEA',                    # Other
        ]
        
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker, Session
        
        engine = create_engine(settings.DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        
        try:
            # Run async operations
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            metar_count = 0
            taf_count = 0
            
            for icao in stations:
                # Fetch METAR
                try:
                    metar_data = loop.run_until_complete(provider.fetch_metar(icao))
                    if metar_data:
                        metar_record = MetarReport(
                            icao=metar_data['icao'],
                            station_name=metar_data.get('station_name', ''),
                            lat=metar_data.get('lat'),
                            lon=metar_data.get('lon'),
                            observed_at=metar_data.get('observed_at'),
                            received_at=datetime.utcnow(),
                            raw_text=metar_data.get('raw_text', ''),
                            temperature_c=metar_data.get('temperature_c'),
                            dewpoint_c=metar_data.get('dewpoint_c'),
                            wind_direction=metar_data.get('wind_direction'),
                            wind_speed_ms=metar_data.get('wind_speed_ms'),
                            wind_gust_ms=metar_data.get('wind_gust_ms'),
                            visibility_m=metar_data.get('visibility_m'),
                            flight_category=metar_data.get('flight_category', 'UNKN'),
                            cloud_layers_json=metar_data.get('cloud_layers_json', '[]'),
                        )
                        db.add(metar_record)
                        metar_count += 1
                        logger.info(f"  ✓ METAR {icao}: {metar_data.get('temperature_c')}°C")
                except Exception as e:
                    logger.warning(f"  ✗ METAR {icao}: {e}")
                
                # Fetch TAF
                try:
                    taf_data = loop.run_until_complete(provider.fetch_taf(icao))
                    if taf_data:
                        taf_record = TafReport(
                            icao=taf_data['icao'],
                            station_name=taf_data.get('station_name', ''),
                            lat=taf_data.get('lat'),
                            lon=taf_data.get('lon'),
                            issued_at=taf_data.get('issued_at'),
                            valid_from=taf_data.get('valid_from'),
                            valid_to=taf_data.get('valid_to'),
                            received_at=datetime.utcnow(),
                            raw_text=taf_data.get('raw_text', ''),
                            groups_json=taf_data.get('groups_json', '[]'),
                        )
                        db.add(taf_record)
                        taf_count += 1
                        logger.info(f"  ✓ TAF {icao}")
                except Exception as e:
                    logger.warning(f"  ✗ TAF {icao}: {e}")
            
            db.commit()
            logger.info(f"METAR/TAF ingestion complete: {metar_count} METAR, {taf_count} TAF")
            
            return {
                'status': 'success',
                'metar_count': metar_count,
                'taf_count': taf_count,
                'timestamp': datetime.utcnow().isoformat()
            }
        
        finally:
            loop.close()
            db.close()
    
    except Exception as exc:
        logger.error(f"METAR/TAF ingestion failed: {exc}")
        raise self.retry(exc=exc, countdown=5 ** self.request.retries)


# ════════════════════════════════════════════════════════════════
# Radiosondes Ingestion
# ════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=3)
def ingest_noaa_soundings(self):
    """
    Fetch NOAA radiosondes from University of Wyoming
    Computes stability indices (CAPE, LI, K-index, etc.)
    Runs every 12 hours
    """
    try:
        logger.info("Starting NOAA soundings ingestion...")
        
        provider = NOAASoundingsProvider()
        
        # European radiosonde stations
        stations = [
            "10410",  # Munich
            "10438",  # Stuttgart
            "10491",  # Vienna
            "12120",  # Prague
            "11035",  # Lindenberg
            "07005",  # Payerne
            "06011",  # Nîmes
            "07110",  # Trappes
            "07755",  # Valencia
        ]
        
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        engine = create_engine(settings.DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            sounding_count = 0
            
            for station_id in stations:
                try:
                    # Fetch sounding
                    sounding = loop.run_until_complete(provider.fetch(station_id))
                    if not sounding or not sounding.get('levels'):
                        logger.warning(f"  ✗ Station {station_id}: No data")
                        continue
                    
                    # Compute stability indices
                    indices = MeteorologicalAlgorithms.compute_all_indices_from_levels(
                        sounding['levels']
                    )
                    
                    # Store in database
                    profile = SoundingProfile(
                        source='noaa',
                        station_id=str(station_id),
                        station_name=sounding.get('station_name', ''),
                        valid_at=datetime.fromisoformat(sounding['valid_at']),
                        received_at=datetime.utcnow(),
                        levels_json=json.dumps(sounding['levels']),
                        
                        # Store computed indices
                        cape=indices.cape,
                        cape_3km=indices.cape_3km,
                        lifted_index=indices.lifted_index,
                        k_index=indices.k_index,
                        total_totals=indices.total_totals,
                        showalter_index=indices.showalter_index,
                        boyden_convection_index=indices.boyden_convection_index,
                        ventilation_rate=indices.ventilation_rate,
                        lcl_m=indices.lcl_m,
                        freezing_level_m=indices.freezing_level_m,
                    )
                    db.add(profile)
                    sounding_count += 1
                    logger.info(f"  ✓ Station {station_id}: CAPE={indices.cape:.0f}, LI={indices.lifted_index:.1f}")
                
                except Exception as e:
                    logger.warning(f"  ✗ Station {station_id}: {e}")
            
            db.commit()
            logger.info(f"Soundings ingestion complete: {sounding_count} profiles")
            
            return {
                'status': 'success',
                'count': sounding_count,
                'timestamp': datetime.utcnow().isoformat()
            }
        
        finally:
            loop.close()
            db.close()
    
    except Exception as exc:
        logger.error(f"Soundings ingestion failed: {exc}")
        raise self.retry(exc=exc, countdown=5 ** self.request.retries)


# ════════════════════════════════════════════════════════════════
# Airspace Ingestion
# ════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=3)
def ingest_openaip(self):
    """
    Download and ingest OpenAIP airspace polygons
    Runs once daily
    """
    try:
        logger.info("Starting OpenAIP ingestion...")
        
        provider = OpenAIPProvider()
        
        # European bounds
        bounds = {
            'north': 70,
            'south': 35,
            'west': -15,
            'east': 40,
        }
        
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from geoalchemy2.elements import from_shape
        from shapely.geometry import shape
        
        engine = create_engine(settings.DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Fetch airspace
            airspace_list = loop.run_until_complete(provider.download(bounds))
            
            if not airspace_list:
                logger.warning("No airspace data retrieved")
                return {'status': 'success', 'count': 0}
            
            count = 0
            for airspace_data in airspace_list:
                try:
                    # Parse geometry
                    geom_dict = json.loads(airspace_data['geometry'])
                    geom_shape = shape(geom_dict)
                    
                    airspace = Airspace(
                        name=airspace_data['name'],
                        airspace_class=airspace_data.get('airspace_class', 'G'),
                        airspace_type=airspace_data.get('airspace_type', 'OTHER'),
                        lower_limit_ft=airspace_data.get('lower_limit_ft', 0),
                        upper_limit_ft=airspace_data.get('upper_limit_ft', 99999),
                        geometry=from_shape(geom_shape, srid=4326),
                        source='openaip',
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                    )
                    db.add(airspace)
                    count += 1
                except Exception as e:
                    logger.warning(f"  ✗ Airspace: {e}")
            
            db.commit()
            logger.info(f"OpenAIP ingestion complete: {count} polygons")
            
            return {
                'status': 'success',
                'count': count,
                'timestamp': datetime.utcnow().isoformat()
            }
        
        finally:
            loop.close()
            db.close()
    
    except Exception as exc:
        logger.error(f"OpenAIP ingestion failed: {exc}")
        raise self.retry(exc=exc, countdown=5 ** self.request.retries)


# ════════════════════════════════════════════════════════════════
# DWD ICON-EU Model Ingestion (6-hourly)
# ════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=3)
def ingest_dwd_icon(self):
    """
    Download and ingest DWD ICON-EU GRIB2 model data
    Runs every 6 hours (00, 06, 12, 18 UTC)
    
    Source: https://opendata.dwd.de/weather/nwp/icon-eu/grib/
    Resolution: 0.05° (~5.5 km)
    Coverage: Full Europe (23.5°W–62.5°E, 29.5°N–70.5°N)
    Forecast: 0-48 hours
    """
    try:
        logger.info("="*70)
        logger.info("DWD ICON-EU GRIB2 INGESTION (Real Data)")
        logger.info("="*70)
        
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        engine = create_engine(settings.DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Process with real DWD integration
            summary = loop.run_until_complete(ingest_dwd_icon_real(db))
            
            logger.info(f"\n✅ DWD ICON Ingestion Summary:")
            logger.info(f"   Downloaded variables: {summary.get('downloaded', [])}")
            logger.info(f"   Failed variables: {summary.get('failed', [])}")
            logger.info(f"   Processed: {summary.get('processed', [])}")
            
            return {
                'status': 'success',
                'summary': summary,
                'timestamp': datetime.utcnow().isoformat()
            }
        
        finally:
            loop.close()
            db.close()
    
    except Exception as exc:
        logger.error(f"❌ DWD ICON ingestion failed: {exc}", exc_info=True)
        raise self.retry(exc=exc, countdown=5 ** self.request.retries)


# ════════════════════════════════════════════════════════════════
# DWD RADOLAN Radar (5-minute updates)
# ════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=2)
def ingest_dwd_radolan(self):
    """
    Download latest DWD RADOLAN radar precipitation composite
    Runs every 5 minutes
    
    Source: https://opendata.dwd.de/weather/radar/radolan/rw/
    Resolution: 1 km² nationwide grid
    Update frequency: Every 5 minutes
    Latency: ~7-8 minutes
    """
    try:
        logger.info("DWD RADOLAN Precipitation Radar")
        
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        engine = create_engine(settings.DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            summary = loop.run_until_complete(ingest_dwd_radolan_real(db))
            
            if summary['status'] == 'success':
                logger.info(f"✅ RADOLAN: {summary.get('file', 'Downloaded')}")
            else:
                logger.warning(f"⚠️  RADOLAN: {summary.get('message', 'Failed')}")
            
            return summary
        
        finally:
            loop.close()
            db.close()
    
    except Exception as exc:
        logger.error(f"RADOLAN ingestion failed: {exc}")
        raise self.retry(exc=exc, countdown=60)  # Retry after 1 minute


# ════════════════════════════════════════════════════════════════
# KNMI HARMONIE-AROME NWP (3-hourly, priority for NL/BE/NRW)
# ════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=3)
def ingest_knmi_harmonie(self):
    """
    Download latest KNMI HARMONIE-AROME GRIB2 model data
    Runs every 3 hours (priority region: NL/BE/NRW 47°N–57°N, 2°W–16°E)
    
    Source: https://api.dataplatform.knmi.nl/open-data/v1/
    Dataset: harmonie-arome-cy43-p3-1-0
    Resolution: 5.5 km (~0.0483°)
    Update frequency: Every 3 hours (00, 03, 06, 09, 12, 15, 18, 21 UTC)
    Coverage: Netherlands, Belgium, parts of Germany (North Rhine-Westphalia)
    
    This is the PRIMARY source for the priority region,
    falls back to DWD ICON-EU if KNMI fetch fails.
    """
    try:
        logger.info("🇳🇱 KNMI HARMONIE-AROME NWP Model Ingestion")
        
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        engine = create_engine(settings.DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            summary = loop.run_until_complete(ingest_knmi_harmonie_real(db))
            
            if summary['status'] == 'success':
                logger.info(f"✅ KNMI HARMONIE: {summary.get('files', 0)} files downloaded")
            else:
                logger.warning(f"⚠️  KNMI HARMONIE: {summary.get('message', 'Failed')}")
            
            return summary
        
        finally:
            loop.close()
            db.close()
    
    except Exception as exc:
        logger.error(f"❌ KNMI HARMONIE ingestion failed: {exc}", exc_info=True)
        raise self.retry(exc=exc, countdown=5 ** self.request.retries)


# ════════════════════════════════════════════════════════════════
# KNMI In-Situ Observations (10-minute station data for NL/BE)
# ════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=2)
def ingest_knmi_insitu(self):
    """
    Download latest 10-minute in-situ meteorological observations from KNMI stations
    Runs every 10 minutes
    
    Source: https://api.dataplatform.knmi.nl/open-data/v1/
    Dataset: 10-minute-in-situ-meteorological-observations-1-0
    Coverage: Netherlands, Belgium stations
    Update frequency: Every 10 minutes
    Latency: ~2-3 minutes
    
    Provides ground-truth observations of:
    - Temperature, humidity, pressure
    - Wind speed/direction
    - Precipitation
    - Radiation (on selected stations)
    """
    try:
        logger.info("🇳🇱 KNMI In-Situ Observations (10-minute)")
        
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        engine = create_engine(settings.DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            summary = loop.run_until_complete(ingest_knmi_insitu_real(db))
            
            if summary['status'] == 'success':
                logger.info(f"✅ KNMI In-Situ: {summary.get('observations', 0)} observations")
            else:
                logger.warning(f"⚠️  KNMI In-Situ: {summary.get('message', 'Failed')}")
            
            return summary
        
        finally:
            loop.close()
            db.close()
    
    except Exception as exc:
        logger.error(f"KNMI In-Situ ingestion failed: {exc}")
        raise self.retry(exc=exc, countdown=60)  # Retry after 1 minute


# ════════════════════════════════════════════════════════════════
# Cleanup Tasks
# ════════════════════════════════════════════════════════════════

@shared_task
def cleanup_expired_data():
    """
    Delete old data beyond retention periods
    Runs hourly
    """
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from datetime import timedelta
        
        engine = create_engine(settings.DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        
        try:
            cutoff_time = datetime.utcnow()
            
            # Delete METAR older than 7 days
            metar_cutoff = cutoff_time - timedelta(days=7)
            metar_count = db.query(MetarReport)\
                .filter(MetarReport.observed_at < metar_cutoff)\
                .delete()
            
            # Delete sounding profiles older than 30 days
            sounding_cutoff = cutoff_time - timedelta(days=30)
            sounding_count = db.query(SoundingProfile)\
                .filter(SoundingProfile.valid_at < sounding_cutoff)\
                .delete()
            
            db.commit()
            
            logger.info(f"Cleaned up: {metar_count} METAR, {sounding_count} soundings")
            
            return {
                'status': 'success',
                'metar_deleted': metar_count,
                'sounding_deleted': sounding_count,
            }
        
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return {'status': 'error', 'message': str(e)}


if __name__ == '__main__':
    # For manual testing
    print("Phase 3 Data Ingestion Tasks")
    print("Use Celery to run these tasks")
