#!/usr/bin/env python3
r"""
DWD ICON-EU GRIB2 Integration - CORRECTED
Real download from DWD Open Data with correct URL structure
File format: icon-eu_europe_regular-lat-lon_single-level_YYYYMDDDHH_FFF_VARIABLE.grib2.bz2
"""

import logging
import os
import asyncio
import bz2
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request
import urllib.error

try:
    import xarray as xr
    import cfgrib
except ImportError:
    pass  # Optional for now

logger = logging.getLogger(__name__)


class DWDIconProvider:
    """
    Real DWD ICON-EU provider
    Correct URL structure from: https://opendata.dwd.de/weather/nwp/icon-eu/grib/HH/VARIABLE/
    
    File format:
    icon-eu_europe_regular-lat-lon_single-level_YYYYMDDDHH_FFF_VARIABLE.grib2.bz2
    Where:
    - YYYYMDDDHH = Run time (2026030212 = 2026-03-02 12:00 UTC)
    - FFF = Forecast hour (000, 001, ... 048)
    - VARIABLE = t_2m, td_2m, u_10m, v_10m, cape_ml, etc.
    """
    
    BASE_URL = "https://opendata.dwd.de/weather/nwp/icon-eu/grib"
    CACHE_DIR = Path("/tmp/dwd_icon")
    
    # Variables available on DWD
    VARIABLES = [
        "t_2m",      # Temperature 2m
        "td_2m",     # Dewpoint 2m
        "u_10m",     # Wind U 10m
        "v_10m",     # Wind V 10m
        "cape_ml",   # CAPE ML
    ]
    
    def __init__(self):
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def get_latest_run_time():
        """Determine latest available 6-hourly model run (00, 06, 12, 18 UTC)"""
        now = datetime.utcnow()
        hour = now.hour
        
        # Files available ~2 hours after run
        if hour < 2:
            run_hour = (now - timedelta(hours=24)).replace(hour=18, minute=0, second=0)
        elif hour < 8:
            run_hour = now.replace(hour=0, minute=0, second=0)
        elif hour < 14:
            run_hour = now.replace(hour=6, minute=0, second=0)
        elif hour < 20:
            run_hour = now.replace(hour=12, minute=0, second=0)
        else:
            run_hour = now.replace(hour=18, minute=0, second=0)
        
        return run_hour
    
    async def download_file(self, variable, forecast_hour="000"):
        """
        Download single GRIB2 file
        Returns decompressed file path or None on failure
        """
        run_time = self.get_latest_run_time()
        run_str = run_time.strftime("%Y%m%d%H")
        hour_str = run_time.hour
        
        # DWD filename format
        bz2_filename = f"icon-eu_europe_regular-lat-lon_single-level_{run_str}_{forecast_hour}_{variable.upper()}.grib2.bz2"
        
        # DWD URL: /grib/HH/VARIABLE/filename
        url = f"{self.BASE_URL}/{hour_str:02d}/{variable}/{bz2_filename}"
        
        filepath_bz2 = self.CACHE_DIR / bz2_filename
        filepath_extracted = self.CACHE_DIR / bz2_filename.replace('.bz2', '')
        
        # Return cached decompressed file
        if filepath_extracted.exists():
            return str(filepath_extracted)
        
        # Download BZ2
        try:
            logger.info(f"  Downloading: {variable} +{int(forecast_hour):02d}h from {url}")
            urllib.request.urlretrieve(url, str(filepath_bz2))
            
            # Decompress
            with bz2.open(str(filepath_bz2), 'rb') as bzf:
                with open(str(filepath_extracted), 'wb') as f:
                    f.write(bzf.read())
            
            logger.info(f"  ✅ {variable} +{int(forecast_hour):02d}h ready")
            os.remove(str(filepath_bz2))  # Delete BZ2 after extract
            return str(filepath_extracted)
            
        except urllib.error.HTTPError as e:
            logger.warning(f"  ❌ HTTP {e.code}: {url}")
        except Exception as e:
            logger.warning(f"  ❌ Download failed: {e}")
        
        return None
    
    async def process_model_run(self):
        """Download complete model run for all variables and forecast hours"""
        run_time = self.get_latest_run_time()
        logger.info(f"🚀 Downloading ICON-EU for {run_time}")
        
        results = {}
        forecast_hours = ["000", "006", "012", "018", "024", "030", "036", "042", "048"]
        
        for variable in self.VARIABLES:
            logger.info(f"  📥 {variable}...")
            for fh in forecast_hours:
                filepath = await self.download_file(variable, fh)
                if filepath:
                    results[f"{variable}_{fh}"] = filepath
        
        logger.info(f"✅ Downloaded {len(results)} files")
        return results


async def ingest_dwd_icon_real(db=None):
    """Real DWD ICON-EU ingestion for Celery"""
    try:
        provider = DWDIconProvider()
        results = await provider.process_model_run()
        logger.info(f"✅ ICON ingestion: {len(results)} files downloaded")
        return {'status': 'success', 'files': len(results)}
    except Exception as e:
        logger.error(f"❌ ICON ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}


class DWDRadolanProvider:
    """DWD RADOLAN Precipitation Radar"""
    
    BASE_URL = "https://opendata.dwd.de/weather/radar/radolan/rw"
    CACHE_DIR = Path("/tmp/dwd_radolan")
    
    def __init__(self):
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    async def download_latest(self):
        """Download latest RADOLAN composite"""
        now = datetime.utcnow() 
        
        # RADOLAN filename: raa01-rw_10000-YYYYMMDDHHMM-dwd.bin.gz
        minute = (now.minute // 5) * 5
        time_str = now.strftime(f'%Y%m%d%H') + f"{minute:02d}"
        filename = f"raa01-rw_10000-{time_str}-dwd.bin.gz"
        
        url = f"{self.BASE_URL}/{filename}"
        filepath = self.CACHE_DIR / filename
        
        try:
            logger.info(f"  Downloading RADOLAN...")
            urllib.request.urlretrieve(url, str(filepath))
            logger.info(f"  ✅ RADOLAN downloaded")
            return str(filepath)
        except Exception as e:
            logger.warning(f"  ❌ RADOLAN failed: {e}")
            return None


async def ingest_dwd_radolan_real(db=None):
    """Real DWD RADOLAN ingestion for Celery"""
    try:
        provider = DWDRadolanProvider()
        filepath = await provider.download_latest()
        if filepath:
            logger.info(f"✅ RADOLAN ingestion complete")
            return {'status': 'success', 'file': filepath}
        else:
            return {'status': 'failed'}
    except Exception as e:
        logger.error(f"❌ RADOLAN ingestion failed: {e}")
        return {'status': 'error', 'message': str(e)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_time = DWDIconProvider.get_latest_run_time()
    print(f"Latest run: {run_time}")
    print(f"Checking ICON files...")

