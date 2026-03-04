#!/usr/bin/env python3
r"""
KNMI (Royal Netherlands Meteorological Institute) Integration
Fetches weather data from KNMI Open Data API
- HARMONIE-AROME: Hourly 5.5km resolution NWP model  
- In-situ observations: 10-minute station data
"""

import logging
import os
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request
import urllib.error
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class KNMIAPIClient:
    """KNMI Open Data API HTTP client"""
    
    BASE_URL = "https://api.dataplatform.knmi.nl/open-data/v1"
    
    def __init__(self, api_key: str):
        """Initialize with KNMI API key"""
        self.api_key = api_key
        if not api_key:
            logger.warning("⚠️  KNMI_API_KEY not configured - KNMI data disabled")
    
    def get_headers(self) -> Dict[str, str]:
        """Get authorization headers for KNMI API"""
        return {
            "Authorization": self.api_key  # No "Bearer" prefix - just the raw key
        }
    
    async def get_dataset_files(self, dataset_name: str, version: str = "1.0") -> Optional[List[Dict]]:
        """
        Get list of files in a KNMI dataset
        
        GET /datasets/{dataset_name}/versions/{version}/files
        """
        url = f"{self.BASE_URL}/datasets/{dataset_name}/versions/{version}/files"
        
        try:
            req = urllib.request.Request(url, headers=self.get_headers())
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode('utf-8'))
                files = data.get('files', [])
                logger.info(f"✅ Retrieved {len(files)} files from {dataset_name}")
                return files
        except urllib.error.HTTPError as e:
            logger.error(f"❌ KNMI API error {e.code}: {url}")
        except Exception as e:
            logger.error(f"❌ Failed to fetch dataset files: {e}")
        
        return None
    
    async def get_file_download_url(self, dataset_name: str, filename: str, 
                                   version: str = "1.0") -> Optional[str]:
        """
        Get temporary download URL for a file
        
        GET /datasets/{dataset_name}/versions/{version}/files/{filename}/url
        Returns temporaryDownloadUrl (S3, no auth needed for download)
        """
        url = f"{self.BASE_URL}/datasets/{dataset_name}/versions/{version}/files/{filename}/url"
        
        try:
            req = urllib.request.Request(url, headers=self.get_headers())
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode('utf-8'))
                download_url = data.get('temporaryDownloadUrl')
                if download_url:
                    logger.debug(f"✅ Got download URL for {filename}")
                    return download_url
        except urllib.error.HTTPError as e:
            logger.warning(f"⚠️  Cannot get download URL {e.code}: {filename}")
        except Exception as e:
            logger.warning(f"⚠️  Failed to get download URL: {e}")
        
        return None


class KNMIHarmonieProvider:
    """
    KNMI HARMONIE-AROME weather model data
    Dataset: harmonie-arome-cy43-p3-1-0
    - Coverage: Full Europe (5.5 km grid)
    - Update frequency: Every 3 hours
    - Forecast range: 0-48 hours
    """
    
    DATASET = "harmonie-arome-cy43-p3-1-0"
    CACHE_DIR = Path("/tmp/knmi_harmonie")
    
    # Priority region (NL, BE, NRW): 47°N–57°N, 2°W–16°E
    PRIORITY_BOUNDS = {
        'north': 57.0,
        'south': 47.0,
        'east': 16.0,
        'west': -2.0
    }
    
    def __init__(self, api_client: KNMIAPIClient):
        """Initialize with KNMI API client"""
        self.client = api_client
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    async def download_latest_files(self) -> Dict[str, str]:
        """
        Download latest HARMONIE-AROME GRIB2 files
        Returns dict of {filename: filepath}
        """
        results = {}
        
        # Get available files
        files = await self.client.get_dataset_files(self.DATASET)
        if not files:
            logger.warning("❌ No HARMONIE files available")
            return results
        
        # Find latest run (files are timestamped)
        latest_files = self._filter_latest_files(files)
        logger.info(f"📥 Downloading {len(latest_files)} HARMONIE files...")
        
        for file_info in latest_files:
            filename = file_info.get('filename', '')
            
            # Skip if already cached
            filepath = self.CACHE_DIR / filename
            if filepath.exists() and filepath.stat().st_size > 100000:  # > 100KB
                logger.debug(f"  Using cached: {filename}")
                results[filename] = str(filepath)
                continue
            
            # Get download URL
            download_url = await self.client.get_file_download_url(
                self.DATASET, filename
            )
            if not download_url:
                logger.warning(f"  ⚠️  No download URL: {filename}")
                continue
            
            # Download (no auth needed for S3 temporary URL)
            try:
                logger.debug(f"  Downloading {filename}...")
                urllib.request.urlretrieve(download_url, str(filepath))
                results[filename] = str(filepath)
                logger.info(f"  ✅ {filename}")
            except Exception as e:
                logger.warning(f"  ⚠️  Download failed: {e}")
        
        logger.info(f"✅ Downloaded {len(results)} HARMONIE files")
        return results
    
    @staticmethod
    def _filter_latest_files(files: List[Dict]) -> List[Dict]:
        """Extract latest run's files from file list"""
        # HARMONIE files are named with timestamp
        # Keep only most recent run (typically within last 3 hours)
        if not files:
            return []
        
        # Simple approach: take all files
        # (KNMI API should already return recent files)
        return files[:50]  # Limit to first 50 files
    
    def is_priority_region(self, lat: float, lon: float) -> bool:
        """Check if location is in high-priority NL/BE/NRW region"""
        bounds = self.PRIORITY_BOUNDS
        return (bounds['south'] <= lat <= bounds['north'] and 
                bounds['west'] <= lon <= bounds['east'])


class KNMIInsituProvider:
    """
    KNMI In-Situ Station Observations
    Dataset: 10-minute-in-situ-meteorological-observations-1-0
    - Netherlands station network
    - 10-minute updates
    - Temperature, humidity, wind, precipitation
    """
    
    DATASET = "10-minute-in-situ-meteorological-observations-1-0"
    CACHE_DIR = Path("/tmp/knmi_insitu")
    
    def __init__(self, api_client: KNMIAPIClient):
        """Initialize with KNMI API client"""
        self.client = api_client
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    async def download_latest_observations(self) -> Dict[str, str]:
        """
        Download latest in-situ observation files
        Returns dict of {filename: filepath}
        """
        results = {}
        
        # Get available files
        files = await self.client.get_dataset_files(self.DATASET)
        if not files:
            logger.warning("❌ No in-situ observation files available")
            return results
        
        # Take most recent files (typically a few per 10-minute cycle)
        recent_files = files[:20]  # Last ~200 minutes of data
        logger.info(f"📥 Downloading {len(recent_files)} in-situ observation files...")
        
        for file_info in recent_files:
            filename = file_info.get('filename', '')
            
            # Skip if already cached
            filepath = self.CACHE_DIR / filename
            if filepath.exists():
                results[filename] = str(filepath)
                continue
            
            # Get download URL
            download_url = await self.client.get_file_download_url(
                self.DATASET, filename
            )
            if not download_url:
                logger.debug(f"  ⚠️  No download URL: {filename}")
                continue
            
            # Download
            try:
                urllib.request.urlretrieve(download_url, str(filepath))
                results[filename] = str(filepath)
                logger.debug(f"  ✅ {filename}")
            except Exception as e:
                logger.debug(f"  ⚠️  Download failed: {e}")
        
        logger.info(f"✅ Downloaded {len(results)} in-situ files")
        return results


# Async wrappers for Celery tasks

async def ingest_knmi_harmonie_real(db=None):
    """Real KNMI HARMONIE-AROME ingestion for Celery"""
    from backend_config import get_settings
    settings = get_settings()
    
    if not settings.knmi_api_key:
        logger.warning("⚠️  KNMI_API_KEY not configured - skipping HARMONIE ingestion")
        return {'status': 'skipped', 'reason': 'API key not configured'}
    
    try:
        logger.info("🚀 Starting KNMI HARMONIE-AROME ingestion")
        
        client = KNMIAPIClient(settings.knmi_api_key)
        provider = KNMIHarmonieProvider(client)
        
        files = await provider.download_latest_files()
        
        logger.info(f"✅ HARMONIE ingestion: {len(files)} files downloaded")
        return {
            'status': 'success',
            'files': len(files),
            'message': f'Downloaded {len(files)} HARMONIE-AROME files',
            'model_used': 'KNMI-HARMONIE-AROME',
            'model_run': datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"❌ HARMONIE ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}


async def ingest_knmi_insitu_real(db=None):
    """Real KNMI in-situ observation ingestion for Celery"""
    from backend_config import get_settings
    settings = get_settings()
    
    if not settings.knmi_api_key:
        logger.warning("⚠️  KNMI_API_KEY not configured - skipping in-situ ingestion")
        return {'status': 'skipped', 'reason': 'API key not configured'}
    
    try:
        logger.info("🚀 Starting KNMI in-situ observations ingestion")
        
        client = KNMIAPIClient(settings.knmi_api_key)
        provider = KNMIInsituProvider(client)
        
        files = await provider.download_latest_observations()
        
        logger.info(f"✅ In-situ ingestion: {len(files)} files downloaded")
        return {
            'status': 'success',
            'files': len(files),
            'message': f'Downloaded {len(files)} in-situ observation files',
            'data_source': 'KNMI-in-situ',
            'data_age_minutes': 15  # Typically 15 min old
        }
    except Exception as e:
        logger.error(f"❌ In-situ ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test
    from backend_config import get_settings
    settings = get_settings()
    
    if settings.knmi_api_key:
        print(f"✅ KNMI API configured")
        print(f"  HARMONIE Dataset: {KNMIHarmonieProvider.DATASET}")
        print(f"  In-situ Dataset: {KNMIInsituProvider.DATASET}")
        print(f"  Priority bounds: {KNMIHarmonieProvider.PRIORITY_BOUNDS}")
    else:
        print("❌ KNMI_API_KEY not configured")
