"""
Copernicus Digital Elevation Model (DEM) Processor

Downloads GLO-30 (30m resolution) DEM tiles for Europe from AWS S3.
ONE-TIME download. After completed, never re-download unless triggered by admin.

Source: AWS S3 public bucket (no authentication)
  s3://copernicus-dem-30m/

Coverage: 35°N–72°N, 25°W–45°E (all of Europe)
Tile naming: Copernicus_DSM_COG_10_{LAT}_{LON}_DEM.tif

Storage: /data/dem/tiles/
Merged VRT: /data/dem/europe_30m.vrt
"""

import os
import logging
from typing import List, Tuple, Optional
import asyncio
from pathlib import Path

logger = logging.getLogger(__name__)


class CopernicusDEMDownloader:
    """
    Downloads DEM tiles from AWS S3 with anonymous access.
    """
    
    S3_BUCKET = "copernicus-dem-30m"
    S3_REGION = "us-west-2"
    TILE_SIZE_DEG = 1  # 1x1 degree tiles
    
    # Coverage bounds for Europe
    COVERAGE = {
        "lat_min": 35,
        "lat_max": 72,
        "lon_min": -25,
        "lon_max": 45
    }
    
    RESOLUTION_M = 30
    
    def __init__(self, output_dir: str = "/opt/glidemate-backend/data/dem/tiles"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def get_tile_coordinates(self) -> List[Tuple[int, int]]:
        """
        Generate list of (lat, lon) tile coordinates for entire Europe.
        """
        tiles = []
        for lat in range(self.COVERAGE["lat_min"], self.COVERAGE["lat_max"]):
            for lon in range(self.COVERAGE["lon_min"], self.COVERAGE["lon_max"]):
                # GeoTIFF tiles are organized by SW corner
                # Format: N00E006 (north) or S00W006 (south)
                tiles.append((lat, lon))
        return tiles
    
    def _format_tile_name(self, lat: int, lon: int) -> str:
        """
        Format tile name per Copernicus naming convention.
        
        Examples:
        - N45E010_Copernicus_DSM_COG_10_N45_E010_DEM.tif
        - N00W005_Copernicus_DSM_COG_10_N00_W005_DEM.tif
        """
        lat_str = f"N{abs(lat):02d}" if lat >= 0 else f"S{abs(lat):02d}"
        lon_str = f"E{abs(lon):03d}" if lon >= 0 else f"W{abs(lon):03d}"
        
        tile_code = f"{lat_str}{lon_str}"
        return f"Copernicus_DSM_COG_10_{lat_str}_{lon_str}_DEM.tif"
    
    def _get_s3_key(self, lat: int, lon: int) -> str:
        """
        Build S3 object key for tile.
        Format: Copernicus_DSM_COG_10_{LAT}_{LON}/Copernicus_DSM_COG_10_{LAT}_{LON}_DEM.tif
        """
        lat_str = f"N{abs(lat):02d}" if lat >= 0 else f"S{abs(lat):02d}"
        lon_str = f"E{abs(lon):03d}" if lon >= 0 else f"W{abs(lon):03d}"
        
        tile_code = f"{lat_str}{lon_str}"
        filename = self._format_tile_name(lat, lon)
        
        return f"Copernicus_DSM_COG_10_{lat_str}_{lon_str}/{filename}"
    
    async def download_tile(self, lat: int, lon: int) -> bool:
        """
        Download a single DEM tile from S3.
        Uses anonymous access (no AWS credentials).
        """
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config
        
        s3_key = self._get_s3_key(lat, lon)
        local_path = self.output_dir / self._format_tile_name(lat, lon)
        
        # Skip if already downloaded
        if local_path.exists():
            logger.debug(f"DEM tile {lat}/{lon} already exists, skipping")
            return True
        
        try:
            # Create S3 client with anonymous access
            s3 = boto3.client(
                's3',
                region_name=self.S3_REGION,
                config=Config(signature_version=UNSIGNED)
            )
            
            logger.info(f"Downloading DEM tile {s3_key}...")
            
            s3.download_file(
                self.S3_BUCKET,
                s3_key,
                str(local_path)
            )
            
            logger.info(f"Downloaded {local_path.name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to download DEM tile {lat}/{lon}: {e}")
            return False
    
    async def download_region(
        self,
        lat_min: int,
        lat_max: int,
        lon_min: int,
        lon_max: int,
        parallel_tasks: int = 4
    ) -> Tuple[int, int]:
        """
        Download DEM tiles for a region (fast parallel downloads).
        
        Args:
            lat_min, lat_max, lon_min, lon_max: Region bounds
            parallel_tasks: Number of concurrent downloads
        
        Returns:
            (successful_count, failed_count)
        """
        tiles = [
            (lat, lon)
            for lat in range(lat_min, lat_max + 1)
            for lon in range(lon_min, lon_max + 1)
        ]
        
        logger.info(f"Downloading {len(tiles)} DEM tiles for region {lat_min}-{lat_max}, {lon_min}-{lon_max}")
        
        success = 0
        failed = 0
        
        # Process in batches
        for i in range(0, len(tiles), parallel_tasks):
            batch = tiles[i:i+parallel_tasks]
            results = await asyncio.gather(
                *[self.download_tile(lat, lon) for lat, lon in batch],
                return_exceptions=True
            )
            
            for result in results:
                if isinstance(result, Exception):
                    failed += 1
                elif result:
                    success += 1
                else:
                    failed += 1
        
        logger.info(f"DEM download complete: {success} succeeded, {failed} failed")
        return success, failed
    
    async def download_europe_full(self) -> Tuple[int, int]:
        """
        Download ALL DEM tiles for Europe (35°N–72°N, 25°W–45°E).
        This is a massive operation (~3000 tiles).
        Consider downloading by region instead.
        """
        logger.warning("Starting full Europe DEM download (this may take hours)")
        return await self.download_region(
            self.COVERAGE["lat_min"],
            self.COVERAGE["lat_max"],
            self.COVERAGE["lon_min"],
            self.COVERAGE["lon_max"],
            parallel_tasks=8  # Increase parallelism for large downloads
        )
    
    def create_vrt(self, output_path: str = "/opt/glidemate-backend/data/dem/europe_30m.vrt") -> bool:
        """
        Merge all downloaded tiles into a single VRT (virtual raster).
        
        Requires GDAL: gdalbuildvrt
        VRT is a lightweight index file pointing to actual tiles.
        """
        try:
            import subprocess
            
            tile_pattern = str(self.output_dir / "*.tif")
            
            logger.info(f"Creating VRT from tiles: {tile_pattern}")
            
            result = subprocess.run(
                ["gdalbuildvrt", output_path, tile_pattern],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                logger.info(f"VRT created successfully: {output_path}")
                return True
            else:
                logger.error(f"VRT creation failed: {result.stderr}")
                return False
        
        except Exception as e:
            logger.error(f"VRT creation error: {e}")
            logger.info("Install GDAL: apt-get install gdal-bin")
            return False
    
    def get_downloaded_tiles(self) -> int:
        """Count downloaded tile files."""
        return len(list(self.output_dir.glob("*.tif")))
    
    def get_download_status(self) -> dict:
        """Get DEM download status."""
        total_needed = (
            (self.COVERAGE["lat_max"] - self.COVERAGE["lat_min"]) *
            (self.COVERAGE["lon_max"] - self.COVERAGE["lon_min"])
        )
        downloaded = self.get_downloaded_tiles()
        
        return {
            "downloaded": downloaded,
            "total_needed": total_needed,
            "percent_complete": round(100 * downloaded / total_needed, 1) if total_needed > 0 else 0,
            "data_directory": str(self.output_dir),
            "tiles_dir_exists": self.output_dir.exists(),
            "vrt_exists": os.path.exists("/opt/glidemate-backend/data/dem/europe_30m.vrt")
        }


# Public API functions

async def download_dem_europe_region(
    lat_min: int = 43,
    lat_max: int = 52,
    lon_min: int = -5,
    lon_max: int = 10
) -> dict:
    """
    Download DEM for a specific region (e.g., France).
    
    Used by:
    - Celery task: download_copernicus_dem
    - Admin endpoint
    """
    downloader = CopernicusDEMDownloader()
    success, failed = await downloader.download_region(lat_min, lat_max, lon_min, lon_max)
    
    return {
        "success": success,
        "failed": failed,
        "status": downloader.get_download_status()
    }


def check_dem_download_status() -> dict:
    """Check current DEM download status."""
    downloader = CopernicusDEMDownloader()
    return downloader.get_download_status()


def create_dem_vrt() -> bool:
    """Create VRT file from downloaded tiles."""
    downloader = CopernicusDEMDownloader()
    return downloader.create_vrt()


def initialize_dem_download_task():
    """
    Initialize DEM download for important regions.
    Called once at backend startup if DEM not yet downloaded.
    """
    logger.info("Initializing DEM download for critical regions...")
    
    status = check_dem_download_status()
    
    if status["percent_complete"] == 0:
        logger.info("No DEM tiles found, scheduling download for France region...")
        # Download France + Alps + Alsace region
        # This will be picked up by Celery task
        return {
            "message": "DEM download scheduled for France region",
            "region": "France (43°N–52°N, 5°W–10°E)",
            "expected_tiles": 80,
            "action": "Task will run once daily at low priority"
        }
    
    return {
        "message": "DEM already partially downloaded",
        "status": status
    }
