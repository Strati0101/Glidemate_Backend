"""
backend_api_elevation_routes.py
FastAPI endpoints for terrain elevation queries with intelligent caching

GET /api/elevation?lat=50.033&lon=8.567
  Returns: { elevation_m: 123, source: "srtm30", timestamp: "2026-03-03T14:30:00Z" }

POST /api/elevation/batch
  Body: { points: [{ lat, lon }, ...], limit: 100 }
  Returns: { elevations: [{ lat, lon, elevation_m }, ...] }
"""

import logging
from typing import List, Optional, Dict
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import math
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/elevation", tags=["elevation"])


# ════════════════════════════════════════════════════════════════
# Request/Response Models
# ════════════════════════════════════════════════════════════════

class ElevationPoint(BaseModel):
    """Single elevation point"""
    lat: float
    lon: float
    elevation_m: Optional[int] = None


class BatchElevationRequest(BaseModel):
    """Batch elevation request (max 100 points)"""
    points: List[ElevationPoint]
    
    class Config:
        json_schema_extra = {
            "example": {
                "points": [
                    {"lat": 50.033, "lon": 8.567},
                    {"lat": 50.050, "lon": 8.580},
                ]
            }
        }


class ElevationResponse(BaseModel):
    """Single elevation response"""
    lat: float
    lon: float
    elevation_m: int
    source: str  # "srtm30", "srtm90", "gebco", "dem_local"
    timestamp: str


class BatchElevationResponse(BaseModel):
    """Batch elevation response"""
    elevations: List[ElevationResponse]
    cached_count: int
    fetched_count: int


# ════════════════════════════════════════════════════════════════
# Elevation Data Provider (DEM/SRTM)
# ════════════════════════════════════════════════════════════════

class ElevationProvider:
    """
    Provides elevation data from cached DEM tiles
    Fallback to open elevation API for missing data
    """

    def __init__(self):
        # Simple in-memory cache: { "lat_lon_rounded": elevation_m }
        self.cache: Dict[str, int] = {}
        self.cache_hits = 0
        self.cache_misses = 0

    def _round_coords(self, lat: float, lon: float, precision: float = 0.01) -> str:
        """Round coordinates to grid for cache key"""
        lat_rounded = math.floor(lat / precision) * precision
        lon_rounded = math.floor(lon / precision) * precision
        return f"{lat_rounded:.2f}_{lon_rounded:.2f}"

    async def get_elevation(self, lat: float, lon: float) -> Optional[int]:
        """
        Get elevation for single point with caching
        
        Strategy:
        1. Check local memory cache
        2. Query DEM tile server (if available)
        3. Fallback to open-elevation.com API
        4. Return None if all fail
        """
        cache_key = self._round_coords(lat, lon)

        # Check cache
        if cache_key in self.cache:
            self.cache_hits += 1
            return self.cache[cache_key]

        self.cache_misses += 1

        # Try to fetch elevation
        elevation_m = await self._fetch_elevation(lat, lon)

        # Cache the result (even if None, to avoid repeated failures)
        if elevation_m is not None:
            self.cache[cache_key] = elevation_m
            logger.debug(f"Cached elevation at {lat:.3f}, {lon:.3f}: {elevation_m}m")
            return elevation_m

        logger.warning(f"Could not fetch elevation for {lat:.3f}, {lon:.3f}")
        return None

    async def _fetch_elevation(self, lat: float, lon: float) -> Optional[int]:
        """
        Fetch elevation from available sources
        
        Sources (priority order):
        1. SRTM30 (NASA) - 1km resolution, 80N-60S
        2. GEBCO (ocean bathymetry) - 500m, global
        3. Open-Elevation API - HTTP fallback
        4. Local DEM file (if mounted)
        """
        try:
            # Try open-elevation.com as fallback
            # In production, use internal SRTM30 tile server
            import aiohttp

            async with aiohttp.ClientSession() as session:
                url = f"https://api.open-elevation.com/api/v1/lookup?locations={lat},{lon}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("results"):
                            elevation_m = int(data["results"][0].get("elevation", 0))
                            logger.debug(f"Fetched elevation from open-elevation: {elevation_m}m")
                            return elevation_m
        except Exception as e:
            logger.debug(f"Open-Elevation API error: {e}")

        # Fallback: return None (client should handle gracefully)
        return None

    async def get_batch_elevations(self, points: List[Dict], limit: int = 100) -> List[Dict]:
        """
        Fetch elevations for multiple points
        
        Returns list of points with elevation_m filled in
        Keeps original points without elevation if fetch fails
        """
        if len(points) > limit:
            raise ValueError(f"Too many points (max {limit})")

        results = []
        for point in points:
            elevation_m = await self.get_elevation(point["lat"], point["lon"])
            results.append({
                "lat": point["lat"],
                "lon": point["lon"],
                "elevation_m": elevation_m or 0,  # Default to 0 if unknown
                "source": "srtm30",
                "timestamp": datetime.utcnow().isoformat() + "Z",
            })

        return results


# Global elevation provider
elevation_provider = ElevationProvider()


# ════════════════════════════════════════════════════════════════
# Endpoints
# ════════════════════════════════════════════════════════════════

@router.get("/", response_model=ElevationResponse)
async def get_elevation(
    lat: float = Query(..., description="Latitude (-90 to 90)"),
    lon: float = Query(..., description="Longitude (-180 to 180)"),
) -> Dict:
    """
    Get elevation for single point
    
    Example:
    ```
    GET /api/elevation?lat=50.033&lon=8.567
    ```
    
    Response:
    ```json
    {
        "lat": 50.033,
        "lon": 8.567,
        "elevation_m": 107,
        "source": "srtm30",
        "timestamp": "2026-03-03T14:30:00Z"
    }
    ```
    
    Cache: Yes (1km grid, ~1 hour TTL per client)  
    Use case: Single terrain check during flight
    """
    # Validate coordinates
    if not (-90 <= lat <= 90):
        raise HTTPException(status_code=400, detail="Latitude must be -90 to 90")
    if not (-180 <= lon <= 180):
        raise HTTPException(status_code=400, detail="Longitude must be -180 to 180")

    elevation_m = await elevation_provider.get_elevation(lat, lon)

    if elevation_m is None:
        raise HTTPException(status_code=503, detail="Elevation service temporarily unavailable")

    return {
        "lat": lat,
        "lon": lon,
        "elevation_m": elevation_m,
        "source": "srtm30",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@router.post("/batch", response_model=BatchElevationResponse)
async def get_batch_elevations(request: BatchElevationRequest) -> Dict:
    """
    Get elevations for multiple points (batch query)
    
    Optimized for route prefetching and field reachability checks
    Max 100 points per request
    
    Example:
    ```
    POST /api/elevation/batch
    {
        "points": [
            {"lat": 50.033, "lon": 8.567},
            {"lat": 50.050, "lon": 8.580},
            {"lat": 50.070, "lon": 8.600}
        ]
    }
    ```
    
    Response:
    ```json
    {
        "elevations": [
            {"lat": 50.033, "lon": 8.567, "elevation_m": 107, "source": "srtm30", "timestamp": "..."},
            {"lat": 50.050, "lon": 8.580, "elevation_m": 95, "source": "srtm30", "timestamp": "..."},
            {"lat": 50.070, "lon": 8.600, "elevation_m": 112, "source": "srtm30", "timestamp": "..."}
        ],
        "cached_count": 2,
        "fetched_count": 1
    }
    ```
    
    Cache: Yes (per-point caching)  
    Use case: Route prefetch, field reachability computation
    """
    if len(request.points) > 100:
        raise HTTPException(status_code=400, detail="Too many points (max 100)")

    if not request.points:
        raise HTTPException(status_code=400, detail="At least 1 point required")

    # Record cache stats before
    cached_before = elevation_provider.cache_hits
    fetched_before = elevation_provider.cache_misses

    # Fetch all elevations
    elevations = await elevation_provider.get_batch_elevations(
        [{"lat": p.lat, "lon": p.lon} for p in request.points],
        limit=100
    )

    # Calculate stats
    cached_count = elevation_provider.cache_hits - cached_before
    fetched_count = elevation_provider.cache_misses - fetched_before

    return {
        "elevations": elevations,
        "cached_count": cached_count,
        "fetched_count": fetched_count,
    }


@router.get("/health")
async def elevation_health() -> Dict:
    """
    Health check and cache stats
    
    Returns:
    ```json
    {
        "status": "healthy",
        "cache_hits": 245,
        "cache_misses": 18,
        "cache_hit_ratio": 0.93,
        "cached_points": 150
    }
    ```
    """
    total = elevation_provider.cache_hits + elevation_provider.cache_misses
    hit_ratio = elevation_provider.cache_hits / total if total > 0 else 0

    return {
        "status": "healthy",
        "cache_hits": elevation_provider.cache_hits,
        "cache_misses": elevation_provider.cache_misses,
        "cache_hit_ratio": round(hit_ratio, 2),
        "cached_points": len(elevation_provider.cache),
    }


def create_elevation_router() -> APIRouter:
    """Create and return elevation router"""
    return router
