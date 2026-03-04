"""
services/cache_manager.py
Redis-based caching interface for API responses and computed data
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import hashlib

try:
    import redis
except ImportError:
    redis = None

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Unified caching interface using Redis
    Implements TTL for different data types
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """
        Initialize Redis connection
        
        Args:
            redis_url: Redis connection URL
        """
        if redis is None:
            logger.warning("redis-py not installed, caching disabled")
            self.enabled = False
            self.client = None
            return
        
        try:
            # Parse URL like redis://localhost:6379/0
            self.redis_url = redis_url
            self.client = redis.from_url(redis_url, decode_responses=True)
            self.client.ping()
            self.enabled = True
            logger.info(f"Redis cache initialized: {redis_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.enabled = False
            self.client = None
    
    def _make_key(self, prefix: str, params: Dict[str, Any]) -> str:
        """
        Generate cache key from prefix + params
        
        Args:
            prefix: Cache key prefix (e.g., 'metar', 'sounding')
            params: Parameters dict
            
        Returns:
            Hashed cache key
        """
        params_str = json.dumps(params, sort_keys=True)
        params_hash = hashlib.sha256(params_str.encode()).hexdigest()[:16]
        return f"{prefix}:{params_hash}"
    
    # ════════════════════════════════════════════════════════════════
    # METAR / TAF Caching
    # ════════════════════════════════════════════════════════════════
    
    def get_metar(self, icao: str) -> Optional[Dict]:
        """Get METAR for station"""
        if not self.enabled:
            return None
        
        try:
            key = f"metar:{icao}"
            data = self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Cache GET error for {icao}: {e}")
            return None
    
    def set_metar(self, icao: str, data: Dict, ttl: int = 1800):
        """Set METAR cache (TTL 30 min default)"""
        if not self.enabled:
            return
        
        try:
            key = f"metar:{icao}"
            self.client.setex(key, ttl, json.dumps(data))
        except Exception as e:
            logger.error(f"Cache SET error for {icao}: {e}")
    
    def set_metar_bundle(self, text: str, ttl: int = 1800):
        """Cache entire METAR bundle (raw text)"""
        if not self.enabled:
            return
        
        try:
            key = "metar:bundle"
            self.client.setex(key, ttl, text)
        except Exception as e:
            logger.error(f"Cache SET error for metar:bundle: {e}")
    
    def get_taf(self, icao: str) -> Optional[Dict]:
        """Get TAF for station"""
        if not self.enabled:
            return None
        
        try:
            key = f"taf:{icao}"
            data = self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Cache GET error for TAF {icao}: {e}")
            return None
    
    def set_taf(self, icao: str, data: Dict, ttl: int = 3600):
        """Set TAF cache (TTL 1 hour default)"""
        if not self.enabled:
            return
        
        try:
            key = f"taf:{icao}"
            self.client.setex(key, ttl, json.dumps(data))
        except Exception as e:
            logger.error(f"Cache SET error for TAF {icao}: {e}")
    
    # ════════════════════════════════════════════════════════════════
    # Sounding / Indices Caching
    # ════════════════════════════════════════════════════════════════
    
    def get_sounding(self, lat: float, lon: float, source: str = 'icon-eu') -> Optional[Dict]:
        """
        Get cached sounding profile
        
        Args:
            lat: Latitude (rounded to 0.1°)
            lon: Longitude (rounded to 0.1°)
            source: 'icon-eu', 'noaa', 'gfs'
            
        Returns:
            Sounding dict or None
        """
        if not self.enabled:
            return None
        
        try:
            # Round to 0.1° grid
            lat_grid = round(lat * 10) / 10
            lon_grid = round(lon * 10) / 10
            
            key = f"sounding:{source}:{lat_grid:.1f}:{lon_grid:.1f}"
            data = self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Cache GET error for sounding: {e}")
            return None
    
    def set_sounding(self, lat: float, lon: float, data: Dict, 
                    source: str = 'icon-eu', ttl: int = 3600):
        """
        Set sounding profile cache (TTL 1 hour default)
        
        Args:
            lat, lon: Location
            data: Sounding dict with levels_json, indices
            source: Data source
            ttl: Time to live in seconds
        """
        if not self.enabled:
            return
        
        try:
            lat_grid = round(lat * 10) / 10
            lon_grid = round(lon * 10) / 10
            
            key = f"sounding:{source}:{lat_grid:.1f}:{lon_grid:.1f}"
            self.client.setex(key, ttl, json.dumps(data))
        except Exception as e:
            logger.error(f"Cache SET error for sounding: {e}")
    
    def get_indices(self, lat: float, lon: float, timestamp: Optional[datetime] = None) -> Optional[Dict]:
        """Get cached stability indices"""
        if not self.enabled:
            return None
        
        try:
            lat_grid = round(lat * 10) / 10
            lon_grid = round(lon * 10) / 10
            
            time_str = timestamp.isoformat() if timestamp else 'latest'
            key = f"indices:{lat_grid:.1f}:{lon_grid:.1f}:{time_str}"
            
            data = self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Cache GET error for indices: {e}")
            return None
    
    def set_indices(self, lat: float, lon: float, data: Dict, 
                    timestamp: Optional[datetime] = None, ttl: int = 3600):
        """Set stability indices cache"""
        if not self.enabled:
            return
        
        try:
            lat_grid = round(lat * 10) / 10
            lon_grid = round(lon * 10) / 10
            
            time_str = timestamp.isoformat() if timestamp else 'latest'
            key = f"indices:{lat_grid:.1f}:{lon_grid:.1f}:{time_str}"
            
            self.client.setex(key, ttl, json.dumps(data))
        except Exception as e:
            logger.error(f"Cache SET error for indices: {e}")
    
    # ════════════════════════════════════════════════════════════════
    # Tile Caching
    # ════════════════════════════════════════════════════════════════
    
    def get_tile_metadata(self, overlay: str, forecast_hour: int, z: int, x: int, y: int) -> Optional[Dict]:
        """
        Get tile caching metadata (whether tile exists, timestamp)
        
        Args:
            overlay: thermal, cloudbase, wind, etc.
            z, x, y: Tile coordinates
            
        Returns:
            {'exists': bool, 'path': str, 'timestamp': iso_time}
        """
        if not self.enabled:
            return None
        
        try:
            key = f"tile:{overlay}:{forecast_hour}:{z}:{x}:{y}"
            data = self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Cache GET error for tile metadata: {e}")
            return None
    
    def set_tile_metadata(self, overlay: str, forecast_hour: int, z: int, x: int, y: int,
                         path: str, ttl: int = 3600):
        """Cache tile file path and generation time"""
        if not self.enabled:
            return
        
        try:
            key = f"tile:{overlay}:{forecast_hour}:{z}:{x}:{y}"
            data = {
                'path': path,
                'timestamp': datetime.utcnow().isoformat(),
                'overlay': overlay,
                'z': z, 'x': x, 'y': y
            }
            self.client.setex(key, ttl, json.dumps(data))
        except Exception as e:
            logger.error(f"Cache SET error for tile metadata: {e}")
    
    # ════════════════════════════════════════════════════════════════
    # Traffic (Live Aircraft) Caching
    # ════════════════════════════════════════════════════════════════
    
    def get_aircraft(self, callsign: str) -> Optional[Dict]:
        """
        Get aircraft position from live traffic cache
        
        Returns:
            {'lat': float, 'lon': float, 'alt_m': int, 'speed_ms': float, 'track_deg': float, 'timestamp': iso_time}
        """
        if not self.enabled:
            return None
        
        try:
            key = f"traffic:{callsign}"
            data = self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Cache GET error for aircraft {callsign}: {e}")
            return None
    
    def set_aircraft(self, callsign: str, position: Dict, ttl: int = 300):
        """
        Set aircraft position (TTL 5 minutes default)
        
        Args:
            callsign: Aircraft registration
            position: {'lat': float, 'lon': float, 'alt_m': int, 'speed_ms': float, 'track_deg': float}
            ttl: Time to live
        """
        if not self.enabled:
            return
        
        try:
            key = f"traffic:{callsign}"
            position['timestamp'] = datetime.utcnow().isoformat()
            self.client.setex(key, ttl, json.dumps(position))
        except Exception as e:
            logger.error(f"Cache SET error for aircraft {callsign}: {e}")
    
    def get_all_aircraft(self) -> list:
        """Get all aircraft in cache"""
        if not self.enabled:
            return []
        
        try:
            keys = self.client.keys("traffic:*")
            aircraft = []
            for key in keys:
                data = self.client.get(key)
                if data:
                    aircraft.append(json.loads(data))
            return aircraft
        except Exception as e:
            logger.error(f"Cache GET error for all aircraft: {e}")
            return []
    
    # ════════════════════════════════════════════════════════════════
    # General purpose caching
    # ════════════════════════════════════════════════════════════════
    
    def get(self, key: str) -> Optional[str]:
        """Get raw value from cache"""
        if not self.enabled:
            return None
        
        try:
            return self.client.get(key)
        except Exception as e:
            logger.error(f"Cache GET error for {key}: {e}")
            return None
    
    def set(self, key: str, value: str, ttl: int = 3600):
        """Set raw value in cache"""
        if not self.enabled:
            return
        
        try:
            self.client.setex(key, ttl, value)
        except Exception as e:
            logger.error(f"Cache SET error for {key}: {e}")
    
    def delete(self, key: str):
        """Delete cache entry"""
        if not self.enabled:
            return
        
        try:
            self.client.delete(key)
        except Exception as e:
            logger.error(f"Cache DELETE error for {key}: {e}")
    
    def flush(self):
        """Clear entire cache (DANGER: use with caution)"""
        if not self.enabled:
            return
        
        try:
            self.client.flushdb()
            logger.warning("Cache flushed (all entries deleted)")
        except Exception as e:
            logger.error(f"Cache FLUSH error: {e}")
