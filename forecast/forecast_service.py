"""
backend_forecast_data_service.py
Weather forecast data service with fallback logic and region-based model selection

Supports:
- KNMI HARMONIE for NL/BE/NRW region (priority region: 47°N–57°N, 2°W–16°E)
- DWD ICON-EU as fallback
- Bundled data storage for offline availability
- model_used field in all responses
"""

import logging
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from pathlib import Path
import gzip
import pickle

logger = logging.getLogger(__name__)


class ForecastDataService:
    """
    Central service for forecast data with fallback logic
    
    Priority rules:
    1. If in priority region (47°N–57°N, 2°W–16°E): Try KNMI first, fallback to DWD
    2. If outside priority region: Use DWD directly
    """
    
    # Priority region boundaries (NL/BE/NRW)
    PRIORITY_REGION = {
        'north': 57.0,
        'south': 47.0,
        'east': 16.0,
        'west': -2.0
    }
    
    def __init__(self, db_session, settings, cache_manager=None):
        """
        Initialize forecast service
        
        Args:
            db_session: SQLAlchemy database session
            settings: Configuration settings object
            cache_manager: Optional cache manager for Redis caching
        """
        self.db = db_session
        self.settings = settings
        self.cache = cache_manager
        
        # Data storage path for bundled data
        self.data_path = Path("/opt/glidemate-backend/data") if not settings.DEBUG else Path("./data")
        self.data_path.mkdir(parents=True, exist_ok=True)
        
        # Import providers
        try:
            from backend_knmi_integration import KNMIAPIClient, KNMIHarmonieProvider, KNMIInsituProvider
            self.knmi_available = True
            self.knmi_key = settings.knmi_api_key
        except Exception as e:
            logger.warning(f"KNMI integration not available: {e}")
            self.knmi_available = False
            self.knmi_key = None
    
    def is_priority_region(self, lat: float, lon: float) -> bool:
        """
        Check if location is in priority region (KNMI coverage)
        
        Args:
            lat: Latitude (-90 to 90)
            lon: Longitude (-180 to 180)
            
        Returns:
            True if in priority region
        """
        return (
            self.PRIORITY_REGION['south'] <= lat <= self.PRIORITY_REGION['north'] and
            self.PRIORITY_REGION['west'] <= lon <= self.PRIORITY_REGION['east']
        )
    
    async def get_sounding_forecast(
        self,
        lat: float,
        lon: float,
        include_bundled: bool = True
    ) -> Dict:
        """
        Get atmospheric sounding forecast with fallback logic
        
        Priority:
        1. If in priority region: Try KNMI HARMONIE, fall back to DWD ICON-EU
        2. If outside priority region: Use DWD ICON-EU
        3. Fall back to cached/bundled data if live fetch fails
        
        Args:
            lat: Latitude
            lon: Longitude
            include_bundled: Include bundled data bundle hash in response
            
        Returns:
            Dict with sounding data, indices, and model_used field
        """
        try:
            in_priority = self.is_priority_region(lat, lon)
            
            # Try primary model
            if in_priority:
                logger.info(f"📍 Priority region detected: {lat:.1f}°N, {lon:.1f}°E - Using KNMI")
                result = await self._get_sounding_from_knmi(lat, lon)
                if result and result['status'] == 'success':
                    result['model_used'] = 'KNMI-HARMONIE-AROME'
                    result['bundle_id'] = self._get_bundle_id(lat, lon, 'knmi-harmonie')
                    return result
                else:
                    logger.warning(f"KNMI fetch failed for {lat},{lon} - falling back to DWD")
            
            # Fall back to DWD
            logger.info(f"Using DWD ICON-EU for {lat:.1f}°N, {lon:.1f}°E")
            result = await self._get_sounding_from_dwd(lat, lon)
            if result and result['status'] == 'success':
                result['model_used'] = 'DWD-ICON-EU'
                result['bundle_id'] = self._get_bundle_id(lat, lon, 'dwd-icon-eu')
                return result
            
            # Fall back to cached data if live fetch failed
            logger.warning(f"Live fetch failed for {lat},{lon} - trying cache")
            cached = await self._get_cached_sounding(lat, lon)
            if cached:
                cached['status'] = 'success_cached'
                cached['data_age_minutes'] = self._calc_age_minutes(cached.get('cached_at'))
                return cached
            
            # All failed
            return {
                'status': 'error',
                'message': 'No forecast data available',
                'lat': lat,
                'lon': lon,
                'model_used': 'none'
            }
        
        except Exception as e:
            logger.error(f"Error in get_sounding_forecast: {e}", exc_info=True)
            return {
                'status': 'error',
                'message': str(e),
                'lat': lat,
                'lon': lon,
                'model_used': 'none'
            }
    
    async def _get_sounding_from_knmi(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Fetch sounding from KNMI HARMONIE
        
        Returns:
            Dict with sounding data or None if fetch fails
        """
        if not self.knmi_available or not self.knmi_key:
            return None
        
        try:
            from backend_knmi_integration import KNMIAPIClient, KNMIHarmonieProvider
            
            client = KNMIAPIClient(self.knmi_key)
            provider = KNMIHarmonieProvider(client)
            
            # Check if location is in KNMI coverage
            if not provider.is_priority_region(lat, lon):
                logger.warning(f"{lat},{lon} not in KNMI coverage area")
                return None
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Get sounding data from KNMI (needs implementation in KNMI provider)
            # For now, return None to trigger fallback
            sounding = None
            
            if sounding:
                return {
                    'status': 'success',
                    'location': {'lat': lat, 'lon': lon},
                    'valid_at': datetime.utcnow().isoformat(),
                    'source': 'knmi-harmonie',
                    'levels': sounding.get('levels', []),
                    'indices': sounding.get('indices', {}),
                    'data_age_minutes': 0
                }
            
            return None
        
        except Exception as e:
            logger.error(f"Error fetching KNMI sounding: {e}")
            return None
    
    async def _get_sounding_from_dwd(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Fetch sounding from DWD ICON-EU
        
        Returns:
            Dict with sounding data or None if fetch fails
        """
        try:
            # Query DWD GRIB data from database
            from sqlalchemy import text
            
            query = text("""
                SELECT data_json, valid_at 
                FROM dwd_icon_data
                WHERE ST_DWithin(
                    ST_Point(:lon, :lat),
                    location_geometry,
                    0.06  -- approximately 6 km buffer
                )
                AND valid_at > now() - interval '24 hours'
                ORDER BY valid_at DESC
                LIMIT 1
            """)
            
            result = self.db.execute(query, {'lat': lat, 'lon': lon}).fetchone()
            
            if result:
                data = json.loads(result[0])
                return {
                    'status': 'success',
                    'location': {'lat': lat, 'lon': lon},
                    'valid_at': result[1].isoformat(),
                    'source': 'dwd-icon-eu',
                    'levels': data.get('levels', []),
                    'indices': data.get('indices', {}),
                    'data_age_minutes': self._calc_age_minutes(result[1])
                }
            
            return None
        
        except Exception as e:
            logger.error(f"Error fetching DWD sounding: {e}")
            return None
    
    async def _get_cached_sounding(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Get cached sounding from Redis or disk
        
        Returns:
            Dict with cached sounding data
        """
        try:
            # Try Redis cache first
            if self.cache:
                cache_key = f"sounding:{lat:.2f}:{lon:.2f}"
                cached_data = await self.cache.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
            
            # Try disk cache (bundled data)
            bundle_path = self._get_bundle_path(lat, lon, 'cached')
            if bundle_path.exists():
                with gzip.open(bundle_path, 'rb') as f:
                    data = pickle.load(f)
                    data['cached_at'] = bundle_path.stat().st_mtime
                    return data
            
            return None
        
        except Exception as e:
            logger.error(f"Error getting cached sounding: {e}")
            return None
    
    def _get_bundle_id(self, lat: float, lon: float, model: str) -> str:
        """
        Generate bundle ID for data grouping/bundling
        
        Bundle ID format: MODEL-YYYYMMDD-GRID
        Example: knmi-harmonie-20260302-5051
        
        Args:
            lat: Latitude
            lon: Longitude
            model: Model name (knmi-harmonie, dwd-icon-eu, etc.)
            
        Returns:
            Bundle ID string
        """
        date_str = datetime.utcnow().strftime("%Y%m%d")
        
        # Grid cell (rounded to nearest integer degree for grouping)
        grid_lat = int(lat)
        grid_lon = int(lon) + 180  # Shift to positive
        grid_id = f"{grid_lat:02d}{grid_lon:03d}"
        
        bundle_id = f"{model}-{date_str}-{grid_id}"
        return bundle_id
    
    def _get_bundle_path(self, lat: float, lon: float, model: str) -> Path:
        """
        Get file path for bundled data storage
        
        Bundled data organized by:
        - data/bundles/MODEL/YYYY/MM/DD/lat_lon.pkl.gz
        
        Args:
            lat: Latitude
            lon: Longitude
            model: Model name
            
        Returns:
            Path object for bundle file
        """
        now = datetime.utcnow()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        
        bundle_dir = self.data_path / "bundles" / model / year / month / day
        bundle_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"{lat:07.2f}_{lon:07.2f}.pkl.gz"
        return bundle_dir / filename
    
    def _calc_age_minutes(self, original_time) -> int:
        """Calculate age of data in minutes"""
        try:
            if isinstance(original_time, str):
                original_dt = datetime.fromisoformat(original_time.replace('Z', '+00:00'))
            else:
                original_dt = original_time
            
            age = datetime.utcnow() - original_dt
            return int(age.total_seconds() / 60)
        except:
            return 9999
    
    async def get_thermal_forecast(
        self,
        lat: float,
        lon: float,
        height_m: int = 0
    ) -> Dict:
        """
        Get thermal strength prediction at specific location
        
        Args:
            lat: Latitude
            lon: Longitude
            height_m: Height above ground (default 0m = surface)
            
        Returns:
            Dict with thermal strength, model_used, bundle_id
        """
        try:
            sounding = await self.get_sounding_forecast(lat, lon)
            
            if sounding['status'] != 'success':
                return {
                    'status': 'error',
                    'message': 'Could not fetch sounding for thermal calculation',
                    'model_used': sounding.get('model_used', 'none')
                }
            
            # Calculate thermal strength from CAPE
            indices = sounding.get('indices', {})
            cape = indices.get('cape', 0)
            
            # Simple thermal strength mapping
            if cape > 2000:
                thermal_strength = 5  # Very strong
            elif cape > 1500:
                thermal_strength = 4  # Strong
            elif cape > 1000:
                thermal_strength = 3  # Moderate
            elif cape > 500:
                thermal_strength = 2  # Weak
            else:
                thermal_strength = 1  # Very weak
            
            return {
                'status': 'success',
                'location': {'lat': lat, 'lon': lon},
                'thermal_strength': thermal_strength,
                'cape': cape,
                'model_used': sounding['model_used'],
                'bundle_id': sounding.get('bundle_id'),
                'valid_at': sounding['valid_at']
            }
        
        except Exception as e:
            logger.error(f"Error calculating thermal forecast: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'model_used': 'none'
            }
    
    async def get_wind_profile(
        self,
        lat: float,
        lon: float
    ) -> Dict:
        """
        Get wind profile at location (speed and direction vs height)
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dict with wind layers, model_used, bundle_id
        """
        try:
            sounding = await self.get_sounding_forecast(lat, lon)
            
            if sounding['status'] != 'success':
                return {
                    'status': 'error',
                    'message': 'Could not fetch sounding for wind profile',
                    'model_used': sounding.get('model_used', 'none')
                }
            
            # Extract wind layers from sounding
            levels = sounding.get('levels', [])
            wind_layers = [
                {
                    'altitude_m': level.get('height_m', 0),
                    'wind_speed_ms': level.get('wind_speed_ms', 0),
                    'wind_direction': level.get('wind_direction_deg', 0)
                }
                for level in levels if 'wind_speed_ms' in level
            ]
            
            return {
                'status': 'success',
                'location': {'lat': lat, 'lon': lon},
                'wind_layers': wind_layers,
                'model_used': sounding['model_used'],
                'bundle_id': sounding.get('bundle_id'),
                'valid_at': sounding['valid_at']
            }
        
        except Exception as e:
            logger.error(f"Error calculating wind profile: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'model_used': 'none'
            }
    
    async def get_weather_summary(
        self,
        lat: float,
        lon: float
    ) -> Dict:
        """
        Get weather summary combining all forecast components
        
        Returns:
            Dict with current conditions, forecast, model_used, bundle_id
        """
        try:
            # Fetch soundings from all available sources
            sounding = await self.get_sounding_forecast(lat, lon)
            thermal = await self.get_thermal_forecast(lat, lon)
            wind = await self.get_wind_profile(lat, lon)
            
            # Build comprehensive summary
            summary = {
                'status': 'success',
                'location': {'lat': lat, 'lon': lon},
                'valid_at': datetime.utcnow().isoformat(),
                'model_used': sounding.get('model_used', 'dwd-icon-eu'),
                'bundle_id': sounding.get('bundle_id'),
                'components': {
                    'sounding': sounding if sounding['status'] == 'success' else None,
                    'thermal': thermal if thermal['status'] == 'success' else None,
                    'wind': wind if wind['status'] == 'success' else None
                },
                'data_freshness': {
                    'is_live': sounding.get('status') == 'success',
                    'age_minutes': sounding.get('data_age_minutes', 0)
                }
            }
            
            return summary
        
        except Exception as e:
            logger.error(f"Error in get_weather_summary: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'model_used': 'none'
            }
    
    def bundle_data_for_offline(
        self,
        lat: float,
        lon: float,
        model: str,
        data: Dict
    ) -> str:
        """
        Bundle forecast data for offline storage
        
        Args:
            lat: Latitude
            lon: Longitude
            model: Model name (knmi-harmonie, dwd-icon-eu)
            data: Forecast data dict
            
        Returns:
            Bundle file path
        """
        try:
            bundle_path = self._get_bundle_path(lat, lon, model)
            
            # Add metadata
            data['bundled_at'] = datetime.utcnow().isoformat()
            data['bundle_id'] = self._get_bundle_id(lat, lon, model)
            data['location'] = {'lat': lat, 'lon': lon}
            
            # Compress and save
            with gzip.open(bundle_path, 'wb') as f:
                pickle.dump(data, f)
            
            logger.info(f"Bundled {model} data for {lat},{lon} → {bundle_path}")
            return str(bundle_path)
        
        except Exception as e:
            logger.error(f"Error bundling data: {e}")
            return ""


# ════════════════════════════════════════════════════════════════
# Database models for forecast storage
# ════════════════════════════════════════════════════════════════

class DWDIconData:
    """
    DWD ICON-EU forecast data storage
    
    Stores GRIB2-derived JSON with location geometry for spatial queries
    """
    __tablename__ = 'dwd_icon_data'
    
    # id, location_geometry, valid_at, data_json, created_at, bundle_id
    # Indexed on: (valid_at, location_geometry)


class KNMIHarmonieData:
    """
    KNMI HARMONIE-AROME forecast data storage
    
    Stores GRIB2-derived JSON for priority region with spatial indexing
    """
    __tablename__ = 'knmi_harmonie_data'
    
    # id, location_geometry, valid_at, data_json, created_at, bundle_id
    # Indexed on: (valid_at, location_geometry)


class ForecastDataBundle:
    """
    Bundled forecast data for offline availability
    
    Groups multiple locations and models for efficient distribution
    """
    __tablename__ = 'forecast_data_bundles'
    
    # id, bundle_id, model, date, data_blob (compressed), size_bytes, created_at
    # Indexed on: (bundle_id, model, date)
