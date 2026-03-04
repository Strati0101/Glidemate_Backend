"""
Météo-France AROME Integration Module

High-resolution (1.3km) model for France and border regions.
Handles JWT token management with automatic renewal (60-minute expiry).
Fetches GRIB2 data via WCS API.

Priority Region: 43°N–52°N, 5°W–10°E (France, Alsace, Lorraine, Swiss border)
Update Frequency: Every 1 hour
"""

import os
import time
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
import base64
import httpx
from functools import lru_cache

logger = logging.getLogger(__name__)


class MeteoFranceTokenManager:
    """
    Manages JWT token lifecycle for Météo-France API.
    Tokens expire every 60 minutes.
    Redis key: 'meteofrance:token' stores {"token": "...", "timestamp": ...}
    """
    
    TOKEN_ENDPOINT = "https://portail-api.meteofrance.fr/token"
    TOKEN_LIFETIME = 3600  # 60 minutes
    RENEWAL_BUFFER = 300  # Renew 5 minutes before expiry
    REDIS_KEY = "meteofrance:token"
    
    def __init__(self, redis_client=None, client_credentials: Optional[str] = None):
        self.redis_client = redis_client
        self.client_credentials = client_credentials or os.getenv("METEOFRANCE_CLIENT_CREDENTIALS", "")
        self.current_token = None
        self.token_timestamp = 0
        
    async def get_valid_token(self) -> str:
        """
        Returns a valid token, renewing if necessary.
        Checks Redis first, then in-memory cache, then API.
        """
        # Try Redis first
        if self.redis_client:
            try:
                cached = await self._get_from_redis()
                if cached and self._is_token_valid(cached["timestamp"]):
                    self.current_token = cached["token"]
                    self.token_timestamp = cached["timestamp"]
                    logger.debug("Using cached Météo-France token from Redis")
                    return cached["token"]
            except Exception as e:
                logger.warning(f"Redis token retrieval failed: {e}")
        
        # Try in-memory cache
        if self.current_token and self._is_token_valid(self.token_timestamp):
            logger.debug("Using in-memory cached Météo-France token")
            return self.current_token
        
        # Renew from API
        logger.info("Renewing Météo-France JWT token from API")
        token = await self._renew_token()
        
        # Store in Redis
        if self.redis_client:
            try:
                await self._store_to_redis(token, time.time())
            except Exception as e:
                logger.warning(f"Redis token storage failed: {e}")
        
        # Store in memory
        self.current_token = token
        self.token_timestamp = time.time()
        return token
    
    async def _renew_token(self) -> str:
        """
        POST to Météo-France token endpoint with client credentials.
        """
        if not self.client_credentials:
            raise ValueError("METEOFRANCE_CLIENT_CREDENTIALS not configured in .env")
        
        # Create Basic auth header from credentials
        auth_str = f"{self.client_credentials}"
        auth_bytes = auth_str.encode('utf-8')
        auth_b64 = base64.b64encode(auth_bytes).decode('utf-8')
        
        headers = {
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        data = {"grant_type": "client_credentials"}
        
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                response = await client.post(
                    self.TOKEN_ENDPOINT,
                    headers=headers,
                    data=data
                )
                response.raise_for_status()
                token_data = response.json()
                
                if "access_token" not in token_data:
                    raise ValueError(f"No access_token in response: {token_data}")
                
                logger.info(f"Météo-France token renewed (valid for {token_data.get('expires_in', 3600)}s)")
                return token_data["access_token"]
            except httpx.HTTPError as e:
                logger.error(f"Météo-France token renewal failed: {e}")
                raise
    
    def _is_token_valid(self, timestamp: float) -> bool:
        """Check if token is still valid (with safety buffer)."""
        age = time.time() - timestamp
        return age < (self.TOKEN_LIFETIME - self.RENEWAL_BUFFER)
    
    async def _get_from_redis(self) -> Optional[Dict]:
        """Retrieve token from Redis."""
        if not self.redis_client:
            return None
        data_str = await self.redis_client.get(self.REDIS_KEY)
        if data_str:
            return json.loads(data_str)
        return None
    
    async def _store_to_redis(self, token: str, timestamp: float):
        """Store token in Redis with TTL."""
        if not self.redis_client:
            return
        data = {"token": token, "timestamp": timestamp}
        await self.redis_client.setex(
            self.REDIS_KEY,
            self.TOKEN_LIFETIME,
            json.dumps(data)
        )


class MeteoFranceAROMEClient:
    """
    Fetches GRIB2 data from Météo-France AROME via WCS API.
    """
    
    BASE_URL = "https://public-api.meteofrance.fr/public/arome/1.0/wcs/MF-NWP-HIGHRES-AROME-001-FRANCE-WCS/GetCoverage"
    PRIORITY_REGION = {
        "lat_min": 43.0,
        "lat_max": 52.0,
        "lon_min": -5.0,
        "lon_max": 10.0
    }
    RESOLUTION_KM = 1.3
    UPDATE_FREQUENCY_MIN = 60
    
    # WCS coverage ID patterns
    VARIABLES = {
        "temperature_2m": "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
        "dewpoint_2m": "DEW_POINT_TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
        "u_wind_10m": "U_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
        "v_wind_10m": "V_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
        "temperature_850": "TEMPERATURE__ISOBARIC_SURFACE",
        "u_wind_850": "U_COMPONENT_OF_WIND__ISOBARIC_SURFACE",
        "v_wind_850": "V_COMPONENT_OF_WIND__ISOBARIC_SURFACE",
        "temperature_700": "TEMPERATURE__ISOBARIC_SURFACE",
        "u_wind_700": "U_COMPONENT_OF_WIND__ISOBARIC_SURFACE",
        "v_wind_700": "V_COMPONENT_OF_WIND__ISOBARIC_SURFACE",
        "temperature_500": "TEMPERATURE__ISOBARIC_SURFACE",
        "u_wind_500": "U_COMPONENT_OF_WIND__ISOBARIC_SURFACE",
        "v_wind_500": "V_COMPONENT_OF_WIND__ISOBARIC_SURFACE",
        "total_precip": "TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE",
        "cloud_cover": "TOTAL_CLOUD_COVER__ISOBARIC_SURFACE",
        "cape": "CAPE__GROUND_OR_WATER_SURFACE"
    }
    
    def __init__(self, token_manager: MeteoFranceTokenManager):
        self.token_manager = token_manager
        self.session = None
    
    async def fetch_forecast(self, lat: float, lon: float, forecast_hour: int = 0) -> Dict:
        """
        Fetch AROME forecast for a single point.
        
        Args:
            lat: Latitude
            lon: Longitude
            forecast_hour: Hours ahead (0-48)
        
        Returns:
            Dictionary with forecast data
        """
        token = await self.token_manager.get_valid_token()
        
        valid_time = datetime.utcnow() + timedelta(hours=forecast_hour)
        valid_time_str = valid_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Fetch multiple variables
        results = {}
        
        async with httpx.AsyncClient(timeout=60) as client:
            for var_key, coverage_id in self.VARIABLES.items():
                try:
                    results[var_key] = await self._fetch_variable(
                        client, token, coverage_id, lat, lon, valid_time_str, var_key
                    )
                except Exception as e:
                    logger.warning(f"Failed to fetch AROME {var_key}: {e}")
        
        return {
            "model": "meteofrance-arome",
            "lat": lat,
            "lon": lon,
            "valid_time": valid_time_str,
            "data": results,
            "resolution_km": self.RESOLUTION_KM
        }
    
    async def _fetch_variable(
        self,
        client: httpx.AsyncClient,
        token: str,
        coverage_id: str,
        lat: float,
        lon: float,
        valid_time_str: str,
        var_key: str
    ) -> Optional[bytes]:
        """
        Fetch a single WCS coverage.
        """
        # Determine height/pressure level from var_key
        height_level = None
        if "2m" in var_key:
            height_level = 2
        elif "10m" in var_key:
            height_level = 10
        
        pressure_level = None
        if "850" in var_key:
            pressure_level = 85000
        elif "700" in var_key:
            pressure_level = 70000
        elif "500" in var_key:
            pressure_level = 50000
        
        # Build WCS request URL
        params = {
            "SERVICE": "WCS",
            "VERSION": "2.0.1",
            "REQUEST": "GetCoverage",
            "format": "application/wmo-grib",
            "coverageId": coverage_id,
            "subset": f"time({valid_time_str})",
            "subset": f"lat({self.PRIORITY_REGION['lat_min']},{self.PRIORITY_REGION['lat_max']})",
            "subset": f"long({self.PRIORITY_REGION['lon_min']},{self.PRIORITY_REGION['lon_max']})"
        }
        
        if height_level:
            params["subset"] = f"height({height_level})"
        if pressure_level:
            params["subset"] = f"isobaricInhPa({pressure_level // 100})"
        
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        try:
            response = await client.get(
                self.BASE_URL,
                params=params,
                headers=headers
            )
            response.raise_for_status()
            return response.content  # GRIB2 binary data
        except httpx.HTTPError as e:
            logger.error(f"WCS GetCoverage failed for {var_key}: {e}")
            raise
    
    def is_in_priority_region(self, lat: float, lon: float) -> bool:
        """Check if point is in AROME priority region."""
        return (
            self.PRIORITY_REGION["lat_min"] <= lat <= self.PRIORITY_REGION["lat_max"]
            and self.PRIORITY_REGION["lon_min"] <= lon <= self.PRIORITY_REGION["lon_max"]
        )


async def parse_arome_grib2(grib2_data: bytes) -> Dict:
    """
    Parse GRIB2 binary data with cfgrib/xarray.
    Returns structured forecast data.
    
    Requires: cfgrib, xarray, eccodes (system library)
    """
    try:
        import cfgrib
        import xarray as xr
        import tempfile
        
        # Write to temp file
        with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as f:
            f.write(grib2_data)
            temp_path = f.name
        
        # Open with cfgrib
        try:
            ds = xr.open_dataset(temp_path, engine='cfgrib')
            # Extract variables and convert to dict
            data = {}
            for var_name in ds.variables:
                if var_name not in ['latitude', 'longitude', 'time']:
                    data[var_name] = ds[var_name].values.tolist()
            
            os.unlink(temp_path)
            return data
        except Exception as e:
            logger.error(f"GRIB2 parsing failed: {e}")
            os.unlink(temp_path)
            return {}
    
    except ImportError:
        logger.warning("cfgrib not installed, returning raw GRIB2 data reference")
        return {"raw_grib2": True, "size_bytes": len(grib2_data)}


# ============================================================================
# Integration with existing backend system
# ============================================================================

async def get_meteofrance_forecast(
    lat: float,
    lon: float,
    redis_client=None,
    forecast_hour: int = 0
) -> Optional[Dict]:
    """
    Public function to get Météo-France AROME forecast.
    
    Used by:
    - Celery task: ingest_meteofrance_arome
    - API endpoint: /api/forecast/meteofrance/{lat}/{lon}
    """
    if not MeteoFranceAROMEClient.is_in_priority_region(lat, lon):
        logger.debug(f"Point ({lat}, {lon}) outside AROME priority region")
        return None
    
    token_manager = MeteoFranceTokenManager(redis_client=redis_client)
    client = MeteoFranceAROMEClient(token_manager)
    
    try:
        forecast = await client.fetch_forecast(lat, lon, forecast_hour)
        return forecast
    except Exception as e:
        logger.error(f"Météo-France AROME fetch failed: {e}")
        return None


# Model info for API response
def get_meteofrance_model_info() -> Dict:
    """Returns model metadata for /api/forecast/models/info endpoint."""
    return {
        "id": "meteofrance-arome",
        "name": "Météo-France AROME",
        "resolution_km": MeteoFranceAROMEClient.RESOLUTION_KM,
        "update_frequency_hours": 1,
        "priority": 12,
        "coverage_lat": [43.0, 52.0],
        "coverage_lon": [-5.0, 10.0],
        "max_forecast_hours": 48
    }
