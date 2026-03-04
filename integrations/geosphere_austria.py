"""
GeoSphere Austria / ZAMG Integration Module

Provides:
1. Station observations (10-minute update frequency)
2. Gridded INCA forecast model (1km resolution, hourly)

NO authentication required - completely open CC BY 4.0 license.

Priority Region: 46°N–49.5°N, 9°E–18°E (Austria, South Bavaria, South Tyrol)
Update Frequency: Every 1 hour
"""

import logging
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
import httpx

logger = logging.getLogger(__name__)


class GeoSphereAustriaClient:
    """
    Fetches station observations and gridded forecasts from GeoSphere Austria API.
    No authentication required.
    """
    
    BASE_URL = "https://dataset.api.hub.geosphere.at/v1"
    
    # Coverage region for Austria
    PRIORITY_REGION = {
        "lat_min": 46.0,
        "lat_max": 49.5,
        "lon_min": 9.0,
        "lon_max": 18.0
    }
    
    RESOLUTION_KM = 1.0
    UPDATE_FREQUENCY_MIN = 60
    
    def __init__(self):
        self.session = None
    
    async def fetch_station_observations(self) -> List[Dict]:
        """
        Fetch current observations from all Austrian TAWES stations.
        
        Returns:
            List of station observations with:
            - Station ID, name, coordinates
            - Temperature, dewpoint, wind, pressure, precip, humidity
        """
        url = f"{self.BASE_URL}/station/current/tawes-v1-10min"
        
        params = {
            "parameters": "TL,TD,FF,DD,P0,RR,SO,RF,TP",  # temp, dewpoint, wind, pressure, precip, sun, humidity
            "bbox": f"{self.PRIORITY_REGION['lat_min']},{self.PRIORITY_REGION['lon_min']},{self.PRIORITY_REGION['lat_max']},{self.PRIORITY_REGION['lon_max']}"
        }
        
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                logger.info(f"Fetched {len(data.get('features', []))} Austrian station observations")
                return self._parse_station_data(data)
            
            except httpx.HTTPError as e:
                logger.error(f"GeoSphere station fetch failed: {e}")
                return []
    
    async def fetch_inca_forecast(
        self,
        forecast_hours: int = 48
    ) -> Optional[Dict]:
        """
        Fetch INCA gridded forecast (1km Austria, up to 48h ahead).
        
        Args:
            forecast_hours: Number of hours to fetch (max 48)
        
        Returns:
            Dictionary with gridded forecast data
        """
        url = f"{self.BASE_URL}/grid/forecast/inca-v1-1h-1km"
        
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(hours=forecast_hours)
        
        params = {
            "parameters": "T2M,TD2M,UU,VV,RR,RH,GL",  # temp 2m, dewpoint, wind components, precip, humidity, solar radiation
            "bbox": f"{self.PRIORITY_REGION['lat_min']},{self.PRIORITY_REGION['lon_min']},{self.PRIORITY_REGION['lat_max']},{self.PRIORITY_REGION['lon_max']}",
            "start": start_time.isoformat() + "Z",
            "end": end_time.isoformat() + "Z"
        }
        
        async with httpx.AsyncClient(timeout=60) as client:
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                # Response is NetCDF - parse with xarray
                data = await self._parse_inca_netcdf(response.content)
                
                logger.info(f"Fetched INCA forecast for {forecast_hours} hours")
                return data
            
            except httpx.HTTPError as e:
                logger.error(f"GeoSphere INCA fetch failed: {e}")
                return None
    
    def _parse_station_data(self, geojson_data: Dict) -> List[Dict]:
        """
        Parse GeoJSON station observations.
        """
        stations = []
        
        for feature in geojson_data.get("features", []):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [None, None])
            
            station = {
                "id": props.get("id"),
                "name": props.get("name"),
                "lon": coords[0],
                "lat": coords[1],
                "elevation_m": props.get("elev"),
                "timestamp": props.get("timestamp"),
                
                # Temperature and humidity  
                "temperature_c": props.get("TL"),
                "dewpoint_c": props.get("TD"),
                "relative_humidity_pct": props.get("RF"),
                
                # Wind
                "wind_speed_ms": props.get("FF"),
                "wind_direction_deg": props.get("DD"),
                
                # Pressure and precipitation
                "pressure_hpa": props.get("P0"),
                "precipitation_mm": props.get("RR"),
                "sunshine_hours": props.get("SO"),
                "temp_extreme": props.get("TP")
            }
            stations.append(station)
        
        return stations
    
    async def _parse_inca_netcdf(self, netcdf_data: bytes) -> Dict:
        """
        Parse NetCDF INCA forecast data with xarray.
        """
        try:
            import xarray as xr
            import tempfile
            
            # Write to temp file
            with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as f:
                f.write(netcdf_data)
                temp_path = f.name
            
            # Open with xarray
            try:
                ds = xr.open_dataset(temp_path)
                
                # Extract key variables  
                forecast = {
                    "model": "geosphere-austria-inca",
                    "resolution_km": self.RESOLUTION_KM,
                    "variables": {
                        "temperature_2m": ds['T2M'].values.tolist() if 'T2M' in ds else None,
                        "dewpoint_2m": ds['TD2M'].values.tolist() if 'TD2M' in ds else None,
                        "u_wind": ds['UU'].values.tolist() if 'UU' in ds else None,
                        "v_wind": ds['VV'].values.tolist() if 'VV' in ds else None,
                        "precipitation": ds['RR'].values.tolist() if 'RR' in ds else None,
                        "relative_humidity": ds['RH'].values.tolist() if 'RH' in ds else None,
                        "solar_radiation": ds['GL'].values.tolist() if 'GL' in ds else None
                    },
                    "coordinates": {
                        "lat": ds['latitude'].values.tolist() if 'latitude' in ds else None,
                        "lon": ds['longitude'].values.tolist() if 'longitude' in ds else None,
                        "time": ds['time'].values.tolist() if 'time' in ds else None
                    }
                }
                
                import os
                os.unlink(temp_path)
                return forecast
            
            except Exception as e:
                logger.error(f"NetCDF parsing failed: {e}")
                import os
                os.unlink(temp_path)
                return None
        
        except ImportError:
            logger.warning("xarray not installed, returning raw NetCDF reference")
            return {"raw_netcdf": True, "size_bytes": len(netcdf_data)}
    
    def is_in_priority_region(self, lat: float, lon: float) -> bool:
        """Check if point is in Austria INCA priority region."""
        return (
            self.PRIORITY_REGION["lat_min"] <= lat <= self.PRIORITY_REGION["lat_max"]
            and self.PRIORITY_REGION["lon_min"] <= lon <= self.PRIORITY_REGION["lon_max"]
        )


async def get_geosphere_austria_forecast(
    lat: float,
    lon: float
) -> Optional[Dict]:
    """
    Public function to get GeoSphere Austria INCA forecast.
    
    Used by:
    - Celery task: ingest_geosphere_austria
    - API endpoint: /api/forecast/geosphere-austria/{lat}/{lon}
    """
    client = GeoSphereAustriaClient()
    
    if not client.is_in_priority_region(lat, lon):
        logger.debug(f"Point ({lat}, {lon}) outside Austria INCA priority region")
        return None
    
    try:
        forecast = await client.fetch_inca_forecast()
        return forecast
    except Exception as e:
        logger.error(f"GeoSphere Austria fetch failed: {e}")
        return None


async def get_geosphere_austria_observations() -> List[Dict]:
    """
    Public function to get current Austrian station observations.
    
    Used by:
    - Celery task: ingest_geosphere_austria_stations
    """
    client = GeoSphereAustriaClient()
    
    try:
        observations = await client.fetch_station_observations()
        return observations
    except Exception as e:
        logger.error(f"GeoSphere Austria stations fetch failed: {e}")
        return []


def get_geosphere_austria_model_info() -> Dict:
    """Returns model metadata for /api/forecast/models/info endpoint."""
    return {
        "id": "geosphere-austria-inca",
        "name": "GeoSphere Austria INCA",
        "resolution_km": GeoSphereAustriaClient.RESOLUTION_KM,
        "update_frequency_hours": 1,
        "priority": 11,
        "coverage_lat": [46.0, 49.5],
        "coverage_lon": [9.0, 18.0],
        "max_forecast_hours": 48,
        "station_data": True
    }
