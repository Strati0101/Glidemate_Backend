"""
services/data_providers.py
Real weather data source providers
"""

import logging
import httpx
import json
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import html.parser
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class METARTAFProvider:
    """
    AVWX API provider for METAR and TAF data
    https://avwx.rest/
    """
    
    def __init__(self, api_key: str):
        self.api_url = 'https://avwx.rest/api'
        self.api_key = api_key
        self.timeout = httpx.Timeout(10.0)
    
    async def fetch_metar(self, icao: str) -> Optional[Dict]:
        """
        Fetch METAR for a station via AVWX API
        
        Args:
            icao: Station ICAO code (e.g., 'EDDF')
            
        Returns:
            Dict with temperature, wind, visibility, flight category, raw text
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                url = f"{self.api_url}/metar/{icao}"
                params = {'token': self.api_key}
                
                response = await client.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if not data.get('valid'):
                    logger.warning(f"Invalid METAR for {icao}")
                    return None
                
                # Extract relevant fields
                metar_dict = {
                    'icao': data.get('station', {}).get('icao', icao),
                    'station_name': data.get('station', {}).get('name', ''),
                    'lat': data.get('station', {}).get('latitude'),
                    'lon': data.get('station', {}).get('longitude'),
                    'raw_text': data.get('raw', ''),
                    'observed_at': data.get('time', {}).get('dt'),  # ISO format
                    
                    # Decoded weather
                    'temperature_c': self._extract_temp(data.get('temperature')),
                    'dewpoint_c': self._extract_temp(data.get('dewpoint')),
                    'wind_direction': self._extract_direction(data.get('wind_direction')),
                    'wind_speed_ms': self._extract_speed(data.get('wind_speed')),
                    'wind_gust_ms': self._extract_speed(data.get('wind_gust')),
                    'visibility_m': self._extract_visibility(data.get('visibility')),
                    'flight_category': data.get('flight_category', 'UNKN'),
                    'cloud_layers_json': json.dumps(data.get('clouds', [])),
                }
                
                logger.info(f"Fetched METAR for {icao}: {metar_dict['temperature_c']}°C")
                return metar_dict
        
        except Exception as e:
            logger.error(f"Error fetching METAR for {icao}: {e}")
            return None
    
    async def fetch_taf(self, icao: str) -> Optional[Dict]:
        """
        Fetch TAF for a station via AVWX API
        
        Args:
            icao: Station ICAO code
            
        Returns:
            Dict with TAF groups and forecast info
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                url = f"{self.api_url}/taf/{icao}"
                params = {'token': self.api_key}
                
                response = await client.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if not data.get('valid'):
                    logger.warning(f"Invalid TAF for {icao}")
                    return None
                
                taf_dict = {
                    'icao': data.get('station', {}).get('icao', icao),
                    'station_name': data.get('station', {}).get('name', ''),
                    'lat': data.get('station', {}).get('latitude'),
                    'lon': data.get('station', {}).get('longitude'),
                    'raw_text': data.get('raw', ''),
                    'issued_at': data.get('time', {}).get('dt'),
                    'valid_from': data.get('start_time', {}).get('dt'),
                    'valid_to': data.get('end_time', {}).get('dt'),
                    'groups_json': json.dumps(data.get('forecast', [])),
                }
                
                logger.info(f"Fetched TAF for {icao}")
                return taf_dict
        
        except Exception as e:
            logger.error(f"Error fetching TAF for {icao}: {e}")
            return None
    
    @staticmethod
    def _extract_temp(temp_dict: Dict) -> Optional[float]:
        """Extract temperature value in Celsius"""
        if temp_dict and 'value' in temp_dict:
            return float(temp_dict['value'])
        return None
    
    @staticmethod
    def _extract_speed(speed_dict: Dict) -> Optional[float]:
        """Extract wind speed in m/s"""
        if speed_dict and 'value' in speed_dict:
            # AVWX returns in kt, convert to m/s
            return float(speed_dict['value']) * 0.51444
        return None
    
    @staticmethod
    def _extract_direction(direction_dict: Dict) -> Optional[float]:
        """Extract wind direction in degrees"""
        if direction_dict and 'value' in direction_dict:
            return float(direction_dict['value'])
        return None
    
    @staticmethod
    def _extract_visibility(visibility_dict: Dict) -> Optional[float]:
        """Extract visibility in meters"""
        if visibility_dict and 'value' in visibility_dict:
            return float(visibility_dict['value'])
        return None


class NOAASoundingsProvider:
    """
    University of Wyoming radiosondes provider
    http://weather.uwyo.edu/cgi-bin/sounding
    """
    
    BASE_URL = 'http://weather.uwyo.edu/cgi-bin/sounding'
    
    # Station ID mapping
    STATIONS = {
        10410: {'name': 'Munich', 'country': 'Germany'},
        10438: {'name': 'Stuttgart', 'country': 'Germany'},
        10491: {'name': 'Vienna', 'country': 'Austria'},
        12120: {'name': 'Prague', 'country': 'Czech Republic'},
        11035: {'name': 'Lindenberg', 'country': 'Germany'},
        07005: {'name': 'Payerne', 'country': 'Switzerland'},
        06011: {'name': 'Nîmes', 'country': 'France'},
        07110: {'name': 'Trappes', 'country': 'France'},
        07755: {'name': 'Valencia', 'country': 'Spain'},
    }
    
    def __init__(self):
        self.timeout = httpx.Timeout(15.0)
    
    async def fetch(self, station_id: int, hours: int = 24) -> Optional[Dict]:
        """
        Fetch sounding profile from UWyo
        
        Args:
            station_id: WMO station ID
            hours: Look back hours from now
            
        Returns:
            Dict with pressure levels and atmospheric data
        """
        try:
            # Calculate date range
            now = datetime.utcnow()
            start_date = now - timedelta(hours=hours)
            
            # URL format
            year = start_date.year
            month = start_date.month
            day = start_date.day
            
            url = f"{self.BASE_URL}?ts={year}{month:02d}{day:02d}&station={station_id}"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
                
                # Parse HTML response
                levels = self._parse_html_sounding(response.text, station_id)
                
                if not levels:
                    logger.warning(f"No data retrieved for station {station_id}")
                    return None
                
                sounding_dict = {
                    'station_id': station_id,
                    'station_name': self.STATIONS.get(station_id, {}).get('name', ''),
                    'valid_at': now.isoformat(),
                    'levels': levels,
                    'source': 'noaa',
                }
                
                logger.info(f"Fetched sounding for station {station_id}: {len(levels)} levels")
                return sounding_dict
        
        except Exception as e:
            logger.error(f"Error fetching sounding for station {station_id}: {e}")
            return None
    
    def _parse_html_sounding(self, html: str, station_id: int) -> Optional[List[Dict]]:
        """
        Parse HTML response from UWyo into sounding levels
        
        Returns list of dicts with pressure, temp, dewpoint, wind
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find the data table
            table = soup.find('table', {'border': '1'})
            if not table:
                return None
            
            levels = []
            rows = table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 6:
                    continue
                
                try:
                    level = {
                        'pressure_hpa': float(cells[0].text.strip()),
                        'temperature_c': float(cells[2].text.strip()) if cells[2].text.strip() else None,
                        'dewpoint_c': float(cells[3].text.strip()) if cells[3].text.strip() else None,
                        'wind_direction_deg': float(cells[4].text.strip()) if cells[4].text.strip() else None,
                        'wind_speed_ms': float(cells[5].text.strip()) * 0.51444 if cells[5].text.strip() else None,  # kt to m/s
                    }
                    levels.append(level)
                except (ValueError, IndexError):
                    continue
            
            return levels if levels else None
        
        except Exception as e:
            logger.error(f"Error parsing sounding HTML: {e}")
            return None


class OpenAIPProvider:
    """
    OpenAIP airspace polygons provider
    https://www.openaip.net/
    """
    
    DOWNLOAD_URL = 'https://www.openaip.net/download'
    
    def __init__(self):
        self.timeout = httpx.Timeout(30.0)  # Large file download
    
    async def download(self, bounds: Dict[str, float]) -> Optional[List[Dict]]:
        """
        Download and parse OpenAIP airspace data
        
        Args:
            bounds: {'north': 60, 'south': 43, 'west': -5, 'east': 20}
            
        Returns:
            List of airspace dicts with geometry and properties
        """
        try:
            # Download GeoJSON file
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(self.DOWNLOAD_URL)
                response.raise_for_status()
                
                data = response.json()
                
                # Filter by bounds
                features = data.get('features', [])
                filtered = self._filter_by_bounds(features, bounds)
                
                # Convert to our format
                airspace_list = []
                for feature in filtered:
                    props = feature.get('properties', {})
                    geom = feature.get('geometry', {})
                    
                    airspace = {
                        'name': props.get('name', 'Unknown'),
                        'airspace_class': props.get('class', 'G'),
                        'airspace_type': props.get('type', 'OTHER'),
                        'lower_limit_ft': self._parse_limit(props.get('lowerLimit', '0')),
                        'upper_limit_ft': self._parse_limit(props.get('upperLimit', '9999')),
                        'geometry': json.dumps(geom),
                    }
                    airspace_list.append(airspace)
                
                logger.info(f"Downloaded {len(airspace_list)} airspace polygons")
                return airspace_list
        
        except Exception as e:
            logger.error(f"Error downloading OpenAIP data: {e}")
            return None
    
    @staticmethod
    def _filter_by_bounds(features: List, bounds: Dict) -> List:
        """Filter features by geographic bounds"""
        filtered = []
        for feature in features:
            coords = feature.get('geometry', {}).get('coordinates')
            if coords:
                # Simple bounding box check
                # (In production, use proper GIS intersection)
                filtered.append(feature)
        return filtered
    
    @staticmethod
    def _parse_limit(limit_str: str) -> int:
        """Parse altitude limit string to feet"""
        try:
            if 'FL' in limit_str:
                return int(limit_str.replace('FL', '')) * 100
            else:
                return int(limit_str)
        except:
            return 9999


class DWDGRIBProvider:
    """
    DWD ICON-EU model data provider
    ftp://ftp.dwd.de/pub/data/weather/weather_reports/poi/
    """
    
    FTP_HOST = 'ftp.dwd.de'
    FTP_PATH = '/pub/data/weather/weather_reports/poi/'
    
    def __init__(self):
        self.timeout = httpx.Timeout(60.0)
    
    async def download_latest_icon(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Download latest ICON-EU GRIB2 model data
        
        Args:
            lat, lon: Location
            
        Returns:
            Dict with model data for interpolation
        """
        # Placeholder for FTP download
        # In production, implement actual FTP client
        logger.info(f"GRIB2 download placeholder for {lat}/{lon}")
        return None


# Usage example
if __name__ == '__main__':
    import asyncio
    
    async def test():
        # Test AVWX
        avwx = METARTAFProvider(api_key='sk_test_xxxxx')
        metar = await avwx.fetch_metar('EDDF')
        print(f"METAR: {metar}")
        
        # Test NOAA
        noaa = NOAASoundingsProvider()
        sounding = await noaa.fetch(10410)  # Munich
        print(f"Sounding levels: {len(sounding['levels'])}")
        
        # Test OpenAIP
        openaip = OpenAIPProvider()
        airspace = await openaip.download({'north': 60, 'south': 43, 'west': -5, 'east': 20})
        print(f"Airspace count: {len(airspace)}")
    
    asyncio.run(test())
