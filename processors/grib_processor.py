"""
grib_processor.py
Parse DWD ICON-EU GRIB2 files and extract variables on model grid
"""

import logging
from typing import Dict, Tuple, Optional
import numpy as np

logger = logging.getLogger(__name__)


class GRIBProcessor:
    """Process GRIB2 files from DWD ICON-EU model"""
    
    # Required variables for soaring calculations
    REQUIRED_VARIABLES = {
        'T': 'Temperature',
        'TD': 'Dewpoint',
        'U': 'U wind component',
        'V': 'V wind component',
        'W': 'Vertical velocity',
        'Z': 'Geopotential height',
        'CLCT': 'Total cloud cover',
        'CLCL': 'Low cloud',
        'CLCM': 'Medium cloud',
        'CLCH': 'High cloud',
        'PMSL': 'Mean sea level pressure',
        'CAPE_CON': 'Convective CAPE',
        'CIN': 'Convective inhibition',
        'T_2M': '2m temperature',
        'TD_2M': '2m dewpoint',
        'U_10M': '10m U wind',
        'V_10M': '10m V wind',
        'ASOB_S': 'Downward solar radiation',
        'TOT_PREC': 'Total precipitation',
    }
    
    # Standard pressure levels (hPa)
    PRESSURE_LEVELS = [1000, 925, 850, 700, 500, 400, 300, 200]
    
    @staticmethod
    def parse_grib2_file(filepath: str) -> Dict:
        """
        Parse GRIB2 file using cfgrib/eccodes
        
        Returns:
            Dict with:
            - 'model': 'ICON-EU'
            - 'forecast_hour': int
            - 'run_time': datetime
            - 'grid': numpy array lat/lon
            - 'variables': {variable_name: {level: data_array}}
        """
        try:
            import cfgrib
        except ImportError:
            logger.error("cfgrib not installed. Install: pip install cfgrib eccodes-python")
            return {}
        
        try:
            # Open GRIB2 with cfgrib
            ds = cfgrib.open_file(filepath)
            
            metadata = {
                'model': 'ICON-EU',
                'filepath': filepath,
                'variables': {},
            }
            
            # Extract forecast info from first variable
            for var_name in ds.data_vars:
                var = ds[var_name]
                if 'time' in var.dims:
                    metadata['run_time'] = var.time.values[0]
                    metadata['forecast_hour'] = 0  # Would parse from file name
                break
            
            # Extract grid
            if 'latitude' in ds.coords and 'longitude' in ds.coords:
                metadata['lat'] = ds.coords['latitude'].values
                metadata['lon'] = ds.coords['longitude'].values
            
            # Extract variables
            for var_name, standard_name in GRIBProcessor.REQUIRED_VARIABLES.items():
                if var_name in ds:
                    var = ds[var_name]
                    metadata['variables'][var_name] = {
                        'data': var.values,
                        'units': var.attrs.get('units', 'unknown'),
                        'standard_name': standard_name,
                    }
            
            logger.info(f"Loaded GRIB2: {filepath}, variables: {len(metadata['variables'])}")
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to parse GRIB2 {filepath}: {e}")
            return {}
    
    @staticmethod
    def interpolate_to_point(grid_data: Dict, lat: float, lon: float) -> Dict:
        """
        Interpolate GRIB2 grid data to a specific lat/lon point
        
        Args:
            grid_data: Output from parse_grib2_file
            lat: Target latitude
            lon: Target longitude
            
        Returns:
            Dict with interpolated values at all pressure levels
        """
        if not grid_data or 'lat' not in grid_data:
            return {}
        
        try:
            from scipy.interpolate import griddata
        except ImportError:
            logger.error("scipy required for interpolation")
            return {}
        
        result = {}
        
        # Find nearest grid points
        lat_grid = grid_data['lat']
        lon_grid = grid_data['lon']
        
        # Flatten grid coordinates
        points = np.column_stack([lat_grid.ravel(), lon_grid.ravel()])
        
        # Interpolate each variable
        for var_name, var_data in grid_data['variables'].items():
            values = var_data['data']
            
            if values.ndim == 2:  # 2D field (single level)
                # Check if lat/lon match grid shape
                if points.shape[0] == values.size:
                    # Simple nearest-neighbor for now
                    distances = np.sqrt((lat_grid - lat)**2 + (lon_grid - lon)**2)
                    nearest_idx = np.argmin(distances)
                    result[var_name] = values.ravel()[nearest_idx]
            
            elif values.ndim == 3:  # 3D field (multiple levels)
                # Interpolate each level
                result[var_name] = {}
                for level_idx in range(values.shape[0]):
                    level_data = values[level_idx]
                    if points.shape[0] == level_data.size:
                        distances = np.sqrt((lat_grid - lat)**2 + (lon_grid - lon)**2)
                        nearest_idx = np.argmin(distances)
                        result[var_name][level_idx] = level_data.ravel()[nearest_idx]
        
        return result
    
    @staticmethod
    def extract_sounding_profile(grid_data: Dict, lat: float, lon: float) -> list:
        """
        Extract atmospheric sounding profile from GRIB2 at specific location
        
        Returns:
            List of dicts with {pressure, height, temp, dewpoint, wind_u, wind_v}
        """
        if not grid_data:
            return []
        
        interpolated = GRIBProcessor.interpolate_to_point(grid_data, lat, lon)
        if not interpolated:
            return []
        
        sounding = []
        
        for level_idx, pressure_hpa in enumerate(GRIBProcessor.PRESSURE_LEVELS):
            level_dict = {
                'pressure_hpa': pressure_hpa,
                'height_m': None,
                'temperature': None,
                'dewpoint': None,
                'wind_u': None,
                'wind_v': None,
            }
            
            # Extract values for this pressure level
            if 'T' in interpolated and isinstance(interpolated['T'], dict):
                level_dict['temperature'] = interpolated['T'].get(level_idx)
            
            if 'TD' in interpolated and isinstance(interpolated['TD'], dict):
                level_dict['dewpoint'] = interpolated['TD'].get(level_idx)
            
            if 'Z' in interpolated and isinstance(interpolated['Z'], dict):
                level_dict['height_m'] = interpolated['Z'].get(level_idx)
            
            if 'U' in interpolated and isinstance(interpolated['U'], dict):
                level_dict['wind_u'] = interpolated['U'].get(level_idx)
            
            if 'V' in interpolated and isinstance(interpolated['V'], dict):
                level_dict['wind_v'] = interpolated['V'].get(level_idx)
            
            # Only add if we have actual data
            if level_dict['temperature'] is not None:
                sounding.append(level_dict)
        
        return sounding
    
    @staticmethod
    def compute_wind_components(wind_u: float, wind_v: float) -> Tuple[float, float]:
        """
        Calculate wind direction and speed from u,v components
        
        Returns:
            (direction_degrees, speed_m_s)
        """
        speed = np.sqrt(wind_u**2 + wind_v**2)
        direction = np.degrees(np.arctan2(wind_u, wind_v)) % 360
        return direction, speed
