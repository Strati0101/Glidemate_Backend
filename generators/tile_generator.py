"""
services/tile_generator.py
Generate meteorological overlay tiles (PNG) for map display
Supports multiple overlays: thermal potential, cloudbase, temperature, wind, etc.
"""

import logging
import numpy as np
from typing import Dict, Tuple, Optional
from datetime import datetime
from pathlib import Path
import json

try:
    from PIL import Image, ImageDraw
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    from matplotlib.cm import ScalarMappable
    import cmocean
except ImportError:
    Image = None
    plt = None
    ScalarMappable = None
    cmocean = None

logger = logging.getLogger(__name__)

# Tile size in pixels
TILE_SIZE = 256

# Colormap definitions for different overlays
COLORMAPS = {
    'thermal': {
        'levels': [0, 1, 2, 3, 4, 5, 6, 7, 8, 10],
        'colors': ['#ffffff', '#e0f2ff', '#b3d9ff', '#80bfff', '#4da6ff', 
                  '#1a8cff', '#0066cc', '#0052a3', '#003d7a', '#000000'],
        'label': 'Thermal Strength (m/s)',
        'units': 'm/s'
    },
    'cloudbase': {
        'levels': [0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 5000],
        'colors': ['#4d0000', '#800000', '#cc0000', '#ff6600', '#ffcc00',
                  '#ccff00', '#66ff00', '#00ff00', '#00ccff', '#0052cc'],
        'label': 'Cloud Base (m MSL)',
        'units': 'm'
    },
    'temperature': {
        'levels': [-20, -10, 0, 5, 10, 15, 20, 25, 30, 40],
        'colors': ['#000080', '#0000ff', '#00ffff', '#00ff00', '#ffff00',
                  '#ff8800', '#ff4400', '#ff0000', '#cc0000', '#800000'],
        'label': 'Temperature (°C)',
        'units': '°C'
    },
    'wind': {
        'levels': [0, 5, 10, 15, 20, 25, 30, 40, 50, 60],
        'colors': ['#ffffff', '#80ff00', '#ffff00', '#ffa500', '#ff6600',
                  '#ff3300', '#ff0000', '#cc0000', '#990000', '#660000'],
        'label': 'Wind Speed (m/s)',
        'units': 'm/s'
    },
    'rain': {
        'levels': [0, 0.1, 0.5, 1, 2, 5, 10, 20, 50, 100],
        'colors': ['#ffffff', '#e6f2ff', '#b3d9ff', '#80bfff', '#4da6ff',
                  '#1a8cff', '#003d99', '#002660', '#001033', '#000000'],
        'label': 'Precipitation (mm)',
        'units': 'mm'
    },
    'cape': {
        'levels': [0, 100, 300, 500, 1000, 1500, 2000, 3000, 4000, 5000],
        'colors': ['#ffffff', '#e0f2ff', '#b3d9ff', '#80bfff', '#ffff00',
                  '#ff9900', '#ff6600', '#ff3300', '#ff0000', '#cc0000'],
        'label': 'CAPE (J/kg)',
        'units': 'J/kg'
    },
    'lifted_index': {
        'levels': [-10, -8, -6, -4, -2, 0, 2, 4, 6, 8],
        'colors': ['#ff0000', '#ff3333', '#ff6666', '#ff9999', '#ffcccc',
                  '#ccffcc', '#99ff99', '#66ff66', '#33ff33', '#00ff00'],
        'label': 'Lifted Index (°C)',
        'units': '°C'
    },
    'wave': {
        'levels': [0, 2, 4, 6, 8, 10, 15, 20, 30, 50],
        'colors': ['#ffffff', '#e6f2ff', '#b3d9ff', '#80bfff', '#4da6ff',
                  '#1a8cff', '#0066cc', '#003d99', '#001a4d', '#000000'],
        'label': 'Wave Strength (m/s)',
        'units': 'm/s'
    },
}


class TileGenerator:
    """
    Generate map tiles from meteorological data
    Supports Web Mercator tiles (z: 0-20, x: 0-2^z-1, y: 0-2^z-1)
    """
    
    def __init__(self, output_dir: str = "/tmp/tiles"):
        """
        Initialize tile generator
        
        Args:
            output_dir: Base directory for tile storage
        """
        if Image is None:
            logger.warning("PIL/Pillow not installed, tile generation disabled")
            self.enabled = False
            return
        
        self.enabled = True
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Tile generator initialized, output: {self.output_dir}")
    
    def lat_lon_to_web_mercator(self, lat: float, lon: float) -> Tuple[float, float]:
        """Convert lat/lon to Web Mercator projection (0-1)"""
        x = (lon + 180) / 360
        y = (1 - np.log(np.tan((lat + 90) * np.pi / 360)) / np.pi) / 2
        return x, y
    
    def web_mercator_to_lat_lon(self, x: float, y: float) -> Tuple[float, float]:
        """Convert Web Mercator (0-1) back to lat/lon"""
        lon = x * 360 - 180
        lat = 2 * np.arctan(np.exp((1 - y) * np.pi)) * 180 / np.pi - 90
        return lat, lon
    
    def get_tile_bounds(self, z: int, x: int, y: int) -> Dict[str, float]:
        """
        Get geographic bounds of a tile
        
        Returns:
            {'north': lat, 'south': lat, 'east': lon, 'west': lon}
        """
        # Convert tile coords to Web Mercator
        n_tiles = 2 ** z
        x_norm = x / n_tiles
        y_norm = y / n_tiles
        x_norm_next = (x + 1) / n_tiles
        y_norm_next = (y + 1) / n_tiles
        
        lat_n, lon_w = self.web_mercator_to_lat_lon(x_norm, y_norm)
        lat_s, lon_e = self.web_mercator_to_lat_lon(x_norm_next, y_norm_next)
        
        return {
            'north': lat_n,
            'south': lat_s,
            'east': lon_e,
            'west': lon_w
        }
    
    def value_to_color(self, value: float, colormap_name: str) -> Tuple[int, int, int]:
        """
        Map value to RGB color using colormap
        
        Args:
            value: Data value
            colormap_name: Key in COLORMAPS dict
            
        Returns:
            RGB tuple (0-255, 0-255, 0-255)
        """
        if colormap_name not in COLORMAPS:
            return (128, 128, 128)  # Gray for unknown
        
        colormap = COLORMAPS[colormap_name]
        levels = colormap['levels']
        colors = colormap['colors']
        
        # Clamp value to level range
        if value < levels[0]:
            color_hex = colors[0]
        elif value >= levels[-1]:
            color_hex = colors[-1]
        else:
            # Find which level bracket we're in
            for i in range(len(levels) - 1):
                if levels[i] <= value < levels[i + 1]:
                    # Linear interpolation between colors
                    color_hex = colors[i]
                    break
        
        # Convert hex to RGB
        color_hex = color_hex.lstrip('#')
        r = int(color_hex[0:2], 16)
        g = int(color_hex[2:4], 16)
        b = int(color_hex[4:6], 16)
        
        return (r, g, b)
    
    def generate_tile(self, z: int, x: int, y: int, overlay: str, 
                     grid_data: np.ndarray, grid_lat: np.ndarray, 
                     grid_lon: np.ndarray, 
                     forecast_hour: int = 0) -> Optional[str]:
        """
        Generate a single tile PNG
        
        Args:
            z, x, y: Tile coordinates
            overlay: Overlay type (thermal, cloudbase, wind, etc.)
            grid_data: 2D array of values (same shape as grid_lat/lon)
            grid_lat: 2D array of latitudes for grid points
            grid_lon: 2D array of longitudes for grid points
            forecast_hour: Forecast hour (for file naming)
            
        Returns:
            Path to generated tile file, or None on error
        """
        if not self.enabled:
            return None
        
        try:
            # Create blank tile
            tile_img = Image.new('RGB', (TILE_SIZE, TILE_SIZE), color=(240, 240, 240))
            pixels = tile_img.load()
            
            # Get tile bounds
            bounds = self.get_tile_bounds(z, x, y)
            
            # For each pixel in tile, find nearest grid value
            for py in range(TILE_SIZE):
                for px in range(TILE_SIZE):
                    # Convert pixel coords to normalized lat/lon
                    norm_x = px / TILE_SIZE
                    norm_y = py / TILE_SIZE
                    
                    lat = bounds['north'] + (bounds['south'] - bounds['north']) * norm_y
                    lon = bounds['west'] + (bounds['east'] - bounds['west']) * norm_x
                    
                    # Find nearest grid point (simple nearest-neighbor)
                    distances = np.sqrt((grid_lat - lat)**2 + (grid_lon - lon)**2)
                    nearest_idx = np.argmin(distances)
                    nearest_idx_2d = np.unravel_index(nearest_idx, grid_lat.shape)
                    
                    value = grid_data[nearest_idx_2d]
                    
                    # Skip NaN values (no data)
                    if np.isnan(value):
                        continue
                    
                    # Get color for this value
                    color = self.value_to_color(value, overlay)
                    pixels[px, py] = color
            
            # Save tile
            tile_path = self.output_dir / overlay / str(z) / str(x)
            tile_path.mkdir(parents=True, exist_ok=True)
            
            tile_file = tile_path / f"{y}_f{forecast_hour}.png"
            tile_img.save(tile_file, 'PNG', optimize=True)
            
            logger.debug(f"Generated tile: {tile_file}")
            return str(tile_file)
        
        except Exception as e:
            logger.error(f"Error generating tile {overlay}/{z}/{x}/{y}: {e}")
            return None
    
    def generate_tile_pyramid(self, overlay: str, grid_data: np.ndarray,
                            grid_lat: np.ndarray, grid_lon: np.ndarray,
                            z_min: int = 4, z_max: int = 10,
                            forecast_hour: int = 0) -> Dict[int, int]:
        """
        Generate complete tile pyramid (multiple zoom levels)
        
        Args:
            overlay: Overlay type
            grid_data, grid_lat, grid_lon: Grid arrays
            z_min, z_max: Zoom level range
            forecast_hour: Forecast hour
            
        Returns:
            {'z4': 16, 'z5': 64, 'z6': 256, ...} (tiles generated per level)
        """
        result = {}
        
        for z in range(z_min, z_max + 1):
            n_tiles = 2 ** z
            count = 0
            
            for x in range(n_tiles):
                for y in range(n_tiles):
                    path = self.generate_tile(z, x, y, overlay, grid_data, 
                                             grid_lat, grid_lon, forecast_hour)
                    if path:
                        count += 1
            
            result[f'z{z}'] = count
            logger.info(f"Generated {count} tiles at zoom {z}")
        
        return result
    
    def create_colorbar_tile(self, overlay: str, width: int = 256, 
                            height: int = 50) -> Optional[str]:
        """
        Create a colorbar legend tile
        
        Args:
            overlay: Overlay type
            width, height: Image dimensions
            
        Returns:
            Path to colorbar image
        """
        if not self.enabled or overlay not in COLORMAPS:
            return None
        
        try:
            colormap = COLORMAPS[overlay]
            img = Image.new('RGB', (width, height), color=(255, 255, 255))
            pixels = img.load()
            
            # Draw color gradient
            levels = colormap['levels']
            colors = colormap['colors']
            
            for x in range(width):
                # Map pixel position to value range
                value_frac = x / width
                value = levels[0] + (levels[-1] - levels[0]) * value_frac
                
                color = self.value_to_color(value, overlay)
                for y in range(height):
                    pixels[x, y] = color
            
            # Save colorbar
            colorbar_path = self.output_dir / 'colorbars'
            colorbar_path.mkdir(parents=True, exist_ok=True)
            
            colorbar_file = colorbar_path / f"{overlay}_colorbar.png"
            img.save(colorbar_file, 'PNG')
            
            return str(colorbar_file)
        
        except Exception as e:
            logger.error(f"Error creating colorbar for {overlay}: {e}")
            return None
