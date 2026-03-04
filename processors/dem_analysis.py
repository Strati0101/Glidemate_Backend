"""
DEM Terrain Analysis Module

Computes terrain features from Copernicus DEM:
- Ridge detection (for wave flying)
- Slope and aspect (for thermal strength)
- Valley detection (for convergence zones)
- Shadow zones (for time-of-day thermal variations)

All features cached in PostgreSQL (static, never changes).
"""

import numpy as np
import logging
from typing import Dict, Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class TerrainAnalyzer:
    """
    Analyzes terrain from DEM raster data.
    """
    
    # Direction vectors for ridge detection (8 directions)
    DIRECTIONS = {
        'N': (0, 1),
        'NE': (1, 1),
        'E': (1, 0),
        'SE': (1, -1),
        'S': (0, -1),
        'SW': (-1, -1),
        'W': (-1, 0),
        'NW': (-1, 1)
    }
    
    DIRECTION_BEARINGS = {
        'N': 0,
        'NE': 45,
        'E': 90,
        'SE': 135,
        'S': 180,
        'SW': 225,
        'W': 270,
        'NW': 315
    }
    
    def __init__(self, dem_vrt_path: str = "/opt/glidemate-backend/data/dem/europe_30m.vrt"):
        self.dem_vrt_path = Path(dem_vrt_path)
        self.dem_data = None
        self.dem_bounds = None
        self.dem_resolution = 30  # meters
    
    def load_dem(self) -> bool:
        """Load DEM VRT into memory."""
        try:
            import rasterio
            
            if not self.dem_vrt_path.exists():
                logger.error(f"DEM VRT not found: {self.dem_vrt_path}")
                return False
            
            with rasterio.open(self.dem_vrt_path) as src:
                self.dem_data = src.read(1)  # Read first band
                self.dem_bounds = src.bounds
                self.dem_transform = src.transform
                
            logger.info(f"DEM loaded: {self.dem_data.shape}, bounds {self.dem_bounds}")
            return True
        
        except ImportError:
            logger.warning("rasterio not installed")
            return False
        except Exception as e:
            logger.error(f"Failed to load DEM: {e}")
            return False
    
    def latlon_to_pixel(self, lat: float, lon: float) -> Tuple[int, int]:
        """Convert latitude/longitude to DEM array coordinates."""
        if self.dem_transform is None:
            return None
        
        # GDAL/rasterio uses (col, row) = (x, y)
        col = int((lon - self.dem_bounds.left) / self.dem_resolution)
        row = int((self.dem_bounds.top - lat) / self.dem_resolution)
        
        if 0 <= col < self.dem_data.shape[1] and 0 <= row < self.dem_data.shape[0]:
            return row, col
        return None
    
    def get_elevation(self, lat: float, lon: float) -> Optional[float]:
        """Get elevation in meters at a point."""
        if self.dem_data is None:
            return None
        
        pixel = self.latlon_to_pixel(lat, lon)
        if pixel is None:
            return None
        
        row, col = pixel
        try:
            return float(self.dem_data[row, col])
        except Exception as e:
            logger.warning(f"Error reading elevation at {lat}/{lon}: {e}")
            return None
    
    def detect_ridges(self, lat: float, lon: float, search_radius_km: int = 200) -> Dict:
        """
        Detect ridges using 8-direction elevation scan.
        
        Returns:
            {
                "ridge_detected": bool,
                "ridge_direction": "N" | "NE" | ... | "NW",
                "ridge_bearing_deg": 0-360,
                "ridge_height_m": elevation difference,
                "ridge_distance_m": distance to ridge peak,
                "analysis": {...details...}
            }
        """
        if self.dem_data is None:
            return self._no_dem_fallback()
        
        center_elev = self.get_elevation(lat, lon)
        if center_elev is None:
            return self._no_dem_fallback()
        
        # Scan in all 8 directions
        ridge_info = {
            "center_elevation_m": center_elev,
            "directions": {}
        }
        
        max_height_diff = 0
        ridge_direction = None
        
        for dir_name, (dlat_factor, dlon_factor) in self.DIRECTIONS.items():
            # Sample elevation in this direction
            max_elev = center_elev
            distance_m = 0
            
            # Walk in direction up to search radius
            step_size_deg = 0.01  # ~1.1 km per step
            steps = int((search_radius_km / 1.1))
            
            for step in range(1, steps):
                sample_lat = lat + dlat_factor * step * step_size_deg
                sample_lon = lon + dlon_factor * step * step_size_deg
                sample_elev = self.get_elevation(sample_lat, sample_lon)
                
                if sample_elev is None:
                    break
                
                distance_m = step * step_size_deg * 111000  # ~111 km per degree
                max_elev = max(max_elev, sample_elev)
            
            height_diff = max_elev - center_elev
            ridge_info["directions"][dir_name] = {
                "max_elevation_m": max_elev,
                "height_difference_m": height_diff,
                "bearing_deg": self.DIRECTION_BEARINGS[dir_name]
            }
            
            # Track maximum ridge
            if height_diff > max_height_diff:
                max_height_diff = height_diff
                ridge_direction = dir_name
        
        # Determine if ridge is significant
        ridge_detected = max_height_diff > 100  # At least 100m elevation gain
        
        return {
            "ridge_detected": ridge_detected,
            "ridge_direction": ridge_direction if ridge_detected else None,
            "ridge_bearing_deg": self.DIRECTION_BEARINGS.get(ridge_direction) if ridge_detected else None,
            "ridge_height_m": max_height_diff,
            "center_elevation_m": center_elev,
            "analysis": ridge_info
        }
    
    def compute_slope_aspect(self, lat: float, lon: float) -> Dict:
        """
        Compute slope angle and aspect (direction slope faces).
        
        Uses simple finite difference method on DEM.
        
        Returns:
            {
                "slope_degrees": 0-90,
                "aspect_degrees": 0-360,
                "is_south_facing": bool,
                "thermal_factor": 0.7-1.3
            }
        """
        if self.dem_data is None:
            return self._no_dem_fallback()
        
        pixel = self.latlon_to_pixel(lat, lon)
        if pixel is None:
            return self._no_dem_fallback()
        
        row, col = pixel
        
        # Get neighboring elevation values
        try:
            center = self.dem_data[row, col]
            
            # Use 3x3 neighborhood
            north = self.dem_data[max(0, row-1), col]
            south = self.dem_data[min(self.dem_data.shape[0]-1, row+1), col]
            east = self.dem_data[row, min(self.dem_data.shape[1]-1, col+1)]
            west = self.dem_data[row, max(0, col-1)]
            
            ne = self.dem_data[max(0, row-1), min(self.dem_data.shape[1]-1, col+1)]
            nw = self.dem_data[max(0, row-1), max(0, col-1)]
            se = self.dem_data[min(self.dem_data.shape[0]-1, row+1), min(self.dem_data.shape[1]-1, col+1)]
            sw = self.dem_data[min(self.dem_data.shape[0]-1, row+1), max(0, col-1)]
            
            # Zevenbergen & Thorne (1987) slope calculation
            dz_dx = ((ne + 2*east + se) - (nw + 2*west + sw)) / (8 * self.dem_resolution)
            dz_dy = ((nw + 2*north + ne) - (sw + 2*south + se)) / (8 * self.dem_resolution)
            
            # Compute slope angle
            slope_rad = np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))
            slope_deg = np.degrees(slope_rad)
            
            # Compute aspect (direction of slope)
            aspect_rad = np.arctan2(dz_dy, -dz_dx)
            aspect_deg = (np.degrees(aspect_rad) + 360) % 360
            
            # South-facing slopes (aspect 135°-225°) are warmer
            is_south_facing = 135 <= aspect_deg <= 225
            
            # Thermal factor based on slope and aspect
            thermal_factor = self._compute_thermal_factor(slope_deg, aspect_deg)
            
            return {
                "slope_degrees": round(slope_deg, 1),
                "aspect_degrees": round(aspect_deg, 1),
                "is_south_facing": is_south_facing,
                "is_steep": slope_deg > 20,
                "thermal_factor": round(thermal_factor, 2),
                "center_elevation_m": float(center)
            }
        
        except Exception as e:
            logger.warning(f"Slope/aspect computation failed at {lat}/{lon}: {e}")
            return self._no_dem_fallback()
    
    def _compute_thermal_factor(self, slope_deg: float, aspect_deg: float) -> float:
        """
        Compute thermal enhancement factor based on terrain geometry.
        
        Rules:
        - South-facing (135°-225°) with slope > 5° → 1.1–1.3
        - North-facing (315°-45° or 315°-360°+0°-45°) → 0.7–0.9
        - Flat (< 5°) → 1.0
        - Steep (> 30°) → 1.05 (helps with boundary layer separation)
        """
        base_factor = 1.0
        
        # Slope contribution
        if slope_deg < 5:
            slope_factor = 1.0
        elif slope_deg > 30:
            slope_factor = 1.05
        else:
            slope_factor = 1.0 + (slope_deg - 5) / 50  # Linear between 5° and 30°
        
        # Aspect contribution
        is_south = 135 <= aspect_deg <= 225
        is_north = aspect_deg > 315 or aspect_deg < 45
        
        if is_south and slope_deg > 5:
            aspect_factor = 1.1 + min(0.2, slope_deg / 100)  # Up to 1.3
        elif is_north and slope_deg > 5:
            aspect_factor = 0.7 + min(0.2, slope_deg / 100)  # Down to 0.7
        else:
            aspect_factor = 1.0
        
        return slope_factor * aspect_factor
    
    def detect_valleys(self, lat: float, lon: float, search_radius_km: int = 50) -> Dict:
        """
        Detect valley terrain using curvature analysis.
        Valleys channel wind and create convergence zones.
        """
        if self.dem_data is None:
            return {"is_valley": False}
        
        # Simple curvature check: if surrounded by higher terrain
        center_elev = self.get_elevation(lat, lon)
        if center_elev is None:
            return {"is_valley": False}
        
        # Sample ring around point
        surrounding_elevs = []
        n_samples = 8
        
        for i in range(n_samples):
            angle = 2 * np.pi * i / n_samples
            dlat = np.sin(angle) * search_radius_km / 111
            dlon = np.cos(angle) * search_radius_km / (111 * np.cos(np.radians(lat)))
            
            sample_elev = self.get_elevation(lat + dlat, lon + dlon)
            if sample_elev is not None:
                surrounding_elevs.append(sample_elev)
        
        if not surrounding_elevs:
            return {"is_valley": False}
        
        # Valley = center lower than surroundings
        surrounding_mean = np.mean(surrounding_elevs)
        elevation_diff = surrounding_mean - center_elev
        
        is_valley = elevation_diff > 50  # At least 50m elevation difference
        
        return {
            "is_valley": is_valley,
            "surrounding_mean_elevation_m": round(surrounding_mean, 1),
            "center_elevation_m": round(center_elev, 1),
            "convergence_boost": 1.15 if is_valley else 1.0
        }
    
    def compute_shadow_factor(self, lat: float, lon: float, utc_hour: int) -> Dict:
        """
        Compute shading based on sun position and terrain aspect.
        
        In shadow (north-facing, sun to south) → thermal factor 0.7
        In sun (south-facing, sun rising) → thermal factor 1.2
        """
        slope_data = self.compute_slope_aspect(lat, lon)
        aspect_deg = slope_data.get("aspect_degrees")
        
        # Simple sun position approximation (UTC hour)
        # At noon (12 UTC), sun is roughly south
        sun_azimuth = (12 - utc_hour) * 15  # 15° per hour
        sun_azimuth = (sun_azimuth + 360) % 360
        
        # Angle between aspect and sun
        angle_diff = abs(aspect_deg - sun_azimuth)
        if angle_diff > 180:
            angle_diff = 360 - angle_diff
        
        # If aspect is similar to sun direction, more illuminated
        if angle_diff < 90:
            shadow_factor = 1.2  # Well illuminated
        elif angle_diff > 90:
            shadow_factor = 0.7  # In shadow
        else:
            shadow_factor = 1.0
        
        return {
            "shadow_factor": shadow_factor,
            "sun_azimuth_deg": round(sun_azimuth, 1),
            "aspect_degrees": aspect_deg,
            "illumination": "well-lit" if shadow_factor > 1.0 else ("shaded" if shadow_factor < 1.0 else "neutral")
        }
    
    def _no_dem_fallback(self) -> Dict:
        """Return neutral terrain analysis when DEM not available."""
        return {
            "dem_available": False,
            "thermal_factor": 1.0,
            "ridge_detected": False,
            "slope_degrees": None,
            "aspect_degrees": None
        }


# Public API

def analyze_terrain(lat: float, lon: float, utc_hour: Optional[int] = None) -> Dict:
    """
    Complete terrain analysis for a location.
    
    Returns all terrain-based corrections:
    - Ridge detection for wave flying
    - Slope/aspect for thermal strength
    - Valley detection for convergence
    - Shadow zones for time-of-day variations
    """
    analyzer = TerrainAnalyzer()
    
    if not analyzer.load_dem():
        return {
            "dem_available": False,
            "message": "DEM not available, using base model"
        }
    
    ridge_data = analyzer.detect_ridges(lat, lon)
    slope_data = analyzer.compute_slope_aspect(lat, lon)
    valley_data = analyzer.detect_valleys(lat, lon)
    
    shadow_data = {}
    if utc_hour is not None:
        shadow_data = analyzer.compute_shadow_factor(lat, lon, utc_hour)
    
    # Combine all factors
    thermal_factor = (
        slope_data.get("thermal_factor", 1.0) *
        valley_data.get("convergence_boost", 1.0) *
        shadow_data.get("shadow_factor", 1.0)
    )
    
    return {
        "location": {"lat": lat, "lon": lon},
        "dem_available": True,
        "ridge": ridge_data,
        "slope_aspect": slope_data,
        "valley": valley_data,
        "shadow": shadow_data,
        "combined_thermal_factor": round(thermal_factor, 2),
        "froude_adjustment_factor": round(ridge_data.get("ridge_height_m", 0) / 1000, 2)  # Ridge height for Froude calc
    }


# Cache terrain analysis in database
def cache_terrain_analysis(db_session, lat: float, lon: float):
    """
    Cache computed terrain analysis in PostgreSQL.
    Called by Celery task.
    """
    from backend_database_models import TerrainContext
    
    analysis = analyze_terrain(lat, lon)
    
    if not analysis.get("dem_available"):
        return
    
    terrain = TerrainContext(
        latitude=lat,
        longitude=lon,
        elevation_m=analysis["slope_aspect"].get("center_elevation_m"),
        slope_deg=analysis["slope_aspect"].get("slope_degrees"),
        aspect_deg=analysis["slope_aspect"].get("aspect_degrees"),
        thermal_factor=analysis["combined_thermal_factor"],
        ridge_height_m=analysis["ridge"].get("ridge_height_m"),
        is_valley=analysis["valley"].get("is_valley"),
        cached_at=datetime.utcnow()
    )
    
    db_session.merge(terrain)
    db_session.commit()
