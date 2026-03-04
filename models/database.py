"""
database/models.py
SQLAlchemy ORM models for meteorological data with PostGIS support
"""

from datetime import datetime, timedelta
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
import json

Base = declarative_base()


class MetarReport(Base):
    """METAR aviation weather reports"""
    __tablename__ = 'metar_reports'
    
    id = Column(Integer, primary_key=True)
    icao = Column(String(4), index=True, nullable=False)
    station_name = Column(String(100))
    lat = Column(Float)
    lon = Column(Float)
    observed_at = Column(DateTime, index=True, nullable=False)
    received_at = Column(DateTime, default=datetime.utcnow)
    raw_text = Column(Text)
    
    # Decoded fields
    temperature_c = Column(Float)
    dewpoint_c = Column(Float)
    wind_direction = Column(Float)  # degrees 0-360
    wind_speed_ms = Column(Float)
    wind_gust_ms = Column(Float, nullable=True)
    visibility_m = Column(Float)
    cloud_layers_json = Column(JSON)  # [{height_m, coverage}, ...]
    flight_category = Column(String(4))  # LIFR, MVFR, IFR, VFR
    
    __table_args__ = (
        Index('idx_metar_station_time', 'icao', 'observed_at'),
        Index('idx_metar_received', 'received_at'),
    )


class TafReport(Base):
    """TAF terminal aerodrome forecasts"""
    __tablename__ = 'taf_reports'
    
    id = Column(Integer, primary_key=True)
    icao = Column(String(4), index=True, nullable=False)
    station_name = Column(String(100))
    lat = Column(Float)
    lon = Column(Float)
    issued_at = Column(DateTime, nullable=False)
    valid_from = Column(DateTime, index=True, nullable=False)
    valid_to = Column(DateTime)
    received_at = Column(DateTime, default=datetime.utcnow)
    raw_text = Column(Text)
    
    # JSON array of forecast groups
    groups_json = Column(JSON)  # [{valid_period, wind, visibility, weather, ...}, ...]
    
    __table_args__ = (
        Index('idx_taf_station_valid', 'icao', 'valid_from'),
    )


class SoundingProfile(Base):
    """Atmospheric sounding profiles from radiosondes or model"""
    __tablename__ = 'sounding_profiles'
    
    id = Column(Integer, primary_key=True)
    source = Column(String(20))  # 'noaa', 'icon-eu', 'gfs'
    station_id = Column(String(20), index=True, nullable=True)
    station_name = Column(String(100))
    lat = Column(Float)
    lon = Column(Float)
    
    valid_at = Column(DateTime, index=True, nullable=False)
    received_at = Column(DateTime, default=datetime.utcnow)
    
    # Sounding levels JSON: [{pressure_hpa, height_m, temp_c, dewpoint_c, wind_dir, wind_speed}, ...]
    levels_json = Column(JSON, nullable=False)
    
    # Computed indices
    cape_j_kg = Column(Float)
    cape_3km = Column(Float, nullable=True)
    cin_j_kg = Column(Float)
    lifted_index = Column(Float)
    k_index = Column(Float)
    total_totals = Column(Float)
    showalter_index = Column(Float)
    boyden_convection_index = Column(Float, nullable=True)
    
    lcl_pressure_hpa = Column(Float)
    lcl_height_m = Column(Float)
    lfc_pressure_hpa = Column(Float, nullable=True)
    el_pressure_hpa = Column(Float, nullable=True)
    
    cloud_base_m = Column(Float, nullable=True)
    thermal_top_m = Column(Float, nullable=True)
    trigger_temperature_c = Column(Float, nullable=True)
    
    freezing_level_m = Column(Float)
    precipitable_water_mm = Column(Float)
    
    ventilation_rate = Column(Float, nullable=True)
    bulk_richardson = Column(Float, nullable=True)
    energy_helicity = Column(Float, nullable=True)
    
    # Soaring summary
    soaring_rating = Column(Integer)  # 0-5
    blue_day_flag = Column(Integer, default=0)
    od_risk = Column(String(20))  # 'none', 'low', 'moderate', 'high'
    
    __table_args__ = (
        Index('idx_sounding_station_time', 'station_id', 'valid_at'),
        Index('idx_sounding_location_time', 'lat', 'lon', 'valid_at'),
    )


class ModelGridMetadata(Base):
    """Metadata for processed model grids (for tiles)"""
    __tablename__ = 'model_grid_metadata'
    
    id = Column(Integer, primary_key=True)
    model = Column(String(20))  # 'icon-eu', 'gfs'
    run_time = Column(DateTime, index=True, nullable=False)
    forecast_hour = Column(Integer, nullable=False)
    variable = Column(String(50), nullable=False)  # 'cape', 'cloud_base', 'thermal_strength', etc
    
    file_path = Column(String(500))  # Path to GRIB2 or cache file
    
    # Spatial extent
    north = Column(Float)
    south = Column(Float)
    east = Column(Float)
    west = Column(Float)
    resolution_deg = Column(Float)  # Grid spacing in degrees
    
    # Tile info
    tiles_dir = Column(String(500))  # Path to generated tiles
    min_zoom = Column(Integer, default=4)
    max_zoom = Column(Integer, default=10)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)  # When to delete old tiles
    
    __table_args__ = (
        Index('idx_model_run_hour', 'model', 'run_time', 'forecast_hour'),
        Index('idx_model_variable_time', 'variable', 'run_time'),
    )


class Airspace(Base):
    """Airspace polygons from OpenAIP"""
    __tablename__ = 'airspace'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    airspace_class = Column(String(10))  # A, B, C, D, E, F, G
    airspace_type = Column(String(50))  # CTR, TMA, CTA, etc
    
    lower_limit_ft = Column(Integer)  # AGL or AMSL
    upper_limit_ft = Column(Integer)
    lower_limit_type = Column(String(20))  # 'AGL', 'MSL'
    upper_limit_type = Column(String(20))
    
    # PostGIS geometry
    geometry = Column(Geometry('POLYGON', srid=4326), nullable=False)
    
    source = Column(String(50))  # 'openaip', 'dfs', etc
    source_id = Column(String(100))
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_airspace_class', 'airspace_class'),
        # Geometry index will be created manually after table creation
    )


class GliderAirfield(Base):
    """Glider airfield database"""
    __tablename__ = 'glider_airfields'
    
    id = Column(Integer, primary_key=True)
    icao = Column(String(4), unique=True, nullable=True, index=True)
    name = Column(String(200), nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    elevation_m = Column(Integer)
    
    # Runway info
    runways_json = Column(JSON)  # [{heading, length_m, surface}, ...]
    
    # Services
    has_fuel = Column(Integer, default=0)
    has_accommodation = Column(Integer, default=0)
    has_maintenance = Column(Integer, default=0)
    
    website = Column(String(200), nullable=True)
    contact_phone = Column(String(50), nullable=True)
    
    __table_args__ = (
        Index('idx_airfield_location', 'lat', 'lon'),
    )


class TrafficCache(Base):
    """Live OGN aircraft traffic (expires quickly)"""
    __tablename__ = 'traffic_cache'
    
    id = Column(Integer, primary_key=True)
    callsign = Column(String(20), unique=True, index=True, nullable=False)
    registration = Column(String(20), nullable=True)
    aircraft_type = Column(String(100), nullable=True)
    
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    altitude_m = Column(Integer)
    vertical_speed_ms = Column(Float)  # positive = climbing
    groundspeed_ms = Column(Float)
    track_degrees = Column(Float)
    
    last_update = Column(DateTime, default=datetime.utcnow, index=True)
    expires_at = Column(DateTime)  # Auto-delete after TTL
    
    __table_args__ = (
        Index('idx_traffic_location', 'lat', 'lon'),
        Index('idx_traffic_expires', 'expires_at'),
    )


class CacheEntry(Base):
    """Generic cache table for API responses"""
    __tablename__ = 'cache_entries'
    
    id = Column(Integer, primary_key=True)
    key = Column(String(500), unique=True, index=True, nullable=False)
    value = Column(JSON, nullable=False)
    content_type = Column(String(100))  # 'application/json', 'image/png', etc
    
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, index=True)
    
    __table_args__ = (
        Index('idx_cache_expires', 'expires_at'),
    )


# ============ NEW MODELS (Phase 3 Extensions) ============


class MeteoFranceAromeForecast(Base):
    """Météo-France AROME model forecasts (1.3km resolution)
    
    Priority Region: 43°N–52°N, 5°W–10°E (France, Alsace, Lorraine, Swiss border)
    Update Frequency: Every 1 hour
    Max Forecast: 48 hours
    """
    __tablename__ = 'meteofrance_arome_forecasts'
    
    id = Column(Integer, primary_key=True)
    lat = Column(Float, index=True, nullable=False)
    lon = Column(Float, index=True, nullable=False)
    
    issued_at = Column(DateTime, nullable=False)
    valid_at = Column(DateTime, index=True, nullable=False)
    forecast_hour = Column(Integer)  # 0-48 hours ahead
    
    received_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Temperature and humidity
    temperature_2m_c = Column(Float)
    dewpoint_2m_c = Column(Float)
    relative_humidity_pct = Column(Float)
    
    # Wind (surface)
    u_wind_10m_ms = Column(Float)
    v_wind_10m_ms = Column(Float)
    wind_direction_deg = Column(Float)
    wind_speed_10m_ms = Column(Float)
    
    # Pressure and clouds
    pressure_hpa = Column(Float)
    total_cloud_cover_pct = Column(Float)
    
    # Precipitation and convection
    total_precipitation_mm = Column(Float)
    cape_j_kg = Column(Float)
    
    # Upper levels (pressure levels)
    temperature_850_c = Column(Float, nullable=True)
    u_wind_850_ms = Column(Float, nullable=True)
    v_wind_850_ms = Column(Float, nullable=True)
    
    temperature_700_c = Column(Float, nullable=True)
    u_wind_700_ms = Column(Float, nullable=True)
    v_wind_700_ms = Column(Float, nullable=True)
    
    temperature_500_c = Column(Float, nullable=True)
    u_wind_500_ms = Column(Float, nullable=True)
    v_wind_500_ms = Column(Float, nullable=True)
    
    # Full GRIB2 data (optional, for detailed parcel analysis)
    grib2_binary = Column(Text, nullable=True)  # base64-encoded
    
    # JSON storage for any additional fields
    extra_data = Column(JSON, nullable=True)
    
    __table_args__ = (
        Index('idx_arome_location_time', 'lat', 'lon', 'valid_at'),
        Index('idx_arome_valid_time', 'valid_at'),
        Index('idx_arome_received', 'received_at'),
    )


class GeoSphereAustriaForecast(Base):
    """GeoSphere Austria INCA model forecasts (1km resolution)
    
    Priority Region: 46°N–49.5°N, 9°E–18°E (Austria, South Bavaria, South Tyrol)
    Update Frequency: Every 1 hour (or from station observations)
    Max Forecast: 48 hours
    """
    __tablename__ = 'geosphere_austria_forecasts'
    
    id = Column(Integer, primary_key=True)
    lat = Column(Float, index=True, nullable=False)
    lon = Column(Float, index=True, nullable=False)
    
    issued_at = Column(DateTime, nullable=False)
    valid_at = Column(DateTime, index=True, nullable=False)
    forecast_hour = Column(Integer)  # 0-48 hours ahead
    
    received_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Temperature and humidity (INCA)
    temperature_2m_c = Column(Float)
    dewpoint_2m_c = Column(Float)
    relative_humidity_pct = Column(Float)
    
    # Wind vectors
    u_wind_ms = Column(Float)
    v_wind_ms = Column(Float)
    wind_direction_deg = Column(Float)
    wind_speed_ms = Column(Float)
    
    # Precipitation and solar radiation
    precipitation_mm = Column(Float)
    solar_radiation_w_m2 = Column(Float, nullable=True)
    
    # JSON for gridded data if applicable
    gridded_data = Column(JSON, nullable=True)
    
    __table_args__ = (
        Index('idx_inca_location_time', 'lat', 'lon', 'valid_at'),
        Index('idx_inca_valid_time', 'valid_at'),
        Index('idx_inca_received', 'received_at'),
    )


class GeoSphereAustriaObservation(Base):
    """GeoSphere Austria TAWES station observations (10-minute frequency)
    
    Network: Austrian weather stations (TAWES = Temperatur, Niederschlag, Wind Einheit Stationen)
    Update: Every 10 minutes
    """
    __tablename__ = 'geosphere_austria_observations'
    
    id = Column(Integer, primary_key=True)
    station_id = Column(String(20), index=True, nullable=False)
    station_name = Column(String(100))
    lat = Column(Float, index=True, nullable=False)
    lon = Column(Float, index=True, nullable=False)
    elevation_m = Column(Float)
    
    observed_at = Column(DateTime, index=True, nullable=False)
    received_at = Column(DateTime, default=datetime.utcnow)
    
    # Meteorological values
    temperature_c = Column(Float)
    dewpoint_c = Column(Float)
    relative_humidity_pct = Column(Float, nullable=True)
    
    wind_direction_deg = Column(Float)
    wind_speed_ms = Column(Float)
    wind_gust_ms = Column(Float, nullable=True)
    
    pressure_hpa = Column(Float, nullable=True)
    precipitation_mm = Column(Float, nullable=True)
    
    sunshine_hours = Column(Float, nullable=True)
    temp_extreme_c = Column(Float, nullable=True)
    
    __table_args__ = (
        Index('idx_zamg_station_time', 'station_id', 'observed_at'),
        Index('idx_zamg_location_time', 'lat', 'lon', 'observed_at'),
    )


class TerrainContext(Base):
    """Cached terrain analysis from Copernicus DEM
    
    Static terrain features computed from 30m DEM.
    Never changes unless DEM is redownloaded.
    Used by parcel.py calculations for Froude number and thermal strength.
    """
    __tablename__ = 'terrain_context'
    
    id = Column(Integer, primary_key=True)
    latitude = Column(Float, index=True, nullable=False, unique=True)
    longitude = Column(Float, index=True, nullable=False, unique=True)
    
    # Elevation and slope
    elevation_m = Column(Float)
    slope_degrees = Column(Float)
    aspect_degrees = Column(Float)
    
    # Ridge detection (for wave flying)
    ridge_height_m = Column(Float, nullable=True)
    ridge_bearing_deg = Column(Float, nullable=True)
    
    # Valley detection (for convergence zones)
    is_valley = Column(Integer, default=0)
    convergence_boost = Column(Float, default=1.0)
    
    # Thermal enhancement factor
    # Accounts for: south-facing slopes (1.1-1.3), north-facing slopes (0.7-0.9)
    thermal_factor = Column(Float, default=1.0)
    
    # When this analysis was computed
    cached_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_terrain_location', 'latitude', 'longitude'),
    )


class ThermalObservation(Base):
    """OGN-detected thermal observations for ML training
    
    Stores circling detections from OGN APRS traffic.
    Used to train XGBoost models and calculate observation bias.
    Only records positive thermals (climb_rate_ms > 0.5 m/s).
    """
    __tablename__ = 'thermal_observations'
    
    id = Column(Integer, primary_key=True)
    
    # Location
    lat = Column(Float, index=True, nullable=False)
    lon = Column(Float, index=True, nullable=False)
    altitude_m = Column(Float)
    alt_m = Column(Float)  # Alias for backward compatibility
    
    # Thermal properties (observed)
    climb_rate_ms = Column(Float, nullable=False)  # Average climb in thermal
    alt_top_m = Column(Float)  # Estimated thermal top
    thermal_radius_m = Column(Float)  # Circling radius
    duration_s = Column(Float, nullable=True)  # Time spent circling
    
    # Timestamp
    timestamp = Column(DateTime, index=True, nullable=False)
    received_at = Column(DateTime, default=datetime.utcnow)
    
    # Source aircraft
    callsign = Column(String(20), nullable=True)
    
    # Model predictions (for bias calculations)
    model_climb_rate_ms = Column(Float, nullable=True)
    model_cape_jkg = Column(Float, nullable=True)
    model_bci_jkg = Column(Float, nullable=True)
    model_thermal_top_m = Column(Float, nullable=True)
    
    # Atmospheric context at observation time
    cape = Column(Float, nullable=True)
    lapse_rate = Column(Float, nullable=True)
    li = Column(Float, nullable=True)  # Lifted Index
    wind_shear_05km = Column(Float, nullable=True)
    surface_temp_c = Column(Float, nullable=True)
    soil_moisture = Column(Float, nullable=True)
    cloud_cover = Column(Float, nullable=True)
    wind_speed_10m = Column(Float, nullable=True)
    wind_direction_10m = Column(Float, nullable=True)
    
    # Terrain
    land_cover_class = Column(String(50), nullable=True)
    elevation_m = Column(Float, nullable=True)
    slope_deg = Column(Float, nullable=True)
    aspect_deg = Column(Float, nullable=True)
    
    # Bias factor (actual / predicted)
    bias_factor = Column(Float, nullable=True)
    
    __table_args__ = (
        Index('idx_thermal_location_time', 'lat', 'lon', 'timestamp'),
        Index('idx_thermal_timestamp', 'timestamp'),
        Index('idx_thermal_location', 'lat', 'lon'),
        Index('idx_thermal_callsign_time', 'callsign', 'timestamp'),
    )


class DiagnosticRun(Base):
    """System diagnostic check results"""
    __tablename__ = 'diagnostic_runs'
    
    id = Column(Integer, primary_key=True)
    
    # Timing
    run_at = Column(DateTime, index=True, default=datetime.utcnow, nullable=False)
    duration_seconds = Column(Float)
    
    # Results summary
    total_checks = Column(Integer)
    passed = Column(Integer)
    failed = Column(Integer)
    warnings = Column(Integer, default=0)
    status = Column(String(20))  # 'OPERATIONAL', 'DEGRADED'
    
    # Auto-fixes
    auto_fixes_applied = Column(Integer, default=0)
    auto_fixes_failed = Column(Integer, default=0)
    
    # Full results JSON
    results_json = Column(JSON)  # Complete diagnostic output
    
    # Context
    trigger = Column(String(50))  # 'startup', 'scheduled', 'manual', 'api'
    notes = Column(Text, nullable=True)
    
    __table_args__ = (
        Index('idx_diagnostic_run_at', 'run_at'),
        Index('idx_diagnostic_status', 'status'),
    )


# ============ Relationships ============

# Would add relationships here if needed:
# sounding = relationship("SoundingProfile", foreign_keys=[...])
# etc.
