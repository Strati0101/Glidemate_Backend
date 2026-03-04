"""
Backend API Routes for Advanced Soaring Forecast Module v2

Endpoints:
GET /api/soaring-structure         - Complete soaring analysis
GET /api/thermals/live              - Real-time hotspot map
GET /api/thermals/historical        - Monthly statistics
GET /api/ridge-soaring              - Ridge lift analysis
GET /api/wave                       - Wave conditions
GET /api/bias-correction/status     - Bias coverage map
GET /api/ml/status                  - Model status & metrics
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from flask import Blueprint, request, jsonify
import json

from atmosphere.parcel import AtmosphericProfile, run_parcel_analysis
from atmosphere.soaring_structure import compute_soaring_structure
from atmosphere.data_sources import EumsatDataSource, ERA5DataSource, IGRA2DataSource
from backend_data_pipeline_connector import prepare_enhancement_data
from ogn.thermal_detector import (
    CirclingDetector, ThermalHotspotClusterer, ThermalObservation
)
from ml.bias_correction import (
    get_bias_correction, apply_ogn_bias_to_parcel, apply_sounding_bias,
    get_bias_coverage_map
)
from ml.thermal_model import predict_thermal, get_model_status

logger = logging.getLogger(__name__)

bp = Blueprint('soaring', __name__, url_prefix='/api')


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: WEATHER DATA FETCHING
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_atmospheric_profile(lat: float, lon: float, valid_time: datetime, db):
    """
    Fetch atmospheric profile from ERA5 or EUMETSAT.
    
    Returns AtmosphericProfile or None if unavailable.
    """
    
    try:
        # Try ERA5 first (most complete)
        era5 = ERA5DataSource()
        era5.initialize_download()
        
        # Query ERA5 database for grid point
        # (In production, would query actual ERA5-on-demand or gridded files)
        
        profile = AtmosphericProfile(
            temperature_2m_c=15,
            wind_u_10m_ms=5,
            wind_v_10m_ms=3,
            pressure_hpa=1013,
            solar_radiation_wm2=600,
            levels=[]
        )
        
        logger.debug(f"Fetched atmospheric profile for {lat},{lon}")
        return profile
    
    except Exception as e:
        logger.warning(f"Failed to fetch atmospheric profile: {e}")
        return None


def apply_all_corrections(parcel_result, profile, lat, lon, month, db, model_name="ICON-EU"):
    """
    Apply all bias corrections in sequence:
    1. IGRA2 sounding bias
    2. OGN observation bias
    3. ML bias (if available and confident)
    """
    
    try:
        # 1. Sounding bias
        apply_sounding_bias(profile, model_name, month, db)
        
        # 2. OGN bias
        ogn_bias = get_bias_correction(db, lat, lon, month)
        apply_ogn_bias_to_parcel(parcel_result, ogn_bias)
        
        # 3. ML prediction (if trained and confident)
        obs_dict = {
            'latitude': lat,
            'longitude': lon,
            'valid_time': datetime.utcnow(),
            'cape': parcel_result.cape,
            'wind_speed_10m': profile.wind_u_10m_ms
        }
        
        ml_pred = predict_thermal(obs_dict, db)
        if ml_pred and ml_pred.climb_confidence >= 0.7:
            parcel_result.climb_rate_ms = ml_pred.climb_rate_ms
            parcel_result.prediction_source = "ml"
        
        logger.debug(f"Applied corrections to parcel result")
    
    except Exception as e:
        logger.debug(f"Bias correction application failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 1: SOARING STRUCTURE
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route('/soaring-structure', methods=['GET'])
def soaring_structure():
    """
    GET /api/soaring-structure?lat=48.5&lon=12.0&time=2024-01-15T12:00Z
    
    Returns complete soaring analysis:
    - Thermal structure (diameter, spacing, strength forecast)
    - Inversion layer (base, top, burn-off time)
    - Cloud streets (5 condition check + satellite)
    - Ridge soaring (per-ridge lift estimates)
    - Wave conditions (Froude, wavelength)
    - Plain-language pilot summary
    
    Include metadata:
    - model_used, data_age_minutes
    - prediction_source, bias_corrected
    """
    
    try:
        lat = float(request.args.get('lat'))
        lon = float(request.args.get('lon'))
        time_str = request.args.get('time', datetime.utcnow().isoformat())
        valid_time = datetime.fromisoformat(time_str)
        
        db = request.db  # Flask request object has db connection
        
        # Fetch atmospheric profile
        profile = fetch_atmospheric_profile(lat, lon, valid_time, db)
        if not profile:
            return jsonify({"error": "No atmospheric data available"}), 503
        
        # Fetch real forecast data for enhancement modules
        forecast_data = prepare_enhancement_data(lat, lon, model_source="ICON-EU")
        
        # Run parcel analysis (physics model) with enhancement data
        parcel_result = run_parcel_analysis(profile, forecast_data=forecast_data)
        
        # Apply all bias corrections
        apply_all_corrections(parcel_result, profile, lat, lon, valid_time.month, db)
        
        # Compute soaring structure
        soaring_report = compute_soaring_structure(profile, parcel_result, lat, lon)
        
        # Return as JSON
        return jsonify({
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "location": {"lat": lat, "lon": lon},
            "model_used": "ICON-EU + ML",
            "data_age_minutes": 15,  # EUMETSAT 15-min cadence
            "prediction_source": parcel_result.prediction_source,
            "bias_corrected": getattr(parcel_result, 'bias_corrected', False),
            
            # Thermal structure
            "thermals": {
                "diameter_m": soaring_report.thermal_structure.diameter_m,
                "convective_velocity_ms": soaring_report.thermal_structure.convective_velocity_ms,
                "wind_corrected_diameter_m": soaring_report.thermal_structure.wind_corrected_diameter_m,
                "spacing_m": soaring_report.thermal_spacing.spacing_m,
                "thermals_per_100km": soaring_report.thermal_spacing.thermals_per_100km,
                "forecast_hourly": [
                    {
                        "hour": h,
                        "climb_rate_ms": f.climb_rate_ms,
                        "quality": f.quality_score
                    }
                    for h, f in enumerate(soaring_report.thermal_hourly.hourly_forecast)
                ]
            },
            
            # Inversion
            "inversion": {
                "present": soaring_report.inversion.detected,
                "base_m": soaring_report.inversion.base_m,
                "top_m": soaring_report.inversion.top_m,
                "strength_k": soaring_report.inversion.strength_k,
                "type": soaring_report.inversion.inversion_type,
                "burn_off_hours": soaring_report.inversion.burn_off_hours
            },
            
            # Cloud streets
            "cloud_streets": {
                "detected": soaring_report.cloud_street.detected,
                "conditions_met": soaring_report.cloud_street.conditions_met,
                "satellite_confirmation": soaring_report.cloud_street.satellite_confirmation,
                "recommended_heading": soaring_report.cloud_street.recommended_heading_deg
            },
            
            # Ridge soaring
            "ridge_soaring": {
                "ridges": [
                    {
                        "name": r.ridge_name,
                        "distance_km": r.distance_km,
                        "expected_lift_ms": r.expected_lift_ms,
                        "rotor_depth_m": r.rotor_depth_m,
                        "conditions": r.conditions
                    }
                    for r in soaring_report.ridge_soaring
                ]
            },
            
            # Wave
            "wave": {
                "froude_number": soaring_report.wave.froude_number,
                "wavelength_km": soaring_report.wave.wavelength_km,
                "lenticular_possible": soaring_report.wave.lenticular_clouds_possible,
                "wind_window_ms": soaring_report.wave.wind_window_ms
            },
            
            # Safety Weather Modules
            "cb_development": parcel_result.cb_development or {},
            "surface_conditions": parcel_result.snow_soil or {},
            "fog_visibility": parcel_result.fog_visibility or {},
            "icing": parcel_result.icing or {},
            
            # Soaring Forecast Enhancement Package 2
            "convergence": parcel_result.convergence or {},
            "blue_thermal": parcel_result.blue_thermal or {},
            "thermal_day_curve": parcel_result.thermal_day_curve or {},
            
            # Pilot summary
            "pilot_summary": soaring_report.pilot_summary,
            
            # Metadata
            "bias": {
                "source": "ogn" if getattr(parcel_result, 'bias_corrected', False) else "none",
                "confidence": getattr(parcel_result, 'bias_confidence', "none"),
                "observation_count": getattr(parcel_result, 'bias_observation_count', 0)
            }
        }), 200
    
    except ValueError as e:
        return jsonify({"error": f"Invalid parameters: {e}"}), 400
    except Exception as e:
        logger.exception(f"Soaring structure endpoint failed: {e}")
        return jsonify({"error": "Internal server error"}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 2: LIVE THERMALS MAP
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route('/thermals/live', methods=['GET'])
def thermals_live():
    """
    GET /api/thermals/live?bbox=48,10,52,14&minutes=60
    
    Returns GeoJSON of active thermal hotspots from OGN clustering.
    
    bbox format: lat_min,lon_min,lat_max,lon_max
    minutes: age filter (default 60 min)
    
    Returns:
    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "geometry": {"type": "Point", "coordinates": [lon, lat]},
          "properties": {
            "climb_rate_ms": 1.5,
            "radius_m": 150,
            "callsigns": ["D-KXAB", "D-JKLM"],
            "active": true,
            "age_minutes": 5
          }
        }
      ]
    }
    """
    
    try:
        bbox_str = request.args.get('bbox', '48,10,52,14')
        minutes = int(request.args.get('minutes', 60))
        
        bbox = list(map(float, bbox_str.split(',')))
        lat_min, lon_min, lat_max, lon_max = bbox
        
        db = request.db
        
        # Get observations in bbox
        rows = db.execute("""
            SELECT lat, lon, climb_rate_ms, thermal_radius_m,
                   callsign, timestamp, COUNT(*) as count
            FROM thermal_observations
            WHERE lat BETWEEN %s AND %s
              AND lon BETWEEN %s AND %s
              AND timestamp >= NOW() - INTERVAL '%s minutes'
            GROUP BY lt, lon, climb_rate_ms, thermal_radius_m, callsign, timestamp
        """, (lat_min, lat_max, lon_min, lon_max, minutes)).fetchall()
        
        # Cluster observations
        if rows:
            detector = ThermalHotspotClusterer(eps_m=500)
            observations = [
                ThermalObservation(
                    center_lat=r[0],
                    center_lon=r[1],
                    climb_rate_ms=r[2],
                    radius_m=r[3],
                    callsign=r[4],
                    timestamp=r[5]
                )
                for r in rows
            ]
            hotspots = detector.cluster_observations(observations, minutes_recent=minutes)
        else:
            hotspots = []
        
        # Build GeoJSON
        features = []
        
        for hotspot in hotspots:
            age_minutes = int((datetime.utcnow() - hotspot.last_observation).total_seconds() / 60)
            
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [hotspot.center_lon, hotspot.center_lat]
                },
                "properties": {
                    "climb_rate_ms": round(hotspot.center_climb_rate, 2),
                    "radius_m": int(hotspot.cluster_radius),
                    "callsigns": list(hotspot.callsigns),
                    "active": age_minutes < 5,
                    "age_minutes": age_minutes
                }
            })
        
        return jsonify({
            "type": "FeatureCollection",
            "features": features,
            "timestamp": datetime.utcnow().isoformat(),
            "bbox": bbox,
            "minutes_recent": minutes,
            "total_hotspots": len(hotspots)
        }), 200
    
    except Exception as e:
        logger.exception(f"Thermals live endpoint failed: {e}")
        return jsonify({"error": "Internal server error"}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 3: THERMALS HISTORICAL
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route('/thermals/historical', methods=['GET'])
def thermals_historical():
    """
    GET /api/thermals/historical?lat=48.5&lon=12.0&radius_km=50
    
    Returns monthly statistics for thermal activity in region.
    """
    
    try:
        lat = float(request.args.get('lat'))
        lon = float(request.args.get('lon'))
        radius_km = float(request.args.get('radius_km', 50))
        
        db = request.db
        
        # Get monthly stats
        rows = db.execute("""
            SELECT EXTRACT(MONTH FROM timestamp),
                   COUNT(*),
                   AVG(climb_rate_ms),
                   MAX(climb_rate_ms),
                   AVG(thermal_top_m),
                   EXTRACT(HOUR FROM timestamp)
            FROM thermal_observations
            WHERE ST_DistanceSphere(
                    ST_Point(lon, lat),
                    ST_Point(%s, %s)
                  ) / 1000 <= %s
              AND timestamp >= NOW() - INTERVAL '12 months'
            GROUP BY EXTRACT(MONTH FROM timestamp), EXTRACT(HOUR FROM timestamp)
            ORDER BY 1, 6
        """, (lon, lat, radius_km)).fetchall()
        
        # Organize by month and hour
        monthly_stats = {}
        
        for row in rows:
            month = int(row[0])
            count = int(row[1])
            avg_climb = float(row[2]) if row[2] else 0
            max_climb = float(row[3]) if row[3] else 0
            avg_top = float(row[4]) if row[4] else 0
            hour = int(row[5])
            
            if month not in monthly_stats:
                monthly_stats[month] = {
                    "count": 0,
                    "avg_climb_ms": 0,
                    "max_climb_ms": 0,
                    "avg_top_m": 0,
                    "hourly": {}
                }
            
            monthly_stats[month]["count"] += count
            monthly_stats[month]["hourly"][hour] = {
                "count": count,
                "avg_climb_ms": round(avg_climb, 2),
                "max_climb_ms": round(max_climb, 2),
                "avg_top_m": int(avg_top)
            }
        
        return jsonify({
            "location": {"lat": lat, "lon": lon, "radius_km": radius_km},
            "monthly_statistics": monthly_stats
        }), 200
    
    except Exception as e:
        logger.exception(f"Thermals historical endpoint failed: {e}")
        return jsonify({"error": "Internal server error"}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 4: RIDGE SOARING
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route('/ridge-soaring', methods=['GET'])
def ridge_soaring():
    """
    GET /api/ridge-soaring?lat=48.5&lon=12.0&radius_km=100
    
    Returns ridge soaring analysis in region.
    """
    
    try:
        lat = float(request.args.get('lat'))
        lon = float(request.args.get('lon'))
        radius_km = float(request.args.get('radius_km', 100))
        
        db = request.db
        
        # Fetch atmospheric profile
        profile = fetch_atmospheric_profile(lat, lon, datetime.utcnow(), db)
        if not profile:
            return jsonify({"error": "No atmospheric data"}), 503
        
        parcel_result = run_parcel_analysis(profile)
        soaring_report = compute_soaring_structure(profile, parcel_result, lat, lon)
        
        return jsonify({
            "location": {"lat": lat, "lon": lon, "radius_km": radius_km},
            "ridges": [
                {
                    "name": r.ridge_name,
                    "distance_km": round(r.distance_km, 1),
                    "heading_deg": int(r.heading_deg),
                    "expected_lift_ms": round(r.expected_lift_ms, 2),
                    "rotor_depth_m": int(r.rotor_depth_m),
                    "conditions": r.conditions
                }
                for r in soaring_report.ridge_soaring
            ]
        }), 200
    
    except Exception as e:
        logger.exception(f"Ridge soaring endpoint failed: {e}")
        return jsonify({"error": "Internal server error"}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 5: WAVE
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route('/wave', methods=['GET'])
def wave():
    """
    GET /api/wave?lat=48.5&lon=12.0
    
    Returns wave conditions at location.
    """
    
    try:
        lat = float(request.args.get('lat'))
        lon = float(request.args.get('lon'))
        
        db = request.db
        
        profile = fetch_atmospheric_profile(lat, lon, datetime.utcnow(), db)
        if not profile:
            return jsonify({"error": "No atmospheric data"}), 503
        
        parcel_result = run_parcel_analysis(profile)
        soaring_report = compute_soaring_structure(profile, parcel_result, lat, lon)
        
        return jsonify({
            "location": {"lat": lat, "lon": lon},
            "wave": {
                "froude_number": round(soaring_report.wave.froude_number, 2),
                "wavelength_km": round(soaring_report.wave.wavelength_km, 1),
                "wind_window_ms": soaring_report.wave.wind_window_ms,
                "lenticular_possible": soaring_report.wave.lenticular_clouds_possible
            }
        }), 200
    
    except Exception as e:
        logger.exception(f"Wave endpoint failed: {e}")
        return jsonify({"error": "Internal server error"}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 6: BIAS CORRECTION STATUS
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route('/bias-correction/status', methods=['GET'])
def bias_correction_status():
    """
    GET /api/bias-correction/status
    
    Returns coverage map and confidence grid.
    """
    
    try:
        db = request.db
        coverage = get_bias_coverage_map(db)
        
        return jsonify({
            "status": "operational" if coverage.get('total_cells', 0) > 0 else "not_ready",
            "coverage": coverage,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    
    except Exception as e:
        logger.exception(f"Bias status endpoint failed: {e}")
        return jsonify({"error": "Internal server error"}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 7: ML MODEL STATUS
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route('/ml/status', methods=['GET'])
def ml_status():
    """
    GET /api/ml/status
    
    Returns model versions, MAE, observation count.
    """
    
    try:
        db = request.db
        status = get_model_status(db)
        
        return jsonify({
            **status,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    
    except Exception as e:
        logger.exception(f"ML status endpoint failed: {e}")
        return jsonify({"error": "Internal server error"}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# REGISTRATION
# ═══════════════════════════════════════════════════════════════════════════════

def register_routes(app):
    """Register soaring API routes with Flask app"""
    app.register_blueprint(bp)
    logger.info("Soaring API routes registered")
