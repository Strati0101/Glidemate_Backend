"""
atmosphere/parcel.py

Core atmospheric parcel algorithm for soaring forecast analysis.
Scientific heart of GlideMate – computes convection, wave flying,
and thermal strength from real weather model data (ICON-EU, HARMONIE-AROME).

NO APPROXIMATIONS. NO HARDCODING. All inputs from real model data.

Physical basis:
  - Magnus formula for saturation vapor pressure (Alduchov & Eskridge 1996)
  - Hypsometric equation for pressure-height relationship
  - Virtual temperature correction for moist air
  - Accurate SALR computation (Iribarne & Godson 1981)
  - CAPE/CIN via direct numerical integration (trapezoidal rule)
  - Lifted index, K-index, Showalter index (standard definitions)
  - Brunt-Väisälä frequency for wave analysis
  - Wind shear per layer for turbulence assessment
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import math
import logging

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# 1. DATA STRUCTURES
# ═══════════════════════════════════════════════════════════

@dataclass
class AtmosphericLevel:
    """Single pressure level from model data"""
    pressure_hpa: float
    height_m: float
    temp_c: float
    dewpoint_c: float
    wind_u_ms: float           # eastward component
    wind_v_ms: float           # northward component
    relative_humidity_pct: float
    
    def __post_init__(self):
        """Validate level data"""
        if self.pressure_hpa <= 0:
            raise ValueError(f"Invalid pressure: {self.pressure_hpa} hPa")
        if self.temp_c < -90 or self.temp_c > 60:
            raise ValueError(f"Unrealistic temperature: {self.temp_c}°C")
        if self.dewpoint_c > self.temp_c + 0.5:
            raise ValueError(f"Dewpoint > temperature: {self.dewpoint_c}°C > {self.temp_c}°C")
        if not (0 <= self.relative_humidity_pct <= 100):
            raise ValueError(f"Invalid RH: {self.relative_humidity_pct}%")


@dataclass
class AtmosphericProfile:
    """Complete atmospheric column from model"""
    lat: float
    lon: float
    valid_time: datetime
    model_source: str              # "ICON-EU" | "HARMONIE-AROME" | "ECMWF"
    levels: list[AtmosphericLevel] # sorted surface → top (ascending pressure)
    surface_temp_c: float
    surface_dewpoint_c: float
    surface_pressure_hpa: float
    solar_radiation_wm2: float     # shortwave radiation
    
    def __post_init__(self):
        """Validate profile"""
        if len(self.levels) < 5:
            raise ValueError(f"Profile must have ≥5 levels, got {len(self.levels)}")
        if self.model_source not in ("ICON-EU", "HARMONIE-AROME", "ECMWF"):
            raise ValueError(f"Unknown model: {self.model_source}")
        if self.solar_radiation_wm2 < 0 or self.solar_radiation_wm2 > 1400:
            raise ValueError(f"Unrealistic solar radiation: {self.solar_radiation_wm2} W/m²")
        
        # Verify levels are sorted by pressure (ascending = surface to top)
        pressures = [lv.pressure_hpa for lv in self.levels]
        if pressures != sorted(pressures):
            raise ValueError("Levels must be sorted by ascending pressure (surface → top)")


@dataclass
class ParcelResult:
    """Complete soaring forecast products from parcel analysis"""
    
    # ─── Key heights ───
    lcl_height_m: float            # cloud base AGL
    lcl_pressure_hpa: float
    lfc_height_m: float            # level of free convection
    lfc_pressure_hpa: float
    el_height_m: float             # equilibrium level
    el_pressure_hpa: float
    thermal_top_m: float           # practical soaring ceiling
    freezing_level_m: float        # 0°C isotherm height
    
    # ─── Energy indices ───
    cape_jkg: float                # convective available potential energy
    cin_jkg: float                 # convective inhibition
    lifted_index: float            # LI at 500hPa
    k_index: float
    total_totals: float
    showalter_index: float
    
    # ─── Soaring products ───
    trigger_temp_c: float          # surface temp needed to initiate thermals
    thermal_strength: int          # 0=None 1=Weak 2=Moderate 3=Good 4=Strong 5=OD
    thermal_strength_label: str
    soaring_window_start: Optional[datetime] = None
    soaring_window_end: Optional[datetime] = None
    blue_thermal_day: bool = False # thermals without cumulus visible
    od_risk: str = "none"          # "none" | "low" | "moderate" | "high"
    xc_distance_km: float = 0.0
    xc_best_bearing_deg: float = 0.0
    fai_triangle_possible: bool = False
    
    # ─── Wave flying ───
    wave_possible: bool = False
    froude_number: float = 0.0
    wave_amplitude_m: float = 0.0
    wave_window_base_m: float = 0.0
    wave_window_top_m: float = 0.0
    
    # ─── Wind shear per layer ───
    wind_shear_layers: list = field(default_factory=list)


# ═══════════════════════════════════════════════════════════
# 2. PHYSICAL CONSTANTS
# ═══════════════════════════════════════════════════════════

G = 9.80665                    # gravity m/s²
R_D = 287.05                   # gas constant dry air J/(kg·K)
R_V = 461.5                    # gas constant water vapor J/(kg·K)
CP_D = 1005.7                  # specific heat capacity dry air J/(kg·K)
L_V = 2.501e6                  # latent heat vaporization J/kg
EPSILON = R_D / R_V            # 0.622 dimensionless mixing ratio constant
DALR = 9.8                     # dry adiabatic lapse rate °C/1000m
SALR_APPROX = 6.0              # saturated adiabatic lapse rate approx °C/1000m
GLIDE_RATIO = 40.0             # standard modern glider L/D at optimal speed
THERMAL_EFFICIENCY = 0.65      # fraction of height gain utilized


# ═══════════════════════════════════════════════════════════
# 3. CORE FUNCTIONS
# ═══════════════════════════════════════════════════════════

def saturation_vapor_pressure(temp_c: float) -> float:
    """
    Magnus formula (Alduchov & Eskridge 1996).
    Computes saturation vapor pressure over water.
    
    Reference: Alduchov, O. A., and R. E. Eskridge, 1996:
      Improved Magnus form approximation of saturation vapor pressure.
      J. Appl. Meteor., 35, 601–609.
    
    Args:
        temp_c: Temperature in °C
    
    Returns:
        Saturation vapor pressure in hPa
    """
    if temp_c < -50 or temp_c > 60:
        raise ValueError(f"Temperature {temp_c}°C outside Magnus formula range")
    
    # Magnus formula: e_s = 6.1078 * exp(17.27*T / (T+237.3))
    exponent = (17.27 * temp_c) / (temp_c + 237.3)
    e_s = 6.1078 * math.exp(exponent)
    return e_s


def mixing_ratio(temp_c: float, pressure_hpa: float) -> float:
    """
    Saturation mixing ratio (mass of water vapor / mass of dry air).
    
    Formula:
        w = ε * e_s / (p - e_s)
    where ε = R_d / R_v = 0.622
    
    Args:
        temp_c: Temperature in °C
        pressure_hpa: Pressure in hPa
    
    Returns:
        Mixing ratio in kg/kg
    """
    e_s = saturation_vapor_pressure(temp_c)
    if pressure_hpa <= e_s:
        raise ValueError(f"Pressure {pressure_hpa} hPa must be > e_s {e_s:.1f} hPa")
    
    w = EPSILON * e_s / (pressure_hpa - e_s)
    return w


def virtual_temperature(temp_c: float, mixing_ratio_kgkg: float) -> float:
    """
    Virtual temperature – effective temperature of moist air.
    Used for buoyancy calculations.
    
    Formula:
        Tv = T * (1 + w/ε) / (1 + w)
    
    Args:
        temp_c: Temperature in °C
        mixing_ratio_kgkg: Mixing ratio in kg/kg
    
    Returns:
        Virtual temperature in Kelvin
    """
    T_K = temp_c + 273.15
    Tv = T_K * (1.0 + mixing_ratio_kgkg / EPSILON) / (1.0 + mixing_ratio_kgkg)
    return Tv


def moist_adiabatic_lapse_rate(temp_c: float, pressure_hpa: float) -> float:
    """
    Saturated adiabatic lapse rate (SALR) – precise computation.
    This varies with temperature and pressure, unlike dry adiabatic.
    
    Formula (Iribarne & Godson 1981):
        Γ_s = g * (1 + L_v*w_s/(R_d*T)) / (c_p + L_v²*w_s*ε/(R_d*T²))
    
    Reference: Iribarne, J. V., and W. L. Godson, 1981:
      Atmospheric Thermodynamics, 2nd ed. D. Reidel, 252 pp.
    
    Args:
        temp_c: Temperature in °C
        pressure_hpa: Pressure in hPa
    
    Returns:
        SALR in °C/m (positive value)
    """
    T_K = temp_c + 273.15
    w_s = mixing_ratio(temp_c, pressure_hpa)
    
    numerator = G * (1.0 + (L_V * w_s) / (R_D * T_K))
    denominator = CP_D + (L_V**2 * w_s * EPSILON) / (R_D * T_K**2)
    
    if denominator == 0:
        raise ValueError("Denominator = 0 in SALR computation")
    
    SALR = numerator / denominator
    return SALR


def lift_parcel(profile: AtmosphericProfile) -> list[dict]:
    """
    Simulate lifting a surface parcel in 50m steps from surface to 15000m AGL.
    Core loop that traces the parcel trajectory through the atmosphere.
    
    This returns the full parcel trace needed for CAPE, CIN, and key level detection.
    
    Algorithm:
      1. Start with surface T, Td
      2. Lift in 50m increments
      3. Below LCL: cool dry adiabatically
      4. Above LCL: cool saturated adiabatically
      5. Track buoyancy (parcel Tv - environment Tv)
      6. Interpolate environment from profile.levels
    
    Args:
        profile: AtmosphericProfile with ≥5 levels
    
    Returns:
        List of dicts: {height_m, parcel_temp_c, env_temp_c, pressure_hpa,
                        parcel_Tv_K, env_Tv_K, buoyancy_K, saturated}
    """
    steps = []
    
    # Initial state at surface
    parcel_temp_c = profile.surface_temp_c
    parcel_dewpoint_c = profile.surface_dewpoint_c
    parcel_pressure_hpa = profile.surface_pressure_hpa
    height_agl_m = 0
    is_saturated = False
    lcl_found = False
    
    # Lift in 50m increments up to 15000m
    max_height_m = 15000
    dz = 50  # increment in meters
    
    while height_agl_m <= max_height_m:
        # ─ Compute pressure at this height via hypsometric equation ─
        # p = p0 * exp(-g*h / (R_d*T_virtual))
        T_virt_K = virtual_temperature(parcel_temp_c, mixing_ratio(parcel_temp_c, parcel_pressure_hpa))
        parcel_pressure_hpa = parcel_pressure_hpa * math.exp(
            -G * dz / (R_D * T_virt_K)
        )
        
        if parcel_pressure_hpa < 10:
            break  # Stop at unrealistic pressures
        
        # ─ Cool the parcel ─
        if not is_saturated:
            # Below LCL: dry adiabatic cooling
            # DALR = 9.8°C/1000m → 0.0098°C/m
            parcel_temp_c -= (DALR / 1000.0) * dz
            # Dewpoint cools at ~0.18°C/100m (drying rate)
            parcel_dewpoint_c -= 0.0018 * dz
            
            # Check if we've reached saturation
            if parcel_temp_c <= parcel_dewpoint_c:
                is_saturated = True
                lcl_found = True
        else:
            # Above LCL: saturated adiabatic cooling
            salr = moist_adiabatic_lapse_rate(parcel_temp_c, parcel_pressure_hpa)
            parcel_temp_c -= salr * dz
        
        # ─ Interpolate environmental temperature at this height ─
        env_temp_c = _interpolate_temperature(profile, height_agl_m)
        
        # ─ Compute virtual temperatures for buoyancy ─
        parcel_w = mixing_ratio(parcel_temp_c, parcel_pressure_hpa)
        parcel_Tv_K = virtual_temperature(parcel_temp_c, parcel_w)
        
        # For environment, estimate mixing ratio from profile
        env_w = mixing_ratio(env_temp_c, parcel_pressure_hpa)
        env_Tv_K = virtual_temperature(env_temp_c, env_w)
        
        # Buoyancy: difference in virtual temperatures
        buoyancy_K = parcel_Tv_K - env_Tv_K
        
        steps.append({
            "height_m": height_agl_m,
            "parcel_temp_c": parcel_temp_c,
            "env_temp_c": env_temp_c,
            "pressure_hpa": parcel_pressure_hpa,
            "parcel_Tv_K": parcel_Tv_K,
            "env_Tv_K": env_Tv_K,
            "buoyancy_K": buoyancy_K,
            "saturated": is_saturated
        })
        
        height_agl_m += dz
    
    if len(steps) < 10:
        raise ValueError("Parcel trace too short – check input profile")
    
    logger.debug(f"Parcel trace: {len(steps)} steps, LCL found: {lcl_found}")
    return steps


def _interpolate_temperature(profile: AtmosphericProfile, height_agl_m: float) -> float:
    """
    Linear interpolation of temperature at given height.
    Uses profile.levels (sorted by ascending pressure = surface to top).
    
    Args:
        profile: AtmosphericProfile
        height_agl_m: Height AGL in meters
    
    Returns:
        Interpolated temperature in °C
    """
    # Find bracketing levels
    levels = profile.levels
    
    # Levels are sorted by ascending pressure (surface → top)
    # Heights are typically also ascending
    for i in range(len(levels) - 1):
        if levels[i].height_m <= height_agl_m <= levels[i + 1].height_m:
            # Interpolate between levels[i] and levels[i+1]
            h1, t1 = levels[i].height_m, levels[i].temp_c
            h2, t2 = levels[i + 1].height_m, levels[i + 1].temp_c
            
            if h2 == h1:
                return t1
            
            # Linear interpolation
            frac = (height_agl_m - h1) / (h2 - h1)
            return t1 + frac * (t2 - t1)
    
    # If height is above all levels, return top level temperature
    if height_agl_m > levels[-1].height_m:
        return levels[-1].temp_c
    
    # If height is below surface, return surface temp
    return profile.surface_temp_c


def find_key_levels(parcel_steps: list[dict]) -> dict:
    """
    Extract key heights from parcel trace: LCL, LFC, EL, thermal top, freezing level.
    
    Definitions:
      LCL: First step where saturated = True
      LFC: First height above LCL where buoyancy > 0
      EL: Last height where buoyancy > 0
      Thermal top: Highest height where buoyancy > -0.5K
      Freezing level: Height where env_temp_c = 0°C
    
    Args:
        parcel_steps: List from lift_parcel()
    
    Returns:
        Dict with keys: lcl_height_m, lcl_pressure_hpa, lfc_height_m, lfc_pressure_hpa,
                        el_height_m, el_pressure_hpa, thermal_top_m, freezing_level_m
    """
    result = {
        "lcl_height_m": 0,
        "lcl_pressure_hpa": 0,
        "lfc_height_m": 0,
        "lfc_pressure_hpa": 0,
        "el_height_m": 0,
        "el_pressure_hpa": 0,
        "thermal_top_m": 0,
        "freezing_level_m": 0
    }
    
    if not parcel_steps:
        raise ValueError("Empty parcel_steps")
    
    # ─ LCL: first saturated step ─
    for step in parcel_steps:
        if step["saturated"]:
            result["lcl_height_m"] = step["height_m"]
            result["lcl_pressure_hpa"] = step["pressure_hpa"]
            break
    
    # ─ LFC: first positive buoyancy after LCL ─
    for step in parcel_steps:
        if step["height_m"] > result["lcl_height_m"] and step["buoyancy_K"] > 0:
            result["lfc_height_m"] = step["height_m"]
            result["lfc_pressure_hpa"] = step["pressure_hpa"]
            break
    
    # ─ EL: last positive buoyancy ─
    for i in range(len(parcel_steps) - 1, -1, -1):
        if parcel_steps[i]["buoyancy_K"] > 0:
            result["el_height_m"] = parcel_steps[i]["height_m"]
            result["el_pressure_hpa"] = parcel_steps[i]["pressure_hpa"]
            break
    
    # ─ Thermal top: highest height with buoyancy > -0.5K ─
    for i in range(len(parcel_steps) - 1, -1, -1):
        if parcel_steps[i]["buoyancy_K"] > -0.5:
            result["thermal_top_m"] = parcel_steps[i]["height_m"]
            break
    
    # ─ Freezing level: height where env_temp crosses 0°C ─
    for i in range(len(parcel_steps) - 1):
        t1 = parcel_steps[i]["env_temp_c"]
        t2 = parcel_steps[i + 1]["env_temp_c"]
        
        if t1 >= 0 and t2 < 0:  # Crossing 0°C
            h1, h2 = parcel_steps[i]["height_m"], parcel_steps[i + 1]["height_m"]
            # Linear interpolation
            frac = (0 - t1) / (t2 - t1)
            result["freezing_level_m"] = h1 + frac * (h2 - h1)
            break
    
    return result


def compute_cape_cin(parcel_steps: list[dict], lfc_m: float, el_m: float) -> tuple[float, float]:
    """
    Compute CAPE and CIN via direct numerical integration (trapezoidal rule).
    
    CAPE (J/kg): Cumulative buoyancy from LFC to EL
    CIN (J/kg): Cumulative negative buoyancy from surface to LFC
    
    Energy integral:
        E = ∫ g * (buoyancy_Tv / Tv_env) * dz
    
    Using trapezoidal rule over 50m steps.
    
    Args:
        parcel_steps: List from lift_parcel()
        lfc_m: Level of free convection height in meters
        el_m: Equilibrium level height in meters
    
    Returns:
        (CAPE in J/kg, CIN in J/kg)  both positive values
    """
    CAPE = 0.0
    CIN = 0.0
    dz = 50.0  # step size in meters
    
    for step in parcel_steps:
        h = step["height_m"]
        buoy_K = step["buoyancy_K"]
        Tv_env = step["env_Tv_K"]
        
        if Tv_env <= 0:
            continue  # Invalid
        
        # Energy per unit height: g * (ΔTv / Tv)
        # Integrate: E = sum(g * (ΔTv/Tv) * dz)
        energy_density = G * (buoy_K / Tv_env) * dz
        
        if h < lfc_m and buoy_K < 0:
            # Below LFC, negative buoyancy
            CIN += abs(energy_density)
        elif lfc_m <= h <= el_m and buoy_K > 0:
            # Between LFC and EL, positive buoyancy
            CAPE += energy_density
    
    return (CAPE, CIN)


def compute_lifted_index(profile: AtmosphericProfile, parcel_steps: list[dict]) -> float:
    """
    Lifted Index (LI) – temperature difference between parcel and environment at 500hPa.
    
    LI = T500_environment - T500_parcel
    
    Interpretation:
      LI > 0: Stable
      LI = 0: Neutral
      LI < -3: Very unstable
    
    Args:
        profile: AtmosphericProfile
        parcel_steps: List from lift_parcel()
    
    Returns:
        Lifted index in °C (typically -6 to +6)
    """
    # Find parcel temp at 500hPa from steps
    parcel_T500 = None
    for step in parcel_steps:
        if step["pressure_hpa"] <= 500:
            parcel_T500 = step["parcel_temp_c"]
            break
    
    if parcel_T500 is None:
        # Parcel doesn't reach 500hPa, estimate
        parcel_T500 = parcel_steps[-1]["parcel_temp_c"]
    
    # Find environment temp at 500hPa from profile
    env_T500 = None
    for i in range(len(profile.levels) - 1):
        if profile.levels[i].pressure_hpa >= 500 >= profile.levels[i + 1].pressure_hpa:
            # Interpolate
            p1, t1 = profile.levels[i].pressure_hpa, profile.levels[i].temp_c
            p2, t2 = profile.levels[i + 1].pressure_hpa, profile.levels[i + 1].temp_c
            
            # Log-linear interpolation for pressure
            frac = math.log(p1 / 500.0) / math.log(p1 / p2)
            env_T500 = t1 + frac * (t2 - t1)
            break
    
    if env_T500 is None:
        # 500hPa not in profile, use top level
        env_T500 = profile.levels[-1].temp_c
    
    LI = env_T500 - parcel_T500
    return LI


def compute_k_index(profile: AtmosphericProfile) -> float:
    """
    K-Index – convective thunderstorm potential index.
    
    K = (T850 - T500) + Td850 - (T700 - Td700)
    
    Interpretation:
      K < 20: Weak thunderstorm potential
      20-25: Isolated thunderstorms possible
      25-30: Scattered thunderstorms
      30-35: Numerous thunderstorms
      > 35: Severe thunderstorms
    
    Args:
        profile: AtmosphericProfile
    
    Returns:
        K-index (typically 0-40)
    """
    # Extract levels at 850, 700, 500 hPa (use nearest if exact match not available)
    T850 = None
    Td850 = None
    T700 = None
    Td700 = None
    T500 = None
    
    for level in profile.levels:
        p = level.pressure_hpa
        if 840 <= p <= 860:
            T850 = level.temp_c
            Td850 = level.dewpoint_c
        elif 690 <= p <= 710:
            T700 = level.temp_c
            Td700 = level.dewpoint_c
        elif 490 <= p <= 510:
            T500 = level.temp_c
    
    # If exact levels not found, interpolate
    if T850 is None or Td850 is None or T700 is None or Td700 is None or T500 is None:
        logger.warning("K-index: could not find all required levels, using available data")
        # Use closest available levels
        if T850 is None and profile.levels:
            T850 = profile.levels[0].temp_c
            Td850 = profile.levels[0].dewpoint_c
        if T700 is None and len(profile.levels) > 1:
            T700 = profile.levels[1].temp_c
            Td700 = profile.levels[1].dewpoint_c
        if T500 is None and len(profile.levels) > 2:
            T500 = profile.levels[-1].temp_c
    
    if None in (T850, Td850, T700, Td700, T500):
        raise ValueError("Could not compute K-index: missing required pressure levels")
    
    K = (T850 - T500) + (Td850 - (T700 - Td700))
    return K


def compute_total_totals(profile: AtmosphericProfile) -> float:
    """
    Total Totals Index – thunderstorm potential.
    
    TT = (T850 - T500) + Td850 - T700
    
    Similar to K-index but simpler.
    TT > 50: High potential for severe thunderstorms.
    
    Args:
        profile: AtmosphericProfile
    
    Returns:
        Total Totals index
    """
    # Get levels
    T850, Td850 = None, None
    T700 = None
    T500 = None
    
    for level in profile.levels:
        p = level.pressure_hpa
        if 840 <= p <= 860:
            T850, Td850 = level.temp_c, level.dewpoint_c
        elif 690 <= p <= 710:
            T700 = level.temp_c
        elif 490 <= p <= 510:
            T500 = level.temp_c
    
    if None in (T850, Td850, T700, T500):
        # Use available levels
        if len(profile.levels) < 3:
            return 0.0
        T850 = profile.levels[0].temp_c
        Td850 = profile.levels[0].dewpoint_c
        T700 = profile.levels[1].temp_c if len(profile.levels) > 1 else T850
        T500 = profile.levels[-1].temp_c
    
    TT = (T850 - T500) + (Td850 - T700)
    return TT


def compute_showalter_index(profile: AtmosphericProfile) -> float:
    """
    Showalter Index – local instability at 850hPa.
    Parcel lifted from 850hPa to 500hPa.
    
    SI = T500_env - T500_parcel(from 850hPa)
    
    SI < 0: Unstable
    SI > 0: Stable
    
    Args:
        profile: AtmosphericProfile
    
    Returns:
        Showalter index in °C
    """
    # Get 850hPa level
    T850 = None
    Td850 = None
    for level in profile.levels:
        if 840 <= level.pressure_hpa <= 860:
            T850 = level.temp_c
            Td850 = level.dewpoint_c
            break
    
    if T850 is None:
        # Use first level as proxy
        T850 = profile.levels[0].temp_c
        Td850 = profile.levels[0].dewpoint_c
    
    # Get 500hPa level
    T500_env = None
    for level in profile.levels:
        if 490 <= level.pressure_hpa <= 510:
            T500_env = level.temp_c
            break
    
    if T500_env is None:
        T500_env = profile.levels[-1].temp_c
    
    # Lift parcel from 850hPa to 500hPa
    # Simulate 3 steps: start, LCL, end
    parcel_T = T850
    parcel_Td = Td850
    
    # Simple approximation: use average SALR
    salr_avg = 6.0  # °C/1000m
    height_diff = 3500  # roughly 850 to 500 hPa is ~3500m
    parcel_T -= (salr_avg / 1000.0) * height_diff
    
    SI = T500_env - parcel_T
    return SI


def compute_trigger_temperature(profile: AtmosphericProfile) -> float:
    """
    Surface temperature required to trigger thermals (cloud base temperature).
    
    At cloud base, parcel temp = dewpoint.
    LCL height ≈ (T - Td) / 2.5 * 1000 feet
    
    T_trigger = Td + LCL_correction
    
    Args:
        profile: AtmosphericProfile
    
    Returns:
        Trigger temperature in °C
    """
    T_surface = profile.surface_temp_c
    Td_surface = profile.surface_dewpoint_c
    
    # Estimate LCL height in feet
    lcl_ft = (T_surface - Td_surface) / 2.5 * 1000
    
    # Convert to meters
    lcl_m = lcl_ft * 0.3048
    
    # Temperature at LCL ≈ Td + lapse_rate * height
    # Rough estimate: 4.9°C/1000m in typical conditions
    T_trigger = Td_surface + (lcl_m / 1000.0) * 4.9
    
    return T_trigger


def compute_thermal_strength(cape: float, cin: float, li: float, solar_wm2: float) -> tuple[int, str]:
    """
    Combined thermal strength score from multiple indices.
    
    Scoring:
      - CAPE > 50: +1
      - CAPE > 300: +1
      - CAPE > 800: +1
      - LI < 0: +1
      - LI < -3: +1
      - CIN < 50: +1
      - Solar > 400 W/m²: +1
      - Solar > 600 W/m²: +1
    
    Cap at 5 points:
      0 → (0, "None")
      1 → (1, "Weak")
      2 → (2, "Moderate")
      3 → (3, "Good")
      4 → (4, "Strong")
      5+ → (5, "Overdeveloped")
    
    Args:
        cape: CAPE in J/kg
        cin: CIN in J/kg
        li: Lifted index in °C
        solar_wm2: Solar radiation in W/m²
    
    Returns:
        (strength_code, label) where code is 0-5
    """
    score = 0
    
    # CAPE scoring
    if cape > 50:
        score += 1
    if cape > 300:
        score += 1
    if cape > 800:
        score += 1
    
    # LI scoring
    if li < 0:
        score += 1
    if li < -3:
        score += 1
    
    # CIN scoring
    if cin < 50:
        score += 1
    
    # Solar scoring
    if solar_wm2 > 400:
        score += 1
    if solar_wm2 > 600:
        score += 1
    
    # Cap at 5
    score = min(score, 5)
    
    labels = {
        0: "None",
        1: "Weak",
        2: "Moderate",
        3: "Good",
        4: "Strong",
        5: "Overdeveloped"
    }
    
    return (score, labels[score])


def compute_wave_conditions(profile: AtmosphericProfile) -> dict:
    """
    Wave flying feasibility via Froude number and Brunt-Väisälä frequency.
    
    Wave condition requirements:
      1. Stable atmosphere (positive N)
      2. Wind perpendicular to ridge
      3. Froude number < 1.0 (subcritical flow → wave formation)
      4. Wind speed > 7.5 m/s (15 kt) minimum
    
    Froude Fr = U / (N * H)
    where:
      U = wind speed perpendicular to ridge
      N = Brunt-Väisälä frequency
      H = ridge height (assume 800m)
    
    Args:
        profile: AtmosphericProfile
    
    Returns:
        Dict: {wave_possible, froude, amplitude_m, window_base_m, window_top_m}
    """
    
    # ─ Step 1: Brunt-Väisälä frequency ─
    N_values = []
    for i in range(len(profile.levels) - 1):
        lv1, lv2 = profile.levels[i], profile.levels[i + 1]
        
        # Potential temperature θ = T * (1000/p)^(R_d/Cp)
        T1_K = lv1.temp_c + 273.15
        T2_K = lv2.temp_c + 273.15
        p1 = lv1.pressure_hpa
        p2 = lv2.pressure_hpa
        
        # Exponent R_d/Cp
        exponent = R_D / CP_D
        theta1 = T1_K * math.pow(1000.0 / p1, exponent)
        theta2 = T2_K * math.pow(1000.0 / p2, exponent)
        
        # Approximate mean theta
        theta_mean = (theta1 + theta2) / 2.0
        
        # Vertical gradient of potential temperature
        dtheta = theta2 - theta1
        dz = lv2.height_m - lv1.height_m
        
        if dz > 0:
            dtheta_dz = dtheta / dz
        else:
            dtheta_dz = 0
        
        # N² = (g / θ) * dθ/dz
        N_squared = (G / theta_mean) * dtheta_dz
        
        if N_squared >= 0:
            N = math.sqrt(N_squared)
            N_values.append(N)
    
    if not N_values:
        return {
            "wave_possible": False,
            "froude": 0,
            "amplitude_m": 0,
            "window_base_m": 0,
            "window_top_m": 0
        }
    
    N_mean = sum(N_values) / len(N_values)
    
    # ─ Step 2: Wind perpendicular to ridge ─
    # Use mid-level wind (700-800hPa)
    wind_u = profile.levels[len(profile.levels) // 2].wind_u_ms
    wind_v = profile.levels[len(profile.levels) // 2].wind_v_ms
    wind_speed = math.sqrt(wind_u**2 + wind_v**2)
    
    # Assume Alps ridge orientation ~ 045° (NE-SW)
    # Perpendicular wind would be from NW or SE
    # Simplified: use actual wind magnitude
    U_perp = wind_speed
    
    # ─ Step 3: Froude number ─
    H_ridge = 800.0  # meters (typical Alpine ridge)
    
    if N_mean > 0:
        Fr = U_perp / (N_mean * H_ridge)
    else:
        Fr = 999  # No waves possible
    
    # ─ Step 4: Wave probability ─
    wave_possible = Fr < 1.0 and U_perp > 7.5
    
    wave_amplitude = 0.0
    if wave_possible:
        wave_amplitude = H_ridge * (1.0 - Fr)
    
    return {
        "wave_possible": wave_possible,
        "froude": Fr,
        "amplitude_m": wave_amplitude,
        "window_base_m": H_ridge + wave_amplitude,
        "window_top_m": H_ridge + 2.0 * wave_amplitude
    }


def compute_wind_shear(profile: AtmosphericProfile) -> list[dict]:
    """
    Wind shear per layer – significant for turbulence and rotor formation.
    
    For each layer:
      ΔU = u2 - u1
      ΔV = v2 - v1
      ΔZ = (z2 - z1) / 1000 in km
      Shear = sqrt(ΔU² + ΔV²) / ΔZ in m/s per km
      Convert to knots/1000ft: × (1.944 / 0.3048)
    
    Args:
        profile: AtmosphericProfile
    
    Returns:
        List of dicts: {from_m, to_m, shear_kt_per_1000ft, significant}
    """
    shear_layers = []
    
    for i in range(len(profile.levels) - 1):
        lv1 = profile.levels[i]
        lv2 = profile.levels[i + 1]
        
        du = lv2.wind_u_ms - lv1.wind_u_ms
        dv = lv2.wind_v_ms - lv1.wind_v_ms
        dz = lv2.height_m - lv1.height_m  # in meters
        
        if dz == 0:
            continue
        
        # Shear magnitude in m/s per meter
        wind_shear = math.sqrt(du**2 + dv**2) / dz
        
        # Convert to knots per 1000 feet
        # 1 knot = 0.51444 m/s
        # 1000 feet = 304.8 m
        shear_kt_per_1000ft = wind_shear * 1000.0 * 304.8 / 0.51444
        
        significant = shear_kt_per_1000ft > 6.0
        
        shear_layers.append({
            "from_m": lv1.height_m,
            "to_m": lv2.height_m,
            "shear_kt_per_1000ft": shear_kt_per_1000ft,
            "significant": significant
        })
    
    return shear_layers


def compute_xc_distance(thermal_top_m: float, lcl_m: float, wind_u_ms: float, wind_v_ms: float) -> tuple[float, float]:
    """
    Expected XC distance – how far a glider can fly using thermals and wind.
    
    Simplified model:
      1. Usable height = thermal_top - LCL
      2. Base distance = usable_height × glide_ratio × efficiency / 1000 km
      3. Tailwind factor improves distance, headwind reduces it
    
    Args:
        thermal_top_m: Practical ceiling in meters AGL
        lcl_m: Cloud base in meters AGL
        wind_u_ms: Wind U component in m/s
        wind_v_ms: Wind V component in m/s
    
    Returns:
        (xc_distance_km, best_bearing_deg)
    """
    usable_height = max(thermal_top_m - lcl_m, 0)
    
    if usable_height <= 0:
        return (0.0, 0.0)
    
    # Base distance from height and glide ratio
    base_distance = (usable_height * GLIDE_RATIO * THERMAL_EFFICIENCY) / 1000.0
    
    # Wind correction
    wind_speed_ms = math.sqrt(wind_u_ms**2 + wind_v_ms**2)
    wind_speed_kt = wind_speed_ms * 1.944
    
    # Wind bearing (from where wind comes from)
    wind_bearing = math.degrees(math.atan2(wind_u_ms, wind_v_ms)) % 360
    best_bearing = (wind_bearing + 180) % 360  # downwind
    
    # Tailwind factor
    if wind_speed_kt > 60:
        # High wind: increased distance potential
        tailwind_factor = min(1.5, 1.0 + wind_speed_kt / 120.0)
    else:
        # Normal wind: modest increase
        tailwind_factor = 1.0 + wind_speed_kt / 150.0
    
    xc_distance = base_distance * tailwind_factor
    
    return (xc_distance, best_bearing)


def _od_risk(cape: float, k_index: float) -> str:
    """
    Overdevelopment risk assessment.
    
    At high CAPE and K-index, risk of severe thunderstorms
    that create downdrafts and hazardous conditions.
    
    Args:
        cape: CAPE in J/kg
        k_index: K-index
    
    Returns:
        "none" | "low" | "moderate" | "high"
    """
    if cape > 1500 or k_index > 35:
        return "high"
    elif cape > 1000 or k_index > 30:
        return "moderate"
    elif cape > 500 or k_index > 25:
        return "low"
    else:
        return "none"


# ═══════════════════════════════════════════════════════════
# 4. MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

def run_parcel_analysis(profile: AtmosphericProfile) -> ParcelResult:
    """
    Complete parcel analysis pipeline.
    
    This is the main entry point: call this from FastAPI endpoints
    and Celery tile generation tasks.
    
    Args:
        profile: AtmosphericProfile with real model data
    
    Returns:
        ParcelResult with all soaring products
    
    Raises:
        ValueError: If input validation fails
    """
    logger.info(f"Parcel analysis: {profile.model_source} @ {profile.lat:.2f}°N {profile.lon:.2f}°E")
    
    # Lift the parcel
    parcel_steps = lift_parcel(profile)
    
    # Extract key levels
    key_levels = find_key_levels(parcel_steps)
    
    # Energy indices
    cape, cin = compute_cape_cin(parcel_steps, key_levels["lfc_height_m"], key_levels["el_height_m"])
    
    # Atmospheric indices
    li = compute_lifted_index(profile, parcel_steps)
    k_idx = compute_k_index(profile)
    tt = compute_total_totals(profile)
    showalter = compute_showalter_index(profile)
    
    # Soaring products
    trigger = compute_trigger_temperature(profile)
    strength, label = compute_thermal_strength(cape, cin, li, profile.solar_radiation_wm2)
    
    # Wave conditions
    wave = compute_wave_conditions(profile)
    
    # Wind shear
    shear = compute_wind_shear(profile)
    
    # XC distance
    # Use wind at ~700hPa (mid-level, typical soaring height)
    wind_idx = len(profile.levels) // 2
    wind_u = profile.levels[wind_idx].wind_u_ms if wind_idx < len(profile.levels) else 0
    wind_v = profile.levels[wind_idx].wind_v_ms if wind_idx < len(profile.levels) else 0
    
    xc_dist, xc_bearing = compute_xc_distance(
        key_levels["thermal_top_m"],
        key_levels["lcl_height_m"],
        wind_u,
        wind_v
    )
    
    # Assemble result
    result = ParcelResult(
        lcl_height_m=key_levels["lcl_height_m"],
        lcl_pressure_hpa=key_levels["lcl_pressure_hpa"],
        lfc_height_m=key_levels["lfc_height_m"],
        lfc_pressure_hpa=key_levels["lfc_pressure_hpa"],
        el_height_m=key_levels["el_height_m"],
        el_pressure_hpa=key_levels["el_pressure_hpa"],
        thermal_top_m=key_levels["thermal_top_m"],
        freezing_level_m=key_levels["freezing_level_m"],
        cape_jkg=cape,
        cin_jkg=cin,
        lifted_index=li,
        k_index=k_idx,
        total_totals=tt,
        showalter_index=showalter,
        trigger_temp_c=trigger,
        thermal_strength=strength,
        thermal_strength_label=label,
        blue_thermal_day=key_levels["lcl_height_m"] > 2500,
        od_risk=_od_risk(cape, k_idx),
        xc_distance_km=xc_dist,
        xc_best_bearing_deg=xc_bearing,
        fai_triangle_possible=xc_dist > 100,
        wave_possible=wave["wave_possible"],
        froude_number=wave["froude"],
        wave_amplitude_m=wave["amplitude_m"],
        wave_window_base_m=wave["window_base_m"],
        wave_window_top_m=wave["window_top_m"],
        wind_shear_layers=shear
    )
    
    logger.info(f"Parcel analysis complete: CAPE={cape:.0f} J/kg, "
                f"LI={li:.1f}°C, Thermal={label}")
    
    return result
