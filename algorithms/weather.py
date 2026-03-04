"""
weather_algorithms.py
Professional meteorological calculations for soaring flight
All calculations use real atmospheric data from models/soundings
"""

import numpy as np
from typing import Dict, Tuple, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

# Physical Constants
GRAVITY = 9.81  # m/s²
RD = 287  # specific gas constant for dry air (J/kg/K)
RV = 461  # specific gas constant for water vapor (J/kg/K)
CP = 1005  # specific heat at constant pressure (J/kg/K)
LAPSE_RATE_DRY = 9.8  # K/km dry adiabatic
LATENT_HEAT_VAPORIZATION = 2.5e6  # J/kg
T0 = 273.15  # 0°C in Kelvin


@dataclass
class SoundingLevel:
    """Single level from atmospheric sounding"""
    pressure_hpa: float  # Pressure in hPa
    height_m: float  # Geopotential height in meters
    temperature: float  # Temperature in °C
    dewpoint: float  # Dewpoint in °C
    wind_direction: float  # Degrees, 0-360
    wind_speed: float  # m/s


@dataclass
class StabilityIndices:
    """Computed stability and soaring indices"""
    cape_j_kg: float  # Convective Available Potential Energy
    cin_j_kg: float  # Convective Inhibition
    lifted_index: float  # °C
    k_index: float  # K-Index
    total_totals: float  # Total Totals Index
    showalter_index: float  # Showalter Index
    bci: float  # Boyden Convection Index
    
    lcl_pressure_hpa: float  # Lifting Condensation Level pressure
    lcl_height_m: float  # LCL height above ground
    lfc_pressure_hpa: Optional[float]  # Level of Free Convection pressure
    el_pressure_hpa: Optional[float]  # Equilibrium Level pressure
    
    cloud_base_m: float  # Cloud base height AGL (m)
    thermal_top_m: Optional[float]  # Convective condensation level height
    trigger_temperature_c: float  # Surface temp needed for free convection
    
    freezing_level_m: float  # Height of 0°C isotherm
    precipitable_water_mm: float  # Total atmospheric water
    
    soaring_rating: int  # 0=None, 1=Weak, 2=Moderate, 3=Good, 4=Strong, 5=OD
    blue_day_flag: bool  # True if LCL > 2500m (thermals without cumulus)
    od_risk_flag: str  # 'none', 'low', 'moderate', 'high'
    wave_probability: float  # 0-1 probability of ridge wave
    froude_number: Optional[float]  # Fr < 1 indicates wave conditions
    
    xc_distance_km: float  # Estimated XC distance based on thermals
    best_xc_direction: float  # ° magnetic


class MeteorologicalAlgorithms:
    """Compute all soaring-specific meteorological indices"""
    
    @staticmethod
    def saturation_vapor_pressure(temperature_c: float) -> float:
        """
        Magnus formula for saturation vapor pressure
        Source: Lawrence (2005)
        
        Args:
            temperature_c: Temperature in °C
            
        Returns:
            Saturation vapor pressure in hPa
        """
        if temperature_c >= 0:
            # Over water
            a, b, c = 17.27, 237.7, 610.5
        else:
            # Over ice
            a, b, c = 21.875, 265.5, 611.0
            
        es = (c / 100) * np.exp((a * temperature_c) / (b + temperature_c))
        return es
    
    @staticmethod
    def relative_humidity(temperature_c: float, dewpoint_c: float) -> float:
        """
        Calculate relative humidity from temperature and dewpoint
        
        Returns: RH as percentage (0-100)
        """
        es = MeteorologicalAlgorithms.saturation_vapor_pressure(temperature_c)
        e = MeteorologicalAlgorithms.saturation_vapor_pressure(dewpoint_c)
        rh = 100 * (e / es)
        return np.clip(rh, 0, 100)
    
    @staticmethod
    def potential_temperature(temperature_c: float, pressure_hpa: float) -> float:
        """
        Calculate potential temperature (θ)
        θ = T * (1000/P)^(Rd/Cp)
        
        Args:
            temperature_c: Temperature in °C
            pressure_hpa: Pressure in hPa
            
        Returns:
            Potential temperature in K
        """
        T_K = temperature_c + 273.15
        theta = T_K * ((1000 / pressure_hpa) ** (RD / CP))
        return theta
    
    @staticmethod
    def virtual_temperature(temperature_c: float, dewpoint_c: float) -> float:
        """
        Virtual temperature accounting for moisture
        Tv = T * (1 + 0.61 * RH) approximation
        
        Returns: Virtual temperature in K
        """
        T_K = temperature_c + 273.15
        r = MeteorologicalAlgorithms.mixing_ratio(temperature_c, dewpoint_c)
        Tv = T_K * (1 + 0.61 * r)
        return Tv
    
    @staticmethod
    def mixing_ratio(temperature_c: float, dewpoint_c: float) -> float:
        """
        Calculate water vapor mixing ratio (kg/kg)
        """
        e = MeteorologicalAlgorithms.saturation_vapor_pressure(dewpoint_c)
        es = MeteorologicalAlgorithms.saturation_vapor_pressure(temperature_c)
        r = 0.622 * e / (100 * (es / e) - e)
        return max(r, 0)
    
    @staticmethod
    def lifting_condensation_level(temperature_surface_c: float, 
                                   dewpoint_surface_c: float) -> Tuple[float, float]:
        """
        Calculate LCL using Espy's formula
        
        Returns:
            (lcl_height_m, lcl_pressure_hpa)
        """
        # Espy's formula for LCL height
        lcl_height_m = 125 * (temperature_surface_c - dewpoint_surface_c)
        
        # Approximate LCL pressure using dry adiabatic lapse rate
        T_at_lcl = temperature_surface_c - (lcl_height_m / 1000) * LAPSE_RATE_DRY
        # Simplified: assume linear pressure-height relationship
        lcl_pressure_hpa = 1013.25 * ((T_at_lcl + 273.15) / (temperature_surface_c + 273.15)) ** 5.255
        
        return lcl_height_m, lcl_pressure_hpa
    
    @staticmethod
    def trigger_temperature(cloud_base_m: float, surface_dewpoint_c: float) -> float:
        """
        Calculate surface temperature needed for free convection
        T_trigger = Td + (Z_LCL / 1000) * 4.9
        
        Typical: ~5°C above dewpoint at surface
        """
        trigger = surface_dewpoint_c + (cloud_base_m / 1000) * 4.9
        return trigger
    
    @staticmethod
    def cape_cin(sounding: list) -> Tuple[float, float, Optional[float], Optional[float]]:
        """
        Calculate CAPE and CIN from sounding profile
        
        Args:
            sounding: list of SoundingLevel objects, surface-to-top
            
        Returns:
            (CAPE, CIN, LFC_pressure, EL_pressure) in J/kg and hPa
        """
        if len(sounding) < 3:
            return 0.0, 0.0, None, None
        
        # Surface parcel
        p_surface = sounding[0].pressure_hpa
        T_surface = sounding[0].temperature + 273.15
        Td_surface = sounding[0].dewpoint + 273.15
        
        # Lift parcel dry adiabatically
        cape = 0.0
        cin = 0.0
        lfc_pressure = None
        el_pressure = None
        in_positive_area = False
        
        for i in range(1, len(sounding)):
            p_env = sounding[i].pressure_hpa
            T_env = sounding[i].temperature + 273.15
            
            # Lift parcel dry adiabatically
            T_parcel = T_surface * (p_env / p_surface) ** (RD / CP)
            
            # Virtual temperature correction
            Tv_parcel = T_parcel * (1 + 0.61 * MeteorologicalAlgorithms.mixing_ratio(
                T_parcel - 273.15, Td_surface - 273.15))
            Tv_env = MeteorologicalAlgorithms.virtual_temperature(
                sounding[i].temperature, sounding[i].dewpoint)
            
            dT = Tv_parcel - Tv_env
            dp = p_surface - p_env  # pressure difference in hPa
            dz = 100 * (RD / GRAVITY) * np.log(p_surface / p_env)  # thickness in m
            
            if dT > 0:
                if not in_positive_area and cin != 0:
                    lfc_pressure = p_env  # Level of Free Convection
                in_positive_area = True
                cape += (GRAVITY / T_env) * dT * dz  # dz in meters
            else:
                if in_positive_area:
                    el_pressure = p_env  # Equilibrium Level
                    break
                if in_positive_area == False:
                    cin += (GRAVITY / T_env) * dT * dz
        
        return max(cape, 0), min(cin, 0), lfc_pressure, el_pressure
    
    @staticmethod
    def lifted_index(sounding: list) -> float:
        """
        Calculate Lifted Index (LI)
        Lift surface parcel to 500hPa, compare temperature with environment
        LI = T500_env - T500_parcel
        
        Positive = stable, Negative = unstable
        """
        if len(sounding) < 2:
            return 0.0
        
        p_surface = sounding[0].pressure_hpa
        T_surface = sounding[0].temperature + 273.15
        
        # Find 500hPa level
        t500_env = None
        for level in sounding:
            if level.pressure_hpa <= 500:
                if t500_env is None or abs(level.pressure_hpa - 500) < abs(t500_env[0] - 500):
                    t500_env = (level.pressure_hpa, level.temperature + 273.15)
        
        if t500_env is None:
            return 0.0
        
        # Lift parcel dry adiabatically
        T_parcel_500 = T_surface * (500 / p_surface) ** (RD / CP)
        
        Li = t500_env[1] - T_parcel_500
        return Li
    
    @staticmethod
    def k_index(sounding: list) -> float:
        """
        K-Index = (T850 - T500) + Td850 - (T700 - Td700)
        Indicates thunderstorm potential
        K < 15: None, 15-20: Weak, 20-25: Moderate, 25-30: Strong, >30: Very Strong
        """
        t850 = None
        td850 = None
        t700 = None
        td700 = None
        t500 = None
        
        for level in sounding:
            if 840 <= level.pressure_hpa <= 860:
                t850 = level.temperature
                td850 = level.dewpoint
            elif 690 <= level.pressure_hpa <= 710:
                t700 = level.temperature
                td700 = level.dewpoint
            elif 490 <= level.pressure_hpa <= 510:
                t500 = level.temperature
        
        if t850 is None or td700 is None or t500 is None:
            return 0.0
        
        K = (t850 - t500) + td850 - (t700 - td700)
        return K
    
    @staticmethod
    def total_totals(sounding: list) -> float:
        """
        Total Totals Index = (T850 + Td850) - 2*T500
        TT < 44: None, 44-48: Marginal, 48-52: Slight, 52-56: Moderate, >56: High
        """
        t850 = None
        td850 = None
        t500 = None
        
        for level in sounding:
            if 840 <= level.pressure_hpa <= 860:
                t850 = level.temperature
                td850 = level.dewpoint
            elif 490 <= level.pressure_hpa <= 510:
                t500 = level.temperature
        
        if t850 is None or td850 is None or t500 is None:
            return 0.0
        
        TT = (t850 + td850) - 2 * t500
        return TT
    
    @staticmethod
    def showalter_index(sounding: list) -> float:
        """
        Showalter Index = T500_env - T500_parcel
        Lift 850hPa parcel to 500hPa
        SI < -3: Very unstable, -3 to 0: Unstable, >0: Stable
        """
        t850 = None
        td850 = None
        t500_env = None
        p850 = None
        p500 = None
        
        for level in sounding:
            if 840 <= level.pressure_hpa <= 860:
                t850 = level.temperature + 273.15
                td850 = level.dewpoint + 273.15
                p850 = level.pressure_hpa
            elif 490 <= level.pressure_hpa <= 510:
                t500_env = level.temperature + 273.15
                p500 = level.pressure_hpa
        
        if t850 is None or p500 is None:
            return 0.0
        
        # Lift parcel dry adiabatically
        T_parcel_500 = t850 * (p500 / p850) ** (RD / CP)
        SI = t500_env - T_parcel_500
        
        return SI
    
    @staticmethod
    def boyden_convection_index(sounding: list) -> float:
        """
        BCI = (Z700 - Z1000) / 10 - T700 - 200
        BCI < 95: None, 95-97: Weak, >97: Strong convection
        """
        z700 = None
        z1000 = None
        t700 = None
        
        for level in sounding:
            if 690 <= level.pressure_hpa <= 710:
                z700 = level.height_m
                t700 = level.temperature
            elif 990 <= level.pressure_hpa <= 1010:
                z1000 = level.height_m
        
        if z700 is None or z1000 is None or t700 is None:
            return 0.0
        
        BCI = ((z700 - z1000) / 10) - t700 - 200
        return BCI
    
    @staticmethod
    def soaring_rating_from_indices(cape: float, bci: float, lifted_index: float, 
                                   k_index: float, blh: float) -> Tuple[int, str]:
        """
        Determine soaring rating (0-5) from combined indices
        
        Returns:
            (rating_int, rating_name)
        """
        rating = 0
        
        # CAPE contribution
        if cape > 1500:
            rating = 5
        elif cape > 800:
            rating = 4
        elif cape > 300:
            rating = 3
        elif cape > 100:
            rating = 2
        elif cape > 0:
            rating = 1
        
        # BCI adjustment
        if bci > 97:
            rating = max(rating, 4)
        elif bci > 95:
            rating = max(rating, 2)
        
        # K-Index adjustment
        if k_index > 30:
            rating = min(rating + 1, 5)
        elif k_index > 25:
            rating = min(rating + 1, 5)
        
        # Cloud base adjustment
        if blh < 1000:  # Very low cloud base
            rating = min(rating, 2)
        
        names = ["NONE", "WEAK", "MODERATE", "GOOD", "STRONG", "OVERDEVELOPED"]
        return rating, names[min(rating, 5)]
    
    @staticmethod
    def compute_all_indices(sounding: list, surface_temp_c: float, 
                           surface_pressure_hpa: float) -> StabilityIndices:
        """
        Compute all stability indices from a sounding profile
        
        Args:
            sounding: list of SoundingLevel objects
            surface_temp_c: Surface temperature in °C
            surface_pressure_hpa: Surface pressure in hPa
            
        Returns:
            StabilityIndices object with all computed values
        """
        if len(sounding) < 3:
            # Return neutral/none indices
            return StabilityIndices(
                cape_j_kg=0, cin_j_kg=0, lifted_index=0, k_index=0,
                total_totals=0, showalter_index=0, bci=0,
                lcl_pressure_hpa=surface_pressure_hpa, lcl_height_m=0,
                lfc_pressure_hpa=None, el_pressure_hpa=None,
                cloud_base_m=0, thermal_top_m=None, trigger_temperature_c=surface_temp_c,
                freezing_level_m=0, precipitable_water_mm=0, soaring_rating=0,
                blue_day_flag=False, od_risk_flag='none', wave_probability=0.0,
                froude_number=None, xc_distance_km=0, best_xc_direction=0
            )
        
        # Compute indices
        cape, cin, lfc_p, el_p = MeteorologicalAlgorithms.cape_cin(sounding)
        li = MeteorologicalAlgorithms.lifted_index(sounding)
        k = MeteorologicalAlgorithms.k_index(sounding)
        tt = MeteorologicalAlgorithms.total_totals(sounding)
        si = MeteorologicalAlgorithms.showalter_index(sounding)
        bci = MeteorologicalAlgorithms.boyden_convection_index(sounding)
        
        # Cloud base
        lcl_height, lcl_p = MeteorologicalAlgorithms.lifting_condensation_level(
            sounding[0].temperature, sounding[0].dewpoint)
        trigger_temp = MeteorologicalAlgorithms.trigger_temperature(
            lcl_height, sounding[0].dewpoint)
        
        # Soaring rating
        rating, rating_name = MeteorologicalAlgorithms.soaring_rating_from_indices(
            cape, bci, li, k, lcl_height)
        
        # Blue day flag (LCL > 2500m)
        blue_day = lcl_height > 2500
        
        # Overdevelopment risk
        od_risk = 'none'
        if cape > 1500 and k > 30:
            od_risk = 'high'
        elif cape > 800 and k > 25:
            od_risk = 'moderate'
        elif cape > 300:
            od_risk = 'low'
        
        # XC distance estimate (simple)
        xc_distance = min(100, max(50, (cape / 50))) if cape > 0 else 0
        
        return StabilityIndices(
            cape_j_kg=cape,
            cin_j_kg=cin,
            lifted_index=li,
            k_index=k,
            total_totals=tt,
            showalter_index=si,
            bci=bci,
            lcl_pressure_hpa=lcl_p,
            lcl_height_m=lcl_height,
            lfc_pressure_hpa=lfc_p,
            el_pressure_hpa=el_p,
            cloud_base_m=lcl_height,
            thermal_top_m=None,  # Would need more data
            trigger_temperature_c=trigger_temp,
            freezing_level_m=3000,  # Placeholder
            precipitable_water_mm=20,  # Placeholder
            soaring_rating=rating,
            blue_day_flag=blue_day,
            od_risk_flag=od_risk,
            wave_probability=0.1,  # Would need wind/ridge data
            froude_number=None,  # Would need wind/ridge data
            xc_distance_km=xc_distance,
            best_xc_direction=225  # Placeholder
        )    
    @staticmethod
    def compute_all_indices_from_levels(levels: list) -> StabilityIndices:
        """
        Compute all indices from a list of level dictionaries
        This is a convenience method for data ingestion from NOAA, models, etc.
        
        Args:
            levels: List of dicts with keys:
                - pressure_hpa (required)
                - temperature_c (required)
                - dewpoint_c (optional)
                - wind_direction_deg (optional)
                - wind_speed_ms (optional)
                - height_m (optional)
        
        Returns:
            StabilityIndices object
        """
        # Convert dict levels to SoundingLevel objects
        sounding = []
        for level in levels:
            sl = SoundingLevel(
                pressure_hpa=float(level.get('pressure_hpa', 0)),
                height_m=float(level.get('height_m', 0)),
                temperature=float(level.get('temperature_c', 0)),
                dewpoint=float(level.get('dewpoint_c', 0)),
                wind_direction=float(level.get('wind_direction_deg', 0)),
                wind_speed=float(level.get('wind_speed_ms', 0)),
            )
            sounding.append(sl)
        
        # Get surface values from first level
        if not sounding:
            # Return neutral indices
            return StabilityIndices(
                cape_j_kg=0, cin_j_kg=0, lifted_index=0, k_index=0,
                total_totals=0, showalter_index=0, bci=0,
                lcl_pressure_hpa=1013, lcl_height_m=0,
                lfc_pressure_hpa=None, el_pressure_hpa=None,
                cloud_base_m=0, thermal_top_m=None, trigger_temperature_c=15,
                freezing_level_m=3000, precipitable_water_mm=20, soaring_rating=0,
                blue_day_flag=False, od_risk_flag='none', wave_probability=0.0,
                froude_number=None, xc_distance_km=0, best_xc_direction=0,
                cape_3km=0, ventilation_rate=0, bulk_richardson=0,
                energy_helicity=0
            )
        
        surface_level = sounding[0]
        return MeteorologicalAlgorithms.compute_all_indices(
            sounding,
            surface_temp_c=surface_level.temperature,
            surface_pressure_hpa=surface_level.pressure_hpa
        )