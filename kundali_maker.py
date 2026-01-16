from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

import swisseph as swe


# -----------------------------
# Config / Helpers
# -----------------------------

SIGNS = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"
]

SIGNS_SANSKRIT = [
    "Mesha", "Vrishabha", "Mithuna", "Karka", "Simha", "Kanya",
    "Tula", "Vrishchika", "Dhanu", "Makara", "Kumbha", "Meena"
]

PLANETS = {
    "Sun": swe.SUN,
    "Moon": swe.MOON,
    "Mars": swe.MARS,
    "Mercury": swe.MERCURY,
    "Jupiter": swe.JUPITER,
    "Venus": swe.VENUS,
    "Saturn": swe.SATURN,
    "Rahu": swe.MEAN_NODE,
    "Uranus": swe.URANUS,
    "Neptune": swe.NEPTUNE,
    "Pluto": swe.PLUTO,
}

PLANET_SYMBOLS = {
    "Sun": "Su",
    "Moon": "Mo",
    "Mars": "Ma",
    "Mercury": "Me",
    "Jupiter": "Ju",
    "Venus": "Ve",
    "Saturn": "Sa",
    "Rahu": "Ra",
    "Ketu": "Ke",
    "Asc": "As",
    "Uranus": "Ur",
    "Neptune": "Ne",
    "Pluto": "Pl",
    "Mandi": "Ma",
    "Gulika": "Gk",
    "Dhuma": "Dh",
    "Vyatipata": "Vy",
    "Parivesha": "Pv",
    "Indrachapa": "Ic",
    "Upaketu": "Uk",
}

# Exaltation signs (0-indexed): planet -> sign_index
EXALTATION = {
    "Sun": 0,      # Aries
    "Moon": 1,     # Taurus
    "Mars": 9,     # Capricorn
    "Mercury": 5,  # Virgo
    "Jupiter": 3,  # Cancer
    "Venus": 11,   # Pisces
    "Saturn": 6,   # Libra
    "Rahu": 1,     # Taurus
    "Ketu": 7,     # Scorpio
}

# Debilitation signs (opposite of exaltation)
DEBILITATION = {
    "Sun": 6,      # Libra
    "Moon": 7,     # Scorpio
    "Mars": 3,     # Cancer
    "Mercury": 11, # Pisces
    "Jupiter": 9,  # Capricorn
    "Venus": 5,    # Virgo
    "Saturn": 0,   # Aries
    "Rahu": 7,     # Scorpio
    "Ketu": 1,     # Taurus
}

# Combustion orbs (degrees from Sun)
COMBUST_ORBS = {
    "Moon": 12,
    "Mars": 17,
    "Mercury": 14,  # 12 if retrograde
    "Jupiter": 11,
    "Venus": 10,    # 8 if retrograde
    "Saturn": 15,
}

# Vimshottari Dasha periods (in years)
DASHA_PERIODS = {
    "Ketu": 7,
    "Venus": 20,
    "Sun": 6,
    "Moon": 10,
    "Mars": 7,
    "Rahu": 18,
    "Jupiter": 16,
    "Saturn": 19,
    "Mercury": 17,
}

# Dasha planet order for Vimshottari system
DASHA_ORDER = [
    "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury"
]

# Total Vimshottari cycle is 120 years
VIMSHOTTARI_CYCLE = 120


def norm_deg(x: float) -> float:
    x = x % 360.0
    if x < 0:
        x += 360.0
    return x


def calculate_upagrahas(sun_lon: float, jd_ut: float, lat: float, lon: float) -> Dict[str, Dict]:
    """
    Calculate Upagrahas (sub-planets/shadow planets) based on Sun's longitude.
    
    Upagrahas calculated:
    - Dhuma: Sun + 133°20'
    - Vyatipata: 360° - Dhuma (or 53°20' - Sun)
    - Parivesha: Vyatipata + 180°
    - Indrachapa (Kodanda): 360° - Parivesha
    - Upaketu: Indrachapa + 16°40'
    - Mandi/Gulika: Based on Saturn's portion of day/night (requires sunrise calculation)
    """
    upagrahas = {}
    
    # Dhuma = Sun + 133°20' (133.333...)
    dhuma_lon = norm_deg(sun_lon + 133.0 + 20.0/60.0)
    
    # Vyatipata = 360° - Dhuma (equivalent to 53°20' - Sun, but normalized)
    vyatipata_lon = norm_deg(360.0 - dhuma_lon)
    
    # Parivesha = Vyatipata + 180°
    parivesha_lon = norm_deg(vyatipata_lon + 180.0)
    
    # Indrachapa (Kodanda) = 360° - Parivesha
    indrachapa_lon = norm_deg(360.0 - parivesha_lon)
    
    # Upaketu = Indrachapa + 16°40' (16.666...)
    upaketu_lon = norm_deg(indrachapa_lon + 16.0 + 40.0/60.0)
    
    # Mandi/Gulika calculation (simplified - based on weekday and Saturn's portion)
    # Mandi is calculated based on the portion of Saturn in the day/night
    # For simplicity, we use an approximation based on birth time
    weekday = int(jd_ut + 1.5) % 7  # 0=Sunday
    
    # Saturn's portion order for each weekday (day births)
    # Sunday=8th, Monday=7th, Tuesday=6th, Wednesday=5th, Thursday=4th, Friday=3rd, Saturday=2nd
    saturn_day_portions = [8, 7, 6, 5, 4, 3, 2]
    # For night births: Sunday=2nd, Monday=1st, Tuesday=7th, etc.
    saturn_night_portions = [2, 1, 7, 6, 5, 4, 3]
    
    # Get approximate sunrise/sunset (simplified: 6am/6pm)
    # In a full implementation, you'd calculate actual sunrise
    birth_hour = (jd_ut % 1) * 24  # Approximate hour from JD fraction
    is_day = 6 <= birth_hour < 18
    
    if is_day:
        portion = saturn_day_portions[weekday]
        day_length = 12.0  # hours (simplified)
        portion_duration = day_length / 8.0
        mandi_time = 6.0 + (portion - 1) * portion_duration  # Start of Saturn's portion
    else:
        portion = saturn_night_portions[weekday]
        night_length = 12.0
        portion_duration = night_length / 8.0
        if birth_hour >= 18:
            mandi_time = 18.0 + (portion - 1) * portion_duration
        else:
            mandi_time = (portion - 1) * portion_duration
    
    # Calculate Mandi longitude based on Lagna at Mandi time
    # Simplified: use Sun's longitude + offset based on portion
    mandi_offset = (portion - 1) * 30.0 + 15.0  # Approximate
    mandi_lon = norm_deg(sun_lon + mandi_offset)
    
    # Gulika is often considered same as Mandi or slightly different
    # Some traditions place Gulika at the start of Saturn's portion, Mandi at the middle
    gulika_lon = norm_deg(mandi_lon - 7.5)  # Slight offset
    
    # Build upagraha data
    for name, ulon in [
        ("Dhuma", dhuma_lon),
        ("Vyatipata", vyatipata_lon),
        ("Parivesha", parivesha_lon),
        ("Indrachapa", indrachapa_lon),
        ("Upaketu", upaketu_lon),
        ("Mandi", mandi_lon),
        ("Gulika", gulika_lon),
    ]:
        sign_idx = deg_to_sign_index(ulon)
        sign_name, d, m, s = deg_to_sign_deg(ulon)
        navamsa_sign = get_navamsa_sign(ulon)
        
        upagrahas[name] = {
            "longitude": round(ulon, 4),
            "sign": sign_name,
            "sign_sanskrit": SIGNS_SANSKRIT[sign_idx],
            "sign_index": sign_idx,
            "navamsa_sign_index": navamsa_sign,
            "navamsa_sign": SIGNS[navamsa_sign],
            "navamsa_sign_sanskrit": SIGNS_SANSKRIT[navamsa_sign],
            "deg": d,
            "min": m,
            "sec": round(s, 2),
            "symbol": PLANET_SYMBOLS.get(name, name[:2]),
        }
    
    return upagrahas


def get_navamsa_sign(longitude: float) -> int:
    """
    Get Navamsa (D9) sign index from longitude.
    
    Navamsa rules:
    - Each sign is divided into 9 navamsas, each 3°20' (3.333...°)
    - Fire signs (Aries, Leo, Sag): Navamsa cycle starts from Aries
    - Earth signs (Taurus, Virgo, Cap): Navamsa cycle starts from Capricorn
    - Air signs (Gemini, Libra, Aqu): Navamsa cycle starts from Libra
    - Water signs (Cancer, Scorpio, Pisces): Navamsa cycle starts from Cancer
    """
    lon = norm_deg(longitude)
    sign_index = int(lon // 30)
    degree_in_sign = lon % 30
    navamsa_pada = int(degree_in_sign / (30.0 / 9.0))  # 0-8, which navamsa within the sign
    
    # Determine starting sign based on element
    element = sign_index % 4  # 0=Fire, 1=Earth, 2=Air, 3=Water
    if element == 0:  # Fire (Aries, Leo, Sag)
        start_sign = 0  # Aries
    elif element == 1:  # Earth (Taurus, Virgo, Cap)
        start_sign = 9  # Capricorn
    elif element == 2:  # Air (Gemini, Libra, Aqu)
        start_sign = 6  # Libra
    else:  # Water (Cancer, Scorpio, Pisces)
        start_sign = 3  # Cancer
    
    navamsa_sign = (start_sign + navamsa_pada) % 12
    return navamsa_sign


def is_combust(planet_name: str, planet_lon: float, sun_lon: float, is_retrograde: bool) -> bool:
    """Check if planet is combust (too close to Sun)."""
    if planet_name not in COMBUST_ORBS:
        return False
    orb = COMBUST_ORBS[planet_name]
    if planet_name == "Mercury" and is_retrograde:
        orb = 12
    elif planet_name == "Venus" and is_retrograde:
        orb = 8
    diff = abs(norm_deg(planet_lon - sun_lon))
    if diff > 180:
        diff = 360 - diff
    return diff <= orb


def deg_to_sign_index(lon: float) -> int:
    """0..11"""
    return int(norm_deg(lon) // 30)


def deg_to_sign_deg(lon: float) -> Tuple[str, int, int, float]:
    """Return (sign, deg, min, sec_float) inside sign."""
    lon = norm_deg(lon)
    s = deg_to_sign_index(lon)
    within = lon - 30 * s
    d = int(within)
    m_float = (within - d) * 60
    m = int(m_float)
    sec = (m_float - m) * 60
    return SIGNS[s], d, m, sec


def whole_sign_house(lagna_sign_index: int, planet_sign_index: int) -> int:
    """Whole sign house number 1..12 from Lagna sign."""
    diff = (planet_sign_index - lagna_sign_index) % 12
    return diff + 1


@dataclass
class BirthInput:
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int
    tz_offset_hours: float
    latitude: float
    longitude: float
    ephe_path: str = "./ephe"
    ayanamsha: int = swe.SIDM_LAHIRI
    use_utc: bool = False  # If True, the time is already in UTC


# -----------------------------
# Core Kundali Maker
# -----------------------------

def is_dst_observed(year: int, month: int, day: int, latitude: float, longitude: float, base_tz_offset: float) -> bool:
    """
    Determine if DST was likely observed on the given date based on location and timezone.
    Returns True if DST adjustment should be applied.
    """
    def nth_weekday_of_month(year_: int, month_: int, weekday: int, n: int) -> int:
        """Return day-of-month for the n-th `weekday` (Mon=0..Sun=6) in a month."""
        first = datetime(year_, month_, 1)
        first_wd = first.weekday()
        delta = (weekday - first_wd) % 7
        return 1 + delta + (n - 1) * 7

    def last_weekday_of_month(year_: int, month_: int, weekday: int) -> int:
        """Return day-of-month for the last `weekday` (Mon=0..Sun=6) in a month."""
        if month_ == 12:
            next_month = datetime(year_ + 1, 1, 1)
        else:
            next_month = datetime(year_, month_ + 1, 1)
        last_day = (next_month - timedelta(days=1)).day
        last = datetime(year_, month_, last_day)
        last_wd = last.weekday()
        delta = (last_wd - weekday) % 7
        return last_day - delta

    def date_tuple(y: int, m: int, d: int) -> tuple[int, int, int]:
        return (y, m, d)

    # Simple DST detection for common regions
    # Note: This is a simplified approach - for production, consider using a proper timezone library
    
    # USA DST detection (rough approximation for most US locations)
    if -125 <= longitude <= -65 and 25 <= latitude <= 49:  # Continental US bounds
        # We evaluate by date only (no time-of-day). On the exact transition Sundays,
        # times before/after 2:00 AM local can differ by 1 hour.
        cur = date_tuple(year, month, day)

        if year >= 2007:
            # 2nd Sunday in March -> 1st Sunday in November
            start_day = nth_weekday_of_month(year, 3, weekday=6, n=2)  # Sunday
            end_day = nth_weekday_of_month(year, 11, weekday=6, n=1)   # Sunday
            start = date_tuple(year, 3, start_day)
            end = date_tuple(year, 11, end_day)
            return start <= cur < end

        if 1987 <= year <= 2006:
            # 1st Sunday in April -> last Sunday in October
            start_day = nth_weekday_of_month(year, 4, weekday=6, n=1)
            end_day = last_weekday_of_month(year, 10, weekday=6)
            start = date_tuple(year, 4, start_day)
            end = date_tuple(year, 10, end_day)
            return start <= cur < end

        # Before 1987: last Sunday in April -> last Sunday in October
        start_day = last_weekday_of_month(year, 4, weekday=6)
        end_day = last_weekday_of_month(year, 10, weekday=6)
        start = date_tuple(year, 4, start_day)
        end = date_tuple(year, 10, end_day)
        return start <= cur < end
    
    # Europe DST detection (UK, Germany, etc.)
    if -10 <= longitude <= 40 and 35 <= latitude <= 70:  # European bounds
        cur = date_tuple(year, month, day)
        start_day = last_weekday_of_month(year, 3, weekday=6)  # last Sunday in March
        end_day = last_weekday_of_month(year, 10, weekday=6)   # last Sunday in October
        start = date_tuple(year, 3, start_day)
        end = date_tuple(year, 10, end_day)
        return start <= cur < end
    
    # Canada (similar to US)
    if -140 <= longitude <= -50 and 40 <= latitude <= 70:  # Canadian bounds
        if 2007 <= year:
            if (month == 3 and day >= 14) or (4 <= month <= 10) or (month == 11 and day <= 7):
                return True
        elif 1987 <= year <= 2006:
            if 4 <= month <= 10:
                return True
    
    # Australia (southern hemisphere, opposite seasons)
    if 110 <= longitude <= 155 and -45 <= latitude <= -10:  # Australian bounds
        # Australian DST: October to April (varies by state)
        # Simplified: October 1 to April 7
        if (month == 10 and day >= 1) or (11 <= month <= 3) or (month == 4 and day <= 7):
            return True
    
    # New Zealand
    if 165 <= longitude <= 180 and -48 <= latitude <= -34:  # NZ bounds
        # NZ DST: Last Sunday in September to First Sunday in April
        # Simplified: September 25 to April 7
        if (month == 9 and day >= 25) or (10 <= month <= 3) or (month == 4 and day <= 7):
            return True
    
    return False


def get_standard_tz_offset(latitude: float, longitude: float) -> float:
    """
    Get the standard (non-DST) timezone offset for a location.
    Returns the standard timezone offset in hours.
    """
    # US timezone boundaries (approximate)
    if 25 <= latitude <= 49 and -125 <= longitude <= -65:
        if longitude >= -67:  # Atlantic (rare in continental US)
            return -4.0
        elif longitude >= -82:  # Eastern
            return -5.0
        elif longitude >= -90:  # Central (approximate boundary)
            return -6.0
        elif longitude >= -105:  # Mountain (approximate boundary)
            return -7.0
        else:  # Pacific
            return -8.0
    
    # India
    if 8 <= latitude <= 37 and 68 <= longitude <= 97:
        return 5.5
    
    # UK/Ireland
    if 49 <= latitude <= 61 and -11 <= longitude <= 2:
        return 0.0
    
    # Western Europe (CET)
    if 35 <= latitude <= 71 and -10 <= longitude <= 17:
        return 1.0
    
    # Eastern Europe (EET)
    if 35 <= latitude <= 71 and 17 <= longitude <= 40:
        return 2.0
    
    # Default: use longitude-based approximation
    return round(longitude / 15.0)


def adjust_for_dst(year: int, month: int, day: int, latitude: float, longitude: float, base_tz_offset: float) -> float:
    """
    Adjust timezone offset for DST if applicable.
    Returns the correct timezone offset for the given date and location.
    
    Strategy:
    1. For regions without DST (India, China, etc.), trust the user's input
    2. For DST regions (US, Europe), normalize based on location and date
    """
    # India: IST (UTC+5:30), no DST - trust user input if it's 5.5
    if 6 <= latitude <= 38 and 68 <= longitude <= 98:
        # User likely entered IST, trust it
        if abs(base_tz_offset - 5.5) < 0.1:
            return 5.5
        return base_tz_offset
    
    # Nepal: UTC+5:45, no DST
    if 26 <= latitude <= 31 and 80 <= longitude <= 89:
        if abs(base_tz_offset - 5.75) < 0.1:
            return 5.75
        return base_tz_offset
    
    # China, Japan, Korea, Southeast Asia - no DST, trust user input
    if 0 <= latitude <= 55 and 97 <= longitude <= 145:
        return base_tz_offset
    
    # Middle East (UAE, Saudi, etc.) - no DST
    if 12 <= latitude <= 42 and 34 <= longitude <= 63:
        return base_tz_offset
    
    # Russia - no DST since 2014
    if 41 <= latitude <= 82 and 27 <= longitude <= 180:
        return base_tz_offset
    
    # For DST regions, normalize the timezone
    standard_tz = get_standard_tz_offset(latitude, longitude)
    
    # Check if DST should be observed on this date
    is_dst = is_dst_observed(year, month, day, latitude, longitude, base_tz_offset)
    
    # Special cases where DST is not observed despite being in DST-observing regions
    # Arizona (except Navajo Nation)
    if -115 <= longitude <= -109 and 31 <= latitude <= 37:
        is_dst = False
    
    # Hawaii doesn't observe DST
    if -160 <= longitude <= -154 and 18 <= latitude <= 23:
        is_dst = False
    
    if is_dst:
        # DST adds 1 hour to standard time
        return standard_tz + 1.0
    else:
        return standard_tz


def convert_to_ist(
    year: int, month: int, day: int, hour: int, minute: int, second: int,
    tz_offset: float, latitude: float, longitude: float
) -> dict:
    """
    Convert any local time to IST (UTC+5:30) with DST adjustment from source location.
    
    Steps:
    1. Apply DST adjustment for the source location
    2. Convert local time to UTC
    3. Convert UTC to IST (+5:30)
    
    Returns dict with IST year, month, day, hour, minute, second and the effective offset used.
    """
    IST_OFFSET = 5.5
    
    # Step 1: Adjust for DST at source location
    adjusted_tz = adjust_for_dst(year, month, day, latitude, longitude, tz_offset)
    dst_applied = adjusted_tz != tz_offset
    
    # Step 2: Convert local time to UTC
    local_decimal_hours = hour + minute / 60 + second / 3600
    utc_decimal_hours = local_decimal_hours - adjusted_tz

    utc_y, utc_m, utc_d = year, month, day
    
    # Handle day rollover for UTC
    while utc_decimal_hours < 0:
        utc_decimal_hours += 24
        dt = datetime(utc_y, utc_m, utc_d, tzinfo=timezone.utc) - timedelta(days=1)
        utc_y, utc_m, utc_d = dt.year, dt.month, dt.day

    while utc_decimal_hours >= 24:
        utc_decimal_hours -= 24
        dt = datetime(utc_y, utc_m, utc_d, tzinfo=timezone.utc) + timedelta(days=1)
        utc_y, utc_m, utc_d = dt.year, dt.month, dt.day

    utc_hour = int(utc_decimal_hours)
    utc_minute = int((utc_decimal_hours - utc_hour) * 60)
    utc_second = int(((utc_decimal_hours - utc_hour) * 60 - utc_minute) * 60)
    
    # Step 3: Convert UTC to IST
    ist_decimal_hours = utc_decimal_hours + IST_OFFSET
    ist_y, ist_m, ist_d = utc_y, utc_m, utc_d

    # Handle day rollover for IST
    while ist_decimal_hours < 0:
        ist_decimal_hours += 24
        dt = datetime(ist_y, ist_m, ist_d, tzinfo=timezone.utc) - timedelta(days=1)
        ist_y, ist_m, ist_d = dt.year, dt.month, dt.day

    while ist_decimal_hours >= 24:
        ist_decimal_hours -= 24
        dt = datetime(ist_y, ist_m, ist_d, tzinfo=timezone.utc) + timedelta(days=1)
        ist_y, ist_m, ist_d = dt.year, dt.month, dt.day
    
    ist_hour = int(ist_decimal_hours)
    ist_minute = int((ist_decimal_hours - ist_hour) * 60)
    ist_second = int(((ist_decimal_hours - ist_hour) * 60 - ist_minute) * 60)
    
    return {
        "year": ist_y,
        "month": ist_m,
        "day": ist_d,
        "hour": ist_hour,
        "minute": ist_minute,
        "second": ist_second,
        "tz_offset": IST_OFFSET,
        "original_tz_offset": tz_offset,
        "dst_adjusted_tz_offset": adjusted_tz,
        "dst_applied": dst_applied,
        "utc_year": utc_y,
        "utc_month": utc_m,
        "utc_day": utc_d,
        "utc_hour": utc_hour,
        "utc_minute": utc_minute,
        "utc_second": utc_second,
    }


def compute_julian_day_local(b: BirthInput, adjusted_tz_offset: float = None) -> float:
    """
    Convert local date+time with fixed tz offset into UT Julian day.
    If use_utc is True, the input time is already in UTC.
    Uses provided adjusted_tz_offset if given, otherwise computes it.
    """
    local_decimal_hours = b.hour + b.minute / 60 + b.second / 3600
    
    if b.use_utc:
        # Time is already in UTC, no conversion needed
        ut_decimal_hours = local_decimal_hours
        y, m, d = b.year, b.month, b.day
    else:
        # Use provided adjusted timezone offset or compute it
        if adjusted_tz_offset is not None:
            tz_to_use = adjusted_tz_offset
        else:
            tz_to_use = adjust_for_dst(b.year, b.month, b.day, b.latitude, b.longitude, b.tz_offset_hours)
        
        ut_decimal_hours = local_decimal_hours - tz_to_use
        y, m, d = b.year, b.month, b.day

    while ut_decimal_hours < 0:
        ut_decimal_hours += 24
        dt = datetime(y, m, d)
        dt2 = dt.replace(tzinfo=timezone.utc)
        dt_prev = dt2.timestamp() - 86400
        prev = datetime.fromtimestamp(dt_prev, tz=timezone.utc)
        y, m, d = prev.year, prev.month, prev.day

    while ut_decimal_hours >= 24:
        ut_decimal_hours -= 24
        dt = datetime(y, m, d, tzinfo=timezone.utc).timestamp() + 86400
        nxt = datetime.fromtimestamp(dt, tz=timezone.utc)
        y, m, d = nxt.year, nxt.month, nxt.day

    jd_ut = swe.julday(y, m, d, ut_decimal_hours)
    return jd_ut


def local_to_utc(year: int, month: int, day: int, hour: int, minute: int, second: int, tz_offset: float) -> dict:
    """
    Convert local time to UTC.
    Returns dict with UTC year, month, day, hour, minute, second.
    """
    local_decimal_hours = hour + minute / 60 + second / 3600
    ut_decimal_hours = local_decimal_hours - tz_offset
    
    y, m, d = year, month, day
    
    while ut_decimal_hours < 0:
        ut_decimal_hours += 24
        dt = datetime(y, m, d, tzinfo=timezone.utc) - timedelta(days=1)
        y, m, d = dt.year, dt.month, dt.day
    
    while ut_decimal_hours >= 24:
        ut_decimal_hours -= 24
        dt = datetime(y, m, d, tzinfo=timezone.utc) + timedelta(days=1)
        y, m, d = dt.year, dt.month, dt.day
    
    ut_hour = int(ut_decimal_hours)
    ut_minute = int((ut_decimal_hours - ut_hour) * 60)
    ut_second = int(((ut_decimal_hours - ut_hour) * 60 - ut_minute) * 60)
    
    return {
        "year": y,
        "month": m,
        "day": d,
        "hour": ut_hour,
        "minute": ut_minute,
        "second": ut_second,
    }


def utc_to_local(year: int, month: int, day: int, hour: int, minute: int, second: int, tz_offset: float) -> dict:
    """
    Convert UTC time to local time.
    Returns dict with local year, month, day, hour, minute, second.
    """
    utc_decimal_hours = hour + minute / 60 + second / 3600
    local_decimal_hours = utc_decimal_hours + tz_offset
    
    y, m, d = year, month, day
    
    while local_decimal_hours < 0:
        local_decimal_hours += 24
        dt = datetime(y, m, d, tzinfo=timezone.utc) - timedelta(days=1)
        y, m, d = dt.year, dt.month, dt.day
    
    while local_decimal_hours >= 24:
        local_decimal_hours -= 24
        dt = datetime(y, m, d, tzinfo=timezone.utc) + timedelta(days=1)
        y, m, d = dt.year, dt.month, dt.day
    
    local_hour = int(local_decimal_hours)
    local_minute = int((local_decimal_hours - local_hour) * 60)
    local_second = int(((local_decimal_hours - local_hour) * 60 - local_minute) * 60)
    
    return {
        "year": y,
        "month": m,
        "day": d,
        "hour": local_hour,
        "minute": local_minute,
        "second": local_second,
    }


def compute_lagna(jd_ut: float, lat: float, lon: float) -> float:
    """
    Compute Ascendant longitude (tropical) using Swiss Ephemeris houses.
    """
    cusps, ascmc = swe.houses_ex(jd_ut, lat, lon, b'P')
    asc_tropical = ascmc[0]
    return asc_tropical


def calculate_bhava_bala(
    planets_out: Dict,
    lagna_sign: int,
    lagna_longitude: float,
    rasi_chart: Dict,
    shad_bala: Dict,
) -> Dict:
    """
    Calculate Bhava Bala (house strength) following AstroSage/B.V. Raman methodology.
    
    Bhava Bala consists of:
    1. Bhavadhipati Bala - Strength of house lord
    2. Bhava Digbala - Directional strength of house
    3. Bhava Drishti Bala - Aspectual strength on house
    4. Bhava Residential Strength - Planets' position within house
    """
    bhava_bala = {}
    
    # Sign rulers
    SIGN_RULERS = {
        0: "Mars", 1: "Venus", 2: "Mercury", 3: "Moon",
        4: "Sun", 5: "Mercury", 6: "Venus", 7: "Mars",
        8: "Jupiter", 9: "Saturn", 10: "Saturn", 11: "Jupiter"
    }
    
    # Benefic and malefic planets
    BENEFICS = ("Jupiter", "Venus", "Mercury", "Moon")
    MALEFICS = ("Sun", "Mars", "Saturn", "Rahu", "Ketu")
    
    def normalize(deg: float) -> float:
        d = deg % 360.0
        return d if d >= 0 else d + 360.0
    
    def angular_distance(a: float, b: float) -> float:
        d = abs(normalize(a) - normalize(b))
        return min(d, 360.0 - d)
    
    def get_house_lord(house_num: int) -> str:
        """Get the lord of a house based on lagna sign."""
        house_sign = (lagna_sign + house_num - 1) % 12
        return SIGN_RULERS.get(house_sign, "")
    
    def get_house_midpoint(house_num: int) -> float:
        """Get the midpoint longitude of a house (whole sign)."""
        house_sign = (lagna_sign + house_num - 1) % 12
        return normalize(house_sign * 30 + 15)
    
    def get_aspect_value(angle: float) -> float:
        """Get aspect value based on angle."""
        angle = abs(angle)
        if angle > 180:
            angle = 360 - angle
        
        if 175 <= angle <= 185:  # Opposition
            return 60.0
        elif 115 <= angle <= 125:  # Trine
            return 30.0
        elif 85 <= angle <= 95:  # Square
            return 45.0
        elif 55 <= angle <= 65:  # Sextile
            return 15.0
        return 0.0
    
    def bhavadhipati_bala(house_num: int) -> float:
        """
        Strength derived from the house lord's Shad Bala.
        The stronger the lord, the stronger the house.
        """
        lord = get_house_lord(house_num)
        if lord in shad_bala:
            # Use natural (uncapped) Shad Bala totals. Map typical 300-600 virupa
            # into a stable 20-60 virupa contribution for Bhava Bala.
            lord_total = float(shad_bala[lord].get("total_shashtiamsas", 300.0))
            return min(60.0, max(20.0, lord_total / 10.0))
        return 35.0
    
    def bhava_digbala(house_num: int) -> float:
        """
        Directional strength of the house.
        Kendras (1,4,7,10) are strongest, then Panaparas (2,5,8,11), then Apoklimas (3,6,9,12).
        Also adds special significance for certain houses.
        """
        # More conservative base values in Shashtiamsas
        if house_num in (1, 4, 7, 10):  # Kendras
            base = 45.0
        elif house_num in (2, 5, 8, 11):  # Panaparas
            base = 30.0
        else:  # Apoklimas (3, 6, 9, 12)
            base = 18.0

        # Smaller bonuses for key houses
        if house_num == 1:
            base += 6.0
        elif house_num == 10:
            base += 5.0
        elif house_num == 9:
            base += 4.0
        elif house_num in (5, 11):
            base += 3.0

        return base
    
    def bhava_drishti_bala(house_num: int) -> float:
        """
        Aspectual strength on the house.
        Benefic aspects add strength, malefic aspects reduce it.
        """
        DRIK_CAP_LOCAL = 60.0
        house_midpoint = get_house_midpoint(house_num)
        total = 0.0
        
        for planet_name, planet_data in planets_out.items():
            if planet_name == "Ketu":
                continue
            
            planet_lon = float(planet_data.get("longitude", 0.0))
            angle = angular_distance(planet_lon, house_midpoint)
            aspect_value = get_aspect_value(angle)
            
            if aspect_value > 0:
                if planet_name in BENEFICS:
                    total += aspect_value / 4
                elif planet_name in MALEFICS:
                    total -= aspect_value / 4
        
        # Cap Drik Bala to avoid extreme stacking in simplified aspect model
        if total > DRIK_CAP_LOCAL:
            return DRIK_CAP_LOCAL
        if total < -DRIK_CAP_LOCAL:
            return -DRIK_CAP_LOCAL
        return total
    
    def bhava_residential_strength(house_num: int) -> float:
        """
        Strength based on planets' position within the house.
        Planets closer to house midpoint give more strength.
        """
        house_midpoint = get_house_midpoint(house_num)
        house_sign = (lagna_sign + house_num - 1) % 12
        planets_in_house = rasi_chart.get(house_sign, [])
        
        total = 0.0
        for planet in planets_in_house:
            if planet == "Asc" or planet not in planets_out:
                continue
            
            planet_data = planets_out[planet]
            planet_lon = float(planet_data.get("longitude", 0.0))
            
            # Distance from house midpoint (0-15 degrees)
            dist = angular_distance(planet_lon, house_midpoint)
            if dist > 15:
                dist = 30 - dist  # Wrap around
            
            # Closer to midpoint = stronger (max 60 at midpoint, 0 at edge)
            residential = max(0.0, (15 - dist) * 4)
            
            # Adjust based on planet nature
            if planet in BENEFICS:
                total += residential
            elif planet in MALEFICS:
                total += residential * 0.5  # Malefics contribute less
        
        return min(60.0, total)
    
    def planet_contribution(house_num: int) -> float:
        """
        Additional strength from planets occupying the house.
        Based on their individual Shad Bala.
        """
        house_sign = (lagna_sign + house_num - 1) % 12
        planets_in_house = rasi_chart.get(house_sign, [])
        
        total = 0.0
        for planet in planets_in_house:
            if planet == "Asc" or planet not in shad_bala:
                continue
            
            planet_total = float(shad_bala[planet].get("total_shashtiamsas", 300.0))
            # Keep occupant contribution meaningful but bounded
            contribution = min(30.0, max(0.0, planet_total / 20.0))

            # Benefics contribute positively
            if planet in BENEFICS:
                total += contribution
            # Malefics can reduce or add depending on house
            elif planet in MALEFICS:
                if house_num in (3, 6, 11):  # Upachaya houses - malefics can help
                    total += contribution * 0.7
                elif house_num in (6, 8, 12):  # Dusthana houses - malefics reduce
                    total -= contribution * 0.4
                else:
                    total += contribution * 0.45
        
        return total
    
    for house in range(1, 13):
        house_sign = (lagna_sign + house - 1) % 12
        planets_in_house = rasi_chart.get(house_sign, [])
        lord = get_house_lord(house)
        
        # Calculate all components
        adhipati = bhavadhipati_bala(house)
        digbala = bhava_digbala(house)
        drishti = bhava_drishti_bala(house)
        residential = bhava_residential_strength(house)
        planet_contrib = planet_contribution(house)
        
        # Total Bhava Bala in Shashtiamsas
        total_shashtiamsas = adhipati + digbala + drishti + residential + planet_contrib

        # Add a stable base so empty houses don't collapse to near-zero.
        # Keeps results in a more interpretable 2-4 Rupa band for most houses.
        base_strength = 60.0
        total_shashtiamsas += base_strength
        
        # Convert to Rupas
        total_rupas = total_shashtiamsas / 60.0
        
        # Determine strength rating (Bhava Bala totals are typically lower than Shad Bala)
        if total_rupas >= 4.0:
            rating = "Very Strong"
        elif total_rupas >= 3.0:
            rating = "Strong"
        elif total_rupas >= 2.0:
            rating = "Medium"
        else:
            rating = "Weak"
        
        # Get lord's position
        lord_house = 0
        lord_sign = ""
        if lord in planets_out:
            lord_sign_idx = planets_out[lord].get("sign_index", 0)
            lord_house = ((lord_sign_idx - lagna_sign) % 12) + 1
            lord_sign = planets_out[lord].get("sign", "")
        
        bhava_bala[house] = {
            "house": house,
            "sign": SIGNS[house_sign],
            "sign_sanskrit": SIGNS_SANSKRIT[house_sign],
            "lord": lord,
            "lord_house": lord_house,
            "lord_sign": lord_sign,
            "planets_in_house": [p for p in planets_in_house if p != "Asc"],
            "bhavadhipati_bala": round(adhipati, 2),
            "bhava_digbala": round(digbala, 2),
            "bhava_drishti_bala": round(drishti, 2),
            "residential_strength": round(residential, 2),
            "planet_contribution": round(planet_contrib, 2),
            "total_shashtiamsas": round(total_shashtiamsas, 2),
            "total_rupas": round(total_rupas, 2),
            "is_strong": total_rupas >= 2.5,
            "rating": rating,
        }
    
    return bhava_bala


def calculate_shad_bala(
    planets_out: Dict,
    lagna_sign: int,
    lagna_longitude: float,
    jd_ut: float,
    birth_hour: float,
    latitude: float,
    longitude: float,
) -> Dict:
    """
    Calculate Shad Bala (sixfold strength) for planets following AstroSage/B.V. Raman methodology.
    
    Traditional Shad Bala components:
    1. Sthana Bala - Positional strength (5 sub-components)
       - Uccha Bala (exaltation strength)
       - Saptavargaja Bala (7 divisional chart strength)
       - Ojayugma Bala (odd/even sign strength)
       - Kendra Bala (angular house strength)
       - Drekkana Bala (decanate strength)
    2. Dig Bala - Directional strength
    3. Kala Bala - Temporal strength (9 sub-components)
       - Divaratri Bala (day/night strength)
       - Paksha Bala (lunar phase strength)
       - Tribhaga Bala (three-part day/night strength)
       - Abda Bala (year lord strength)
       - Masa Bala (month lord strength)
       - Vara Bala (weekday lord strength)
       - Hora Bala (hour lord strength)
       - Ayana Bala (declination strength)
       - Yuddha Bala (planetary war strength)
    4. Chesta Bala - Motional strength (based on relative speed)
    5. Naisargika Bala - Natural strength (fixed values)
    6. Drik Bala - Aspectual strength (with partial aspects)
    """
    shad_bala: Dict[str, Dict] = {}

    # Deep debilitation points (Neecha) in degrees - classical Uccha Bala uses distance from Neecha
    # Exaltation (Uccha) is exactly 180° opposite
    DEBILITATION_DEG = {
        "Sun": 190.0,      # 10° Libra (opposite of 10° Aries)
        "Moon": 213.0,     # 3° Scorpio (opposite of 3° Taurus)
        "Mars": 118.0,     # 28° Cancer (opposite of 28° Capricorn)
        "Mercury": 345.0,  # 15° Pisces (opposite of 15° Virgo)
        "Jupiter": 275.0,  # 5° Capricorn (opposite of 5° Cancer)
        "Venus": 177.0,    # 27° Virgo (opposite of 27° Pisces)
        "Saturn": 20.0,    # 20° Aries (opposite of 20° Libra)
    }

    # Dig Bala strongest houses (1=East/Asc, 4=North/IC, 7=West/Desc, 10=South/MC)
    DIG_BALA_HOUSE = {
        "Sun": 10,      # South (MC) - noon strength
        "Mars": 10,     # South (MC) - noon strength
        "Jupiter": 1,   # East (Asc) - morning strength
        "Mercury": 1,   # East (Asc) - morning strength
        "Saturn": 7,    # West (Desc) - evening strength
        "Moon": 4,      # North (IC) - midnight strength
        "Venus": 4,     # North (IC) - midnight strength
    }

    # Naisargika Bala (natural strength) - fixed values in Shashtiamsas
    NAISARGIKA_BALA = {
        "Sun": 60.0,
        "Moon": 51.43,
        "Venus": 42.85,
        "Jupiter": 34.28,
        "Mercury": 25.71,
        "Mars": 17.14,
        "Saturn": 8.57,
        "Rahu": 8.57,
        "Ketu": 8.57,
    }

    # Required strength in Shashtiamsas (minimum for planet to be considered strong)
    # These translate to Rupas: Sun=5, Moon=6, Mars=5, Mercury=7, Jupiter=6.5, Venus=5.5, Saturn=5
    REQUIRED_STRENGTH = {
        "Sun": 300,      # 5 Rupas
        "Moon": 360,     # 6 Rupas
        "Mars": 300,     # 5 Rupas
        "Mercury": 420,  # 7 Rupas
        "Jupiter": 390,  # 6.5 Rupas
        "Venus": 330,    # 5.5 Rupas
        "Saturn": 300,   # 5 Rupas
        "Rahu": 300,
        "Ketu": 300,
    }

    # Moolatrikona signs (0-indexed)
    MOOLATRIKONA = {
        "Sun": 4,       # Leo (0-20°)
        "Moon": 1,      # Taurus (4-30°)
        "Mars": 0,      # Aries (0-12°)
        "Mercury": 5,   # Virgo (16-20°)
        "Jupiter": 8,   # Sagittarius (0-10°)
        "Venus": 6,     # Libra (0-15°)
        "Saturn": 10,   # Aquarius (0-20°)
    }

    # Own signs for each planet
    OWN_SIGNS = {
        "Sun": [4],           # Leo
        "Moon": [3],          # Cancer
        "Mars": [0, 7],       # Aries, Scorpio
        "Mercury": [2, 5],    # Gemini, Virgo
        "Jupiter": [8, 11],   # Sagittarius, Pisces
        "Venus": [1, 6],      # Taurus, Libra
        "Saturn": [9, 10],    # Capricorn, Aquarius
        "Rahu": [10],         # Aquarius (some traditions)
        "Ketu": [7],          # Scorpio (some traditions)
    }

    # Natural friendships
    NATURAL_FRIENDS = {
        "Sun": ["Moon", "Mars", "Jupiter"],
        "Moon": ["Sun", "Mercury"],
        "Mars": ["Sun", "Moon", "Jupiter"],
        "Mercury": ["Sun", "Venus"],
        "Jupiter": ["Sun", "Moon", "Mars"],
        "Venus": ["Mercury", "Saturn"],
        "Saturn": ["Mercury", "Venus"],
        "Rahu": ["Mercury", "Venus", "Saturn"],
    }

    NATURAL_ENEMIES = {
        "Sun": ["Venus", "Saturn"],
        "Moon": [],
        "Mars": ["Mercury"],
        "Mercury": ["Moon"],
        "Jupiter": ["Mercury", "Venus"],
        "Venus": ["Sun", "Moon"],
        "Saturn": ["Sun", "Moon", "Mars"],
        "Rahu": ["Sun", "Moon", "Mars"],
    }

    # Average daily motion in degrees
    AVERAGE_SPEED = {
        "Sun": 0.9856,
        "Moon": 13.1764,
        "Mars": 0.5240,
        "Mercury": 1.3833,
        "Jupiter": 0.0831,
        "Venus": 1.2000,
        "Saturn": 0.0335,
    }

    # Weekday lords (0=Sunday)
    WEEKDAY_LORDS = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn"]

    # Classical Shadbala does not cap component values - they sum naturally
    # Typical ranges (for reference only, not used for capping):
    # - Sthana Bala: 100-250+ virupa
    # - Dig Bala: 0-60 virupa
    # - Kala Bala: 50-200+ virupa
    # - Chesta Bala: 0-60 virupa
    # - Naisargika Bala: 8.57-60 virupa (fixed)
    # - Drik Bala: can be positive or negative

    def normalize(deg: float) -> float:
        d = deg % 360.0
        return d if d >= 0 else d + 360.0

    def angular_distance(a: float, b: float) -> float:
        d = abs(normalize(a) - normalize(b))
        return min(d, 360.0 - d)

    def get_sign(lon: float) -> int:
        return int(normalize(lon) // 30)

    def get_navamsa_sign_local(lon: float) -> int:
        """
        Get Navamsa (D9) sign index from longitude.
        Uses correct element-based starting signs.
        """
        lon = normalize(lon)
        sign_index = int(lon // 30)
        degree_in_sign = lon % 30
        navamsa_pada = int(degree_in_sign / (30.0 / 9.0))  # 0-8
        
        # Determine starting sign based on element
        element = sign_index % 4  # 0=Fire, 1=Earth, 2=Air, 3=Water
        if element == 0:  # Fire (Aries, Leo, Sag)
            start_sign = 0  # Aries
        elif element == 1:  # Earth (Taurus, Virgo, Cap)
            start_sign = 9  # Capricorn
        elif element == 2:  # Air (Gemini, Libra, Aqu)
            start_sign = 6  # Libra
        else:  # Water (Cancer, Scorpio, Pisces)
            start_sign = 3  # Cancer
        
        return (start_sign + navamsa_pada) % 12

    def get_drekkana_sign(lon: float) -> int:
        lon = normalize(lon)
        sign_index = int(lon // 30)
        deg_in_sign = lon % 30
        if deg_in_sign < 10:
            return sign_index
        elif deg_in_sign < 20:
            return (sign_index + 4) % 12
        else:
            return (sign_index + 8) % 12

    def get_saptamsa_sign(lon: float) -> int:
        lon = normalize(lon)
        sign_index = int(lon // 30)
        deg_in_sign = lon % 30
        saptamsa_num = int(deg_in_sign / (30 / 7))
        if sign_index % 2 == 0:  # Odd sign
            return (sign_index + saptamsa_num) % 12
        else:  # Even sign
            return (sign_index + 6 + saptamsa_num) % 12

    def get_dwadasamsa_sign(lon: float) -> int:
        lon = normalize(lon)
        sign_index = int(lon // 30)
        deg_in_sign = lon % 30
        dwadasamsa_num = int(deg_in_sign / 2.5)
        return (sign_index + dwadasamsa_num) % 12

    def get_trimsamsa_sign(lon: float) -> int:
        lon = normalize(lon)
        sign_index = int(lon // 30)
        deg_in_sign = lon % 30
        # Trimsamsa division varies by odd/even sign
        if sign_index % 2 == 0:  # Odd sign
            if deg_in_sign < 5:
                return 0  # Aries (Mars)
            elif deg_in_sign < 10:
                return 10  # Aquarius (Saturn)
            elif deg_in_sign < 18:
                return 8  # Sagittarius (Jupiter)
            elif deg_in_sign < 25:
                return 2  # Gemini (Mercury)
            else:
                return 6  # Libra (Venus)
        else:  # Even sign
            if deg_in_sign < 5:
                return 1  # Taurus (Venus)
            elif deg_in_sign < 12:
                return 5  # Virgo (Mercury)
            elif deg_in_sign < 20:
                return 11  # Pisces (Jupiter)
            elif deg_in_sign < 25:
                return 9  # Capricorn (Saturn)
            else:
                return 7  # Scorpio (Mars)

    def get_hora_sign(lon: float) -> int:
        lon = normalize(lon)
        sign_index = int(lon // 30)
        deg_in_sign = lon % 30
        if sign_index % 2 == 0:  # Odd sign
            return 4 if deg_in_sign < 15 else 3  # Leo or Cancer
        else:  # Even sign
            return 3 if deg_in_sign < 15 else 4  # Cancer or Leo

    def is_own_sign(planet: str, sign: int) -> bool:
        return sign in OWN_SIGNS.get(planet, [])

    def is_moolatrikona(planet: str, lon: float) -> bool:
        sign = get_sign(lon)
        deg_in_sign = lon % 30
        mt_sign = MOOLATRIKONA.get(planet)
        if mt_sign is None or sign != mt_sign:
            return False
        # Check degree ranges for moolatrikona
        if planet == "Sun" and 0 <= deg_in_sign <= 20:
            return True
        if planet == "Moon" and 4 <= deg_in_sign <= 30:
            return True
        if planet == "Mars" and 0 <= deg_in_sign <= 12:
            return True
        if planet == "Mercury" and 16 <= deg_in_sign <= 20:
            return True
        if planet == "Jupiter" and 0 <= deg_in_sign <= 10:
            return True
        if planet == "Venus" and 0 <= deg_in_sign <= 15:
            return True
        if planet == "Saturn" and 0 <= deg_in_sign <= 20:
            return True
        return False

    def get_relationship(planet: str, sign: int) -> str:
        """Get relationship of planet with sign lord."""
        sign_lords = {
            0: "Mars", 1: "Venus", 2: "Mercury", 3: "Moon",
            4: "Sun", 5: "Mercury", 6: "Venus", 7: "Mars",
            8: "Jupiter", 9: "Saturn", 10: "Saturn", 11: "Jupiter"
        }
        lord = sign_lords.get(sign)
        if lord == planet:
            return "own"
        if lord in NATURAL_FRIENDS.get(planet, []):
            return "friend"
        if lord in NATURAL_ENEMIES.get(planet, []):
            return "enemy"
        return "neutral"

    # ==================== STHANA BALA ====================

    def uccha_bala(planet_name: str, planet_lon: float) -> float:
        """
        Classical Uccha Bala: max 60 Virupa at exaltation, 0 at debilitation.
        Formula: distance_from_neecha / 3 (since 180/3 = 60 max).
        """
        neecha = DEBILITATION_DEG.get(planet_name)
        if neecha is None:
            return 30.0  # Neutral for nodes (Rahu/Ketu)
        d = angular_distance(planet_lon, neecha)  # 0..180
        return d / 3.0  # Max 60 at exaltation (180° from neecha)

    def saptavargaja_bala(planet_name: str, planet_lon: float) -> float:
        """
        Strength from 7 divisional charts: Rasi, Hora, Drekkana, Saptamsa, Navamsa, Dwadasamsa, Trimsamsa.
        Moolatrikona=45, Own=30, Great Friend=22.5, Friend=15, Neutral=7.5, Enemy=3.75, Great Enemy=1.875
        """
        total = 0.0
        rasi_sign = get_sign(planet_lon)
        
        # Rasi chart (special: moolatrikona gives 45)
        if is_moolatrikona(planet_name, planet_lon):
            total += 45.0
        elif is_own_sign(planet_name, rasi_sign):
            total += 30.0
        else:
            rel = get_relationship(planet_name, rasi_sign)
            if rel == "friend":
                total += 15.0
            elif rel == "enemy":
                total += 3.75
            else:
                total += 7.5

        # Other 6 vargas
        varga_signs = [
            get_hora_sign(planet_lon),
            get_drekkana_sign(planet_lon),
            get_saptamsa_sign(planet_lon),
            get_navamsa_sign_local(planet_lon),
            get_dwadasamsa_sign(planet_lon),
            get_trimsamsa_sign(planet_lon),
        ]

        for varga_sign in varga_signs:
            if is_own_sign(planet_name, varga_sign):
                total += 30.0
            else:
                rel = get_relationship(planet_name, varga_sign)
                if rel == "friend":
                    total += 15.0
                elif rel == "enemy":
                    total += 3.75
                else:
                    total += 7.5

        return total

    def ojayugma_bala(planet_name: str, planet_lon: float) -> float:
        """
        Odd/Even sign strength.
        Moon/Venus get 15 in even signs, others get 15 in odd signs.
        Same applies to navamsa. Max 30.
        """
        rasi_sign = get_sign(planet_lon)
        navamsa_sign = get_navamsa_sign_local(planet_lon)
        total = 0.0

        is_female = planet_name in ("Moon", "Venus")
        rasi_even = (rasi_sign % 2 == 1)  # 0-indexed, so odd index = even sign
        navamsa_even = (navamsa_sign % 2 == 1)

        if is_female:
            if rasi_even:
                total += 15.0
            if navamsa_even:
                total += 15.0
        else:
            if not rasi_even:
                total += 15.0
            if not navamsa_even:
                total += 15.0

        return total

    def kendra_bala(planet_name: str, house: int) -> float:
        """Angular house strength: Kendra=60, Panapara=30, Apoklima=15."""
        if house in (1, 4, 7, 10):
            return 60.0
        elif house in (2, 5, 8, 11):
            return 30.0
        else:
            return 15.0

    def drekkana_bala(planet_name: str, planet_lon: float) -> float:
        """
        Decanate strength based on planet gender.
        Male (Sun, Jupiter, Mars): strong in 1st drekkana (0-10°)
        Neutral (Saturn, Mercury): strong in 2nd drekkana (10-20°)
        Female (Moon, Venus): strong in 3rd drekkana (20-30°)
        """
        deg_in_sign = normalize(planet_lon) % 30
        drekkana = 1 if deg_in_sign < 10 else (2 if deg_in_sign < 20 else 3)

        male_planets = ("Sun", "Jupiter", "Mars")
        neutral_planets = ("Saturn", "Mercury")
        female_planets = ("Moon", "Venus")

        if planet_name in male_planets and drekkana == 1:
            return 15.0
        elif planet_name in neutral_planets and drekkana == 2:
            return 15.0
        elif planet_name in female_planets and drekkana == 3:
            return 15.0
        return 0.0

    def calc_sthana_bala(planet_name: str, planet_lon: float, house: int) -> Dict:
        """Calculate total Sthana Bala with all 5 components (classical, no capping)."""
        uccha = uccha_bala(planet_name, planet_lon)
        saptavargaja = saptavargaja_bala(planet_name, planet_lon)
        ojayugma = ojayugma_bala(planet_name, planet_lon)
        kendra = kendra_bala(planet_name, house)
        drekkana = drekkana_bala(planet_name, planet_lon)

        total = uccha + saptavargaja + ojayugma + kendra + drekkana

        return {
            "uccha": round(uccha, 2),
            "saptavargaja": round(saptavargaja, 2),
            "ojayugma": round(ojayugma, 2),
            "kendra": round(kendra, 2),
            "drekkana": round(drekkana, 2),
            "total": round(total, 2),
        }

    # ==================== DIG BALA ====================

    def dig_bala(planet_name: str, planet_lon: float) -> float:
        """
        Directional strength based on house position.
        Max 60 at strongest house midpoint, 0 at opposite.
        """
        strongest_house = DIG_BALA_HOUSE.get(planet_name)
        if strongest_house is None:
            return 30.0  # Neutral for Rahu/Ketu
        
        # Calculate midpoint of strongest house
        strongest_long = normalize(lagna_longitude + (strongest_house - 1) * 30)
        d = angular_distance(planet_lon, strongest_long)
        return max(0.0, (180.0 - d) / 3.0)

    # ==================== KALA BALA ====================

    def get_weekday(jd: float) -> int:
        """Get weekday from Julian day (0=Sunday, 1=Monday, etc.)."""
        return int(jd + 1.5) % 7

    def get_year_lord(jd: float) -> str:
        """Get lord of the year (Abda lord)."""
        # Using 360-day year calculation
        year_num = int((jd - 588465.5) / 360) % 7
        return WEEKDAY_LORDS[year_num]

    def get_month_lord(jd: float) -> str:
        """Get lord of the month (Masa lord)."""
        # Using 30-day month calculation
        month_num = int((jd - 588465.5) / 30) % 7
        return WEEKDAY_LORDS[month_num]

    def get_hora_lord(jd: float, birth_hour: float, lat: float, lon: float) -> str:
        """Get lord of the hora (planetary hour)."""
        weekday = get_weekday(jd)
        # Simplified hora calculation
        hora_num = int(birth_hour) % 24
        # Hora sequence starts from weekday lord
        hora_sequence = [0, 3, 6, 2, 5, 1, 4]  # Sun, Moon, Mars, Mercury, Jupiter, Venus, Saturn order
        start_idx = hora_sequence.index(weekday)
        current_idx = (start_idx + hora_num) % 7
        return WEEKDAY_LORDS[hora_sequence[current_idx]]

    def divaratri_bala(planet_name: str, birth_hour: float, is_day: bool) -> float:
        """
        Day/Night strength.
        Moon, Saturn, Mars: strong at midnight (60), weak at noon (0)
        Sun, Jupiter, Venus: strong at noon (60), weak at midnight (0)
        Mercury: always 60
        """
        if planet_name == "Mercury":
            return 60.0
        
        night_planets = ("Moon", "Saturn", "Mars")
        day_planets = ("Sun", "Jupiter", "Venus")
        
        # Calculate strength based on time of day
        # 0 = midnight, 12 = noon
        hour_from_midnight = birth_hour if birth_hour < 12 else 24 - birth_hour
        
        if planet_name in night_planets:
            # Strong at midnight, weak at noon
            return (12 - abs(hour_from_midnight)) * 5
        elif planet_name in day_planets:
            # Strong at noon, weak at midnight
            return abs(hour_from_midnight) * 5
        return 30.0

    def paksha_bala(sun_lon: float, moon_lon: float, planet_name: str) -> float:
        """
        Lunar phase strength.
        Benefics (Jupiter, Venus, Moon, Mercury): strong in Shukla Paksha (waxing)
        Malefics (Sun, Mars, Saturn): strong in Krishna Paksha (waning)
        Moon's paksha bala is doubled.
        """
        # Calculate tithi (lunar day)
        diff = normalize(moon_lon - sun_lon)
        
        benefics = ("Jupiter", "Venus", "Moon", "Mercury")
        
        if planet_name in benefics:
            bala = diff / 3.0  # Max 60 at full moon
        else:
            bala = (180.0 - min(diff, 360 - diff)) / 3.0
        
        # Double Moon's paksha bala
        if planet_name == "Moon":
            bala *= 2.0
        
        return min(60.0, bala)

    def tribhaga_bala(planet_name: str, birth_hour: float, sunrise: float = 6.0, sunset: float = 18.0) -> float:
        """
        Three-part day/night strength.
        Jupiter always gets 60.
        Day: 1st part=Mercury, 2nd part=Sun, 3rd part=Saturn
        Night: 1st part=Moon, 2nd part=Venus, 3rd part=Mars
        """
        if planet_name == "Jupiter":
            return 60.0
        
        day_length = sunset - sunrise
        night_length = 24 - day_length
        
        if sunrise <= birth_hour < sunset:  # Daytime
            time_in_day = birth_hour - sunrise
            part = int(time_in_day / (day_length / 3))
            if part == 0 and planet_name == "Mercury":
                return 60.0
            elif part == 1 and planet_name == "Sun":
                return 60.0
            elif part == 2 and planet_name == "Saturn":
                return 60.0
        else:  # Nighttime
            if birth_hour >= sunset:
                time_in_night = birth_hour - sunset
            else:
                time_in_night = birth_hour + (24 - sunset)
            part = int(time_in_night / (night_length / 3))
            if part == 0 and planet_name == "Moon":
                return 60.0
            elif part == 1 and planet_name == "Venus":
                return 60.0
            elif part == 2 and planet_name == "Mars":
                return 60.0
        
        return 0.0

    def abda_bala(planet_name: str, jd: float) -> float:
        """Year lord gets 15 Shashtiamsas."""
        return 15.0 if planet_name == get_year_lord(jd) else 0.0

    def masa_bala(planet_name: str, jd: float) -> float:
        """Month lord gets 30 Shashtiamsas."""
        return 30.0 if planet_name == get_month_lord(jd) else 0.0

    def vara_bala(planet_name: str, jd: float) -> float:
        """Weekday lord gets 45 Shashtiamsas."""
        weekday = get_weekday(jd)
        return 45.0 if planet_name == WEEKDAY_LORDS[weekday] else 0.0

    def hora_bala(planet_name: str, jd: float, birth_hour: float, lat: float, lon: float) -> float:
        """Hora lord gets 60 Shashtiamsas."""
        return 60.0 if planet_name == get_hora_lord(jd, birth_hour, lat, lon) else 0.0

    def ayana_bala(planet_name: str, planet_lon: float) -> float:
        """
        Declination-based strength.
        Northern declination favors Sun, Mars, Jupiter, Venus, Mercury.
        Southern declination favors Moon, Saturn.
        Sun's ayana bala is doubled.
        """
        # Calculate approximate declination from longitude
        # Using simplified formula: decl = 23.45 * sin(longitude)
        decl = 23.45 * math.sin(math.radians(planet_lon))
        
        north_planets = ("Sun", "Mars", "Jupiter", "Venus", "Mercury")
        
        if planet_name in north_planets:
            bala = 30.0 + (decl * 30.0 / 23.45)
        else:
            bala = 30.0 - (decl * 30.0 / 23.45)
        
        # Double Sun's ayana bala
        if planet_name == "Sun":
            bala *= 2.0
        
        return max(0.0, min(60.0, bala))

    def calc_kala_bala(
        planet_name: str,
        planet_lon: float,
        sun_lon: float,
        moon_lon: float,
        jd: float,
        birth_hour: float,
        lat: float,
        lon: float,
    ) -> Dict:
        """Calculate total Kala Bala with all components (classical, no capping)."""
        divaratri = divaratri_bala(planet_name, birth_hour, True)
        paksha = paksha_bala(sun_lon, moon_lon, planet_name)
        tribhaga = tribhaga_bala(planet_name, birth_hour)
        abda = abda_bala(planet_name, jd)
        masa = masa_bala(planet_name, jd)
        vara = vara_bala(planet_name, jd)
        hora = hora_bala(planet_name, jd, birth_hour, lat, lon)
        ayana = ayana_bala(planet_name, planet_lon)

        total = divaratri + paksha + tribhaga + abda + masa + vara + hora + ayana

        return {
            "divaratri": round(divaratri, 2),
            "paksha": round(paksha, 2),
            "tribhaga": round(tribhaga, 2),
            "abda": round(abda, 2),
            "masa": round(masa, 2),
            "vara": round(vara, 2),
            "hora": round(hora, 2),
            "ayana": round(ayana, 2),
            "total": round(total, 2),
        }

    # ==================== CHESTA BALA ====================

    def chesta_bala(planet_name: str, speed: float, is_retrograde: bool) -> float:
        """
        Motional strength based on relative speed.
        Retrograde planets get high chesta bala (up to 60).
        Sun and Moon don't get chesta bala.
        """
        # Nodes (Rahu/Ketu) don't use classical Chesta Bala in this simplified model
        if planet_name in ("Sun", "Moon", "Rahu", "Ketu"):
            return 0.0
        
        avg_speed = AVERAGE_SPEED.get(planet_name, 1.0)
        if avg_speed == 0:
            return 30.0
        
        # Relative speed ratio
        speed_ratio = abs(speed) / avg_speed
        
        if is_retrograde:
            # Retrograde planets are considered strong
            return 60.0
        elif speed_ratio < 0.5:
            # Very slow (almost stationary) - strong
            return 45.0 + (0.5 - speed_ratio) * 30
        elif speed_ratio < 1.0:
            # Slower than average
            return 30.0 + (1.0 - speed_ratio) * 30
        else:
            # Faster than average - weaker
            return max(0.0, 30.0 - (speed_ratio - 1.0) * 15)

    # ==================== NAISARGIKA BALA ====================

    def naisargika_bala(planet_name: str) -> float:
        """Natural strength - fixed values based on luminosity."""
        return NAISARGIKA_BALA.get(planet_name, 8.57)

    # ==================== DRIK BALA ====================

    def get_aspect_strength(angle: float) -> float:
        """
        Get aspect strength with partial aspects.
        180° = 100% (60), 120° = 50% (30), 90° = 75% (45), 60° = 25% (15)
        """
        angle = abs(angle)
        if angle > 180:
            angle = 360 - angle
        
        # Full aspect at 180°
        if 175 <= angle <= 185:
            return 60.0
        # Trine aspect at 120°
        elif 115 <= angle <= 125:
            return 30.0
        # Square aspect at 90°
        elif 85 <= angle <= 95:
            return 45.0
        # Sextile aspect at 60°
        elif 55 <= angle <= 65:
            return 15.0
        # Interpolate for other angles
        elif 65 < angle < 85:
            return 15.0 + (angle - 65) * (45.0 - 15.0) / 20
        elif 95 < angle < 115:
            return 45.0 - (angle - 95) * (45.0 - 30.0) / 20
        elif 125 < angle < 175:
            return 30.0 + (angle - 125) * (60.0 - 30.0) / 50
        
        return 0.0

    def drik_bala(planet_name: str, planet_lon: float, all_planets: Dict) -> float:
        """
        Aspectual strength.
        Positive if aspected by benefics, negative if by malefics.
        """
        benefics = ("Jupiter", "Venus", "Mercury", "Moon")
        malefics = ("Sun", "Mars", "Saturn", "Rahu")
        
        total = 0.0
        for other_name, other_data in all_planets.items():
            if other_name == planet_name or other_name == "Ketu":
                continue
            
            other_lon = float(other_data.get("longitude", 0.0))
            angle = angular_distance(planet_lon, other_lon)
            aspect_strength = get_aspect_strength(angle)
            
            if aspect_strength > 0:
                if other_name in benefics:
                    total += aspect_strength / 4
                elif other_name in malefics:
                    total -= aspect_strength / 4
        
        return total

    # ==================== MAIN CALCULATION ====================

    sun_lon = float(planets_out.get("Sun", {}).get("longitude", 0.0))
    moon_lon = float(planets_out.get("Moon", {}).get("longitude", 0.0))

    for planet_name, planet_data in planets_out.items():

        planet_lon = float(planet_data.get("longitude", 0.0))
        planet_speed = float(planet_data.get("speed", 0.0)) if "speed" in planet_data else 0.0
        is_retrograde = bool(planet_data.get("retrograde", False))
        house = int(planet_data.get("house_whole_sign", 1))

        # Calculate all components
        sthana = calc_sthana_bala(planet_name, planet_lon, house)
        dig = dig_bala(planet_name, planet_lon)
        kala = calc_kala_bala(planet_name, planet_lon, sun_lon, moon_lon, jd_ut, birth_hour, latitude, longitude)
        chesta = chesta_bala(planet_name, planet_speed, is_retrograde)
        naisargika = naisargika_bala(planet_name)
        drik = drik_bala(planet_name, planet_lon, planets_out)

        # Total in Shashtiamsas
        total_shashtiamsas = sthana["total"] + dig + kala["total"] + chesta + naisargika + drik

        # Convert to Rupas (divide by 60). Component normalization above already
        # prevents inflated totals, so avoid additional global scaling.
        total_rupas = (total_shashtiamsas / 60.0)

        required = REQUIRED_STRENGTH.get(planet_name, 300)
        required_rupas = required / 60.0
        ratio = total_rupas / required_rupas if required_rupas > 0 else 0

        # Match frontend thresholds: Strong ≥120%, Medium ≥90%, Weak <90%
        if ratio >= 1.20:
            strength = "Strong"
        elif ratio >= 0.90:
            strength = "Medium"
        else:
            strength = "Weak"

        shad_bala[planet_name] = {
            "sthana_bala": sthana,
            "dig_bala": round(dig, 2),
            "kala_bala": kala,
            "chesta_bala": round(chesta, 2),
            "naisargika_bala": round(naisargika, 2),
            "drik_bala": round(drik, 2),
            "total_shashtiamsas": round(total_shashtiamsas, 2),
            "total_rupas": round(total_rupas, 2),
            "required_rupas": round(required_rupas, 2),
            "ratio": round(ratio, 2),
            # Keep consistent with strength thresholds used by frontend
            "is_strong": ratio >= 1.20,
            "strength": strength,
        }

    return shad_bala


def calculate_vimshottari_dasha(moon_longitude: float, birth_datetime: datetime) -> Dict:
    """
    Calculate Vimshottari Dasha periods based on Moon's nakshatra position.
    Returns dict with dasha periods and their start/end dates.
    """
    # Get nakshatra from Moon longitude (27 nakshatras, each 13°20')
    nakshatra_index = int(moon_longitude / (360 / 27))  # 0-26
    
    # Map nakshatra to dasha lord (correct mapping based on Ashwini = Ketu)
    # Each nakshatra is 13°20' (800 minutes)
    nakshatra_lords = [
        "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury",  # 1-9: Ashwini to Ashlesha
        "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury",  # 10-18: Magha to Jyeshtha
        "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury",  # 19-27: Mula to Revati
    ]
    
    # Get the starting dasha lord
    current_dasha_lord = nakshatra_lords[nakshatra_index]
    
    # Calculate how much of the current dasha period has passed
    nakshatra_degree = moon_longitude % (360 / 27)  # 0-13.333°
    nakshatra_portion = nakshatra_degree / (360 / 27)  # 0-1 within the nakshatra
    
    total_dasha_years = DASHA_PERIODS[current_dasha_lord]
    years_passed = total_dasha_years * nakshatra_portion
    years_remaining = total_dasha_years - years_passed
    
    
    # Calculate dasha periods (Mahadasha segments from birth onward)
    dasha_periods = []

    # Current Mahadasha at birth is usually a partial segment (remaining only)
    segment_start = birth_datetime
    segment_end = birth_datetime + timedelta(days=years_remaining * 365.25)

    dasha_periods.append({
        "planet": current_dasha_lord,
        "start_datetime": segment_start.isoformat(),
        "start_date": segment_start.strftime("%Y-%m-%d"),
        "start_year": segment_start.year,
        "start_month": segment_start.month,
        "start_day": segment_start.day,
        "end_datetime": segment_end.isoformat(),
        "end_date": segment_end.strftime("%Y-%m-%d"),
        "end_year": segment_end.year,
        "end_month": segment_end.month,
        "end_day": segment_end.day,
        "years": round(years_remaining, 6),
        "total_years": total_dasha_years,
        "years_passed": round(years_passed, 6),
        "is_current": True,
    })
    
    # Add remaining dasha periods in order
    current_index = DASHA_ORDER.index(current_dasha_lord)
    end_date = segment_end
    
    for i in range(1, 9):  # 8 more dashas to complete 120 years
        next_index = (current_index + i) % 9
        planet = DASHA_ORDER[next_index]
        years = DASHA_PERIODS[planet]
        
        next_end_date = end_date + timedelta(days=years * 365.25)
        dasha_periods.append({
            "planet": planet,
            "start_datetime": end_date.isoformat(),
            "start_date": end_date.strftime("%Y-%m-%d"),
            "start_year": end_date.year,
            "start_month": end_date.month,
            "start_day": end_date.day,
            "end_datetime": next_end_date.isoformat(),
            "end_date": next_end_date.strftime("%Y-%m-%d"),
            "end_year": next_end_date.year,
            "end_month": next_end_date.month,
            "end_day": next_end_date.day,
            "years": round(years, 6),
            "total_years": years,
            "years_passed": 0.0,
            "is_current": False,
        })
        
        end_date = next_end_date
    
    return {
        "current_dasha": current_dasha_lord,
        "moon_nakshatra": nakshatra_index + 1,  # 1-27
        "moon_nakshatra_name": get_nakshatra_name(nakshatra_index),
        "moon_nakshatra_pada": int(nakshatra_portion * 4) + 1,  # 1-4
        "periods": dasha_periods,
    }


def calculate_antardashas(dasha_planet: str, dasha_start_date: datetime, dasha_years: float) -> List[Dict]:
    """
    Calculate Antardashas (sub-periods) for a given dasha period.
    Antardashas start from the dasha lord and follow the Vimshottari order.
    
    Formula (full Mahadasha): Antardasha duration = (Mahadasha planet years × Antardasha planet years) / 120
    If the Mahadasha segment is partial (e.g. current-at-birth), we skip the elapsed Antardasha time and
    return the remaining Antardashas clipped to the segment window.
    """
    antardashas = []
    segment_start = dasha_start_date
    segment_end = dasha_start_date + timedelta(days=dasha_years * 365.25)
    current_date = segment_start

    mahadasha_full_years = DASHA_PERIODS[dasha_planet]
    elapsed_years = max(0.0, mahadasha_full_years - dasha_years)

    start_index = DASHA_ORDER.index(dasha_planet)

    # Build full antardasha sequence durations for the Mahadasha
    full_seq: List[Dict] = []
    for i in range(9):
        planet = DASHA_ORDER[(start_index + i) % 9]
        ant_years = (mahadasha_full_years * DASHA_PERIODS[planet]) / VIMSHOTTARI_CYCLE
        full_seq.append({"planet": planet, "years": ant_years})

    # Skip elapsed antardasha time to align to segment_start
    remaining_to_skip = elapsed_years
    idx = 0
    while idx < len(full_seq) and remaining_to_skip > 1e-12:
        d = full_seq[idx]["years"]
        if remaining_to_skip >= d - 1e-12:
            remaining_to_skip -= d
            idx += 1
        else:
            break

    # If we are inside an antardasha, first entry is the remaining portion
    if idx < len(full_seq) and remaining_to_skip > 1e-12:
        planet = full_seq[idx]["planet"]
        remaining_years = full_seq[idx]["years"] - remaining_to_skip
        end_date = current_date + timedelta(days=remaining_years * 365.25)
        if end_date > segment_end:
            end_date = segment_end
        antardashas.append({
            "planet": planet,
            "start_datetime": current_date.isoformat(),
            "start_date": current_date.strftime("%Y-%m-%d"),
            "start_year": current_date.year,
            "start_month": current_date.month,
            "start_day": current_date.day,
            "end_datetime": end_date.isoformat(),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "end_year": end_date.year,
            "end_month": end_date.month,
            "end_day": end_date.day,
            "years": round((end_date - current_date).total_seconds() / (365.25 * 24 * 3600), 6),
        })
        current_date = end_date
        idx += 1

    # Add subsequent full antardashas until we reach segment_end
    while idx < len(full_seq) and current_date < segment_end:
        planet = full_seq[idx]["planet"]
        d_years = full_seq[idx]["years"]
        end_date = current_date + timedelta(days=d_years * 365.25)
        if end_date > segment_end:
            end_date = segment_end
        antardashas.append({
            "planet": planet,
            "start_datetime": current_date.isoformat(),
            "start_date": current_date.strftime("%Y-%m-%d"),
            "start_year": current_date.year,
            "start_month": current_date.month,
            "start_day": current_date.day,
            "end_datetime": end_date.isoformat(),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "end_year": end_date.year,
            "end_month": end_date.month,
            "end_day": end_date.day,
            "years": round((end_date - current_date).total_seconds() / (365.25 * 24 * 3600), 6),
        })
        current_date = end_date
        idx += 1
    
    return antardashas


def get_nakshatra_name(index: int) -> str:
    """Get nakshatra name by index (0-26)."""
    nakshatras = [
        "Ashwini", "Bharani", "Krittika", "Rohini", "Mrigashirsha", "Ardra", "Punarvasu",
        "Pushya", "Ashlesha", "Magha", "Purva Phalguni", "Uttara Phalguni", "Hasta", "Chitra",
        "Swati", "Vishakha", "Anuradha", "Jyeshtha", "Mula", "Purva Ashadha", "Uttara Ashadha",
        "Shravana", "Dhanishta", "Shatabhisha", "Purva Bhadrapada", "Uttara Bhadrapada", "Revati"
    ]
    return nakshatras[index] if 0 <= index < len(nakshatras) else "Unknown"


def kundali(b: BirthInput) -> Dict:
    swe.set_ephe_path(b.ephe_path)
    swe.set_sid_mode(b.ayanamsha, 0, 0)

    # DST handling is only for converting local civil time -> UT (Julian day).
    # Kala Bala and Dasha must use LOCAL civil time at birthplace, not IST.
    original_tz_offset = b.tz_offset_hours
    adjusted_tz_offset = adjust_for_dst(b.year, b.month, b.day, b.latitude, b.longitude, original_tz_offset)
    dst_applied = adjusted_tz_offset != original_tz_offset
    dst_adjustment = adjusted_tz_offset - original_tz_offset

    # For UI/debug only: also compute IST equivalent of the entered local time.
    ist_time = convert_to_ist(
        b.year, b.month, b.day, b.hour, b.minute, b.second,
        b.tz_offset_hours, b.latitude, b.longitude
    )

    jd_ut = compute_julian_day_local(b, adjusted_tz_offset)

    flags = swe.FLG_SWIEPH | swe.FLG_SPEED | swe.FLG_SIDEREAL

    ay = swe.get_ayanamsa(jd_ut)
    asc_trop = compute_lagna(jd_ut, b.latitude, b.longitude)
    asc_sid = norm_deg(asc_trop - ay)

    lagna_sign = deg_to_sign_index(asc_sid)
    lagna_sign_name, ld, lm, ls = deg_to_sign_deg(asc_sid)

    planets_out: Dict[str, Dict] = {}

    # First pass: basic planet data
    for name, p in PLANETS.items():
        result, _ = swe.calc_ut(jd_ut, p, flags)
        lon = norm_deg(result[0])
        lon_speed = result[3]
        retro = lon_speed < 0

        sign_idx = deg_to_sign_index(lon)
        sign_name, d, m, s = deg_to_sign_deg(lon)
        house = whole_sign_house(lagna_sign, sign_idx)
        navamsa_sign = get_navamsa_sign(lon)

        planets_out[name] = {
            "longitude": round(lon, 4),
            "speed": round(lon_speed, 6),  # Daily motion in degrees for Chesta Bala
            "sign": sign_name,
            "sign_sanskrit": SIGNS_SANSKRIT[sign_idx],
            "sign_index": sign_idx,
            "navamsa_sign_index": navamsa_sign,
            "navamsa_sign": SIGNS[navamsa_sign],
            "navamsa_sign_sanskrit": SIGNS_SANSKRIT[navamsa_sign],
            "deg": d,
            "min": m,
            "sec": round(s, 2),
            "house_whole_sign": house,
            "retrograde": retro,
            "symbol": PLANET_SYMBOLS[name],
            "exalted": sign_idx == EXALTATION.get(name),
            "debilitated": sign_idx == DEBILITATION.get(name),
            "vargottama": sign_idx == navamsa_sign,
            "combust": False,
        }

    # Second pass: combustion (needs Sun longitude)
    sun_lon = float(planets_out["Sun"]["longitude"])
    for name in list(planets_out.keys()):
        if name == "Sun":
            continue
        planets_out[name]["combust"] = is_combust(
            name,
            float(planets_out[name]["longitude"]),
            sun_lon,
            bool(planets_out[name]["retrograde"]),
        )

    # Ketu = Rahu + 180
    rahu_lon = float(planets_out["Rahu"]["longitude"])
    ketu_lon = norm_deg(rahu_lon + 180.0)
    ketu_sign_idx = deg_to_sign_index(ketu_lon)
    ketu_sign_name, kd, km, ks = deg_to_sign_deg(ketu_lon)
    ketu_house = whole_sign_house(lagna_sign, ketu_sign_idx)
    ketu_navamsa_sign = get_navamsa_sign(ketu_lon)

    planets_out["Ketu"] = {
        "longitude": round(ketu_lon, 4),
        "speed": planets_out["Rahu"].get("speed", 0.0),  # Same speed as Rahu
        "sign": ketu_sign_name,
        "sign_sanskrit": SIGNS_SANSKRIT[ketu_sign_idx],
        "sign_index": ketu_sign_idx,
        "navamsa_sign_index": ketu_navamsa_sign,
        "navamsa_sign": SIGNS[ketu_navamsa_sign],
        "navamsa_sign_sanskrit": SIGNS_SANSKRIT[ketu_navamsa_sign],
        "deg": kd,
        "min": km,
        "sec": round(ks, 2),
        "house_whole_sign": ketu_house,
        "retrograde": planets_out["Rahu"].get("retrograde", False),
        "symbol": PLANET_SYMBOLS["Ketu"],
        "exalted": ketu_sign_idx == EXALTATION.get("Ketu"),
        "debilitated": ketu_sign_idx == DEBILITATION.get("Ketu"),
        "vargottama": ketu_sign_idx == ketu_navamsa_sign,
        "combust": False,
    }

    # Calculate Upagrahas
    upagrahas = calculate_upagrahas(sun_lon, jd_ut, b.latitude, b.longitude)
    
    # Add house info to upagrahas
    for uname, uinfo in upagrahas.items():
        uinfo["house_whole_sign"] = whole_sign_house(lagna_sign, int(uinfo["sign_index"]))

    # Build rasi chart
    rasi_chart: Dict[int, List[str]] = {i: [] for i in range(12)}
    for pname, info in planets_out.items():
        rasi_chart[int(info["sign_index"])].append(pname)
    # Add upagrahas to rasi chart
    for uname, uinfo in upagrahas.items():
        rasi_chart[int(uinfo["sign_index"])].append(uname)
    rasi_chart[lagna_sign].insert(0, "Asc")

    # Build Navamsa (D9) chart
    navamsa_chart: Dict[int, List[str]] = {i: [] for i in range(12)}
    for pname, info in planets_out.items():
        nav_sign_idx = int(info.get("navamsa_sign_index", 0))
        navamsa_chart[nav_sign_idx].append(pname)
    # Add upagrahas to navamsa chart
    for uname, uinfo in upagrahas.items():
        nav_sign_idx = int(uinfo.get("navamsa_sign_index", 0))
        navamsa_chart[nav_sign_idx].append(uname)
    nav_asc_sign = get_navamsa_sign(asc_sid)
    navamsa_chart[nav_asc_sign].insert(0, "Asc")

    # Kala Bala depends on LOCAL civil time at birthplace.
    # If the request is in UTC, convert UTC -> local using DST-adjusted offset.
    if getattr(b, "use_utc", False):
        local_birth = utc_to_local(
            year=b.year,
            month=b.month,
            day=b.day,
            hour=b.hour,
            minute=b.minute,
            second=b.second,
            tz_offset=adjusted_tz_offset,
        )
        birth_hour_local = (
            local_birth["hour"] + local_birth["minute"] / 60.0 + local_birth["second"] / 3600.0
        )
        birth_datetime_local = datetime(
            local_birth["year"],
            local_birth["month"],
            local_birth["day"],
            local_birth["hour"],
            local_birth["minute"],
            local_birth["second"],
        )
    else:
        birth_hour_local = b.hour + b.minute / 60.0 + b.second / 3600.0
        birth_datetime_local = datetime(b.year, b.month, b.day, b.hour, b.minute, b.second)

    # Calculate Shad Bala with all required parameters using LOCAL time
    shad_bala = calculate_shad_bala(
        planets_out=planets_out,
        lagna_sign=lagna_sign,
        lagna_longitude=asc_sid,
        jd_ut=jd_ut,
        birth_hour=birth_hour_local,
        latitude=b.latitude,
        longitude=b.longitude,
    )
    
    # Calculate Bhava Bala using Shad Bala results
    bhava_bala = calculate_bhava_bala(
        planets_out=planets_out,
        lagna_sign=lagna_sign,
        lagna_longitude=asc_sid,
        rasi_chart=rasi_chart,
        shad_bala=shad_bala,
    )

    # Calculate Dasha periods using LOCAL civil time
    moon_longitude = float(planets_out["Moon"]["longitude"])
    dasha_data = calculate_vimshottari_dasha(moon_longitude, birth_datetime_local)
    
    # Calculate Antardashas for all dasha periods
    for i, period in enumerate(dasha_data["periods"]):
        if period.get("start_datetime"):
            try:
                start_date = datetime.fromisoformat(period["start_datetime"])
            except Exception:
                start_date = datetime(period["start_year"], period["start_month"], period["start_day"])
        else:
            start_date = datetime(period["start_year"], period["start_month"], period["start_day"])

        antardashas = calculate_antardashas(period["planet"], start_date, float(period["years"]))
        period["antardashas"] = antardashas

    return {
        "meta": {
            "ayanamsha": "Lahiri" if b.ayanamsha == swe.SIDM_LAHIRI else str(b.ayanamsha),
            "jd_ut": round(jd_ut, 6),
        },
        "birth": {
            "date": f"{b.year:04d}-{b.month:02d}-{b.day:02d}",
            "time": f"{b.hour:02d}:{b.minute:02d}:{b.second:02d}",
            "tz_offset_hours": b.tz_offset_hours,
            "adjusted_tz_offset_hours": adjusted_tz_offset,
            "dst_applied": dst_applied,
            "dst_adjustment_hours": dst_adjustment if dst_applied else 0,
            "latitude": b.latitude,
            "longitude": b.longitude,
            "ist_date": f"{ist_time['year']:04d}-{ist_time['month']:02d}-{ist_time['day']:02d}",
            "ist_time": f"{ist_time['hour']:02d}:{ist_time['minute']:02d}:{ist_time['second']:02d}",
            "ist_tz_offset": 5.5,
            "debug_time_conversion": {
                "source_original_tz_offset": ist_time["original_tz_offset"],
                "source_effective_tz_offset": ist_time["dst_adjusted_tz_offset"],
                "dst_applied_by_backend": ist_time["dst_applied"],
                "utc_date": f"{ist_time['utc_year']:04d}-{ist_time['utc_month']:02d}-{ist_time['utc_day']:02d}",
                "utc_time": f"{ist_time['utc_hour']:02d}:{ist_time['utc_minute']:02d}:{ist_time['utc_second']:02d}",
                "ist_date": f"{ist_time['year']:04d}-{ist_time['month']:02d}-{ist_time['day']:02d}",
                "ist_time": f"{ist_time['hour']:02d}:{ist_time['minute']:02d}:{ist_time['second']:02d}",
            },
        },
        "lagna": {
            "longitude": round(asc_sid, 4),
            "sign": lagna_sign_name,
            "sign_sanskrit": SIGNS_SANSKRIT[lagna_sign],
            "sign_index": lagna_sign,
            "deg": ld,
            "min": lm,
            "sec": round(ls, 2),
            "symbol": "As",
            "navamsa_sign": SIGNS[nav_asc_sign],
            "navamsa_sign_sanskrit": SIGNS_SANSKRIT[nav_asc_sign],
            "navamsa_sign_index": nav_asc_sign,
        },
        "planets": planets_out,
        "upagrahas": upagrahas,
        "rasi_chart": rasi_chart,
        "navamsa_chart": navamsa_chart,
        "shad_bala": shad_bala,
        "bhava_bala": bhava_bala,
        "dasha": dasha_data,
        "signs": SIGNS,
        "signs_sanskrit": SIGNS_SANSKRIT,
    }


def print_text_chart(k: Dict) -> None:
    print("\n=== Kundali (Vedic / Sidereal) ===")
    l = k["lagna"]
    print(f"Lagna: {l['sign']} {l['deg']}°{l['min']}'{l['sec']}\"")

    print("\nPlanets:")
    for pname in ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Rahu", "Ketu"]:
        p = k["planets"][pname]
        r = "R" if p["retrograde"] else "D"
        print(f"  {pname:8s} {p['sign']:11s} {p['deg']:2d}°{p['min']:02d}'  House:{p['house_whole_sign']:2d}  {r}")

    print("\nRāśi chart (sign -> planets):")
    for i, s in enumerate(SIGNS):
        pls = ", ".join(k["rasi_chart"].get(i, [])) if k["rasi_chart"].get(i) else "-"
        print(f"  {s:11s}: {pls}")


if __name__ == "__main__":
    b = BirthInput(
        year=1998, month=8, day=10,
        hour=14, minute=30, second=0,
        tz_offset_hours=5.5,
        latitude=19.0760,
        longitude=72.8777,
        ephe_path="./ephe",
    )

    k = kundali(b)
    print_text_chart(k)

    with open("kundali_output.json", "w", encoding="utf-8") as f:
        json.dump(k, f, indent=2)
    print("\nSaved: kundali_output.json")
