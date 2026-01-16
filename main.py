from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import swisseph as swe
import os
import json
import sqlite3
import threading
import uuid
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from kundali_maker import BirthInput, kundali, local_to_utc, utc_to_local

app = FastAPI(
    title="Kundali API",
    description="Vedic Astrology Birth Chart Generator using Swiss Ephemeris",
    version="1.0.0"
)


def _parse_cors_origins(raw: str) -> List[str]:
    origins = [o.strip() for o in (raw or "").split(",")]
    return [o for o in origins if o]


_cors_origins_env = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173",
)
_cors_origins = _parse_cors_origins(_cors_origins_env)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

EPHE_PATH = os.environ.get("EPHE_PATH", "./ephe")
DB_PATH = os.environ.get("DB_PATH", "./kundali.db")
_DB_LOCK = threading.Lock()


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _db_init() -> None:
    with _DB_LOCK:
        conn = _db_connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS charts (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    birth_data_json TEXT NOT NULL,
                    kundali_data_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    location_name TEXT,
                    latitude REAL,
                    longitude REAL,
                    timezone REAL
                );
                """
            )
            # Unique chart name per user (case-insensitive)
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_charts_user_name
                ON charts(user_id, name COLLATE NOCASE);
                """
            )
            conn.commit()
        finally:
            conn.close()


@app.on_event("startup")
def _startup() -> None:
    _db_init()


class KundaliRequest(BaseModel):
    year: int = Field(..., ge=1, le=3000, description="Birth year")
    month: int = Field(..., ge=1, le=12, description="Birth month")
    day: int = Field(..., ge=1, le=31, description="Birth day")
    hour: int = Field(..., ge=0, le=23, description="Birth hour (24h format)")
    minute: int = Field(..., ge=0, le=59, description="Birth minute")
    second: int = Field(0, ge=0, le=59, description="Birth second")
    tz_offset_hours: float = Field(..., ge=-12, le=14, description="Timezone offset in hours (e.g., 5.5 for IST)")
    latitude: float = Field(..., ge=-90, le=90, description="Birth place latitude")
    longitude: float = Field(..., ge=-180, le=180, description="Birth place longitude")
    ayanamsha: Optional[str] = Field("lahiri", description="Ayanamsha system: lahiri, raman, krishnamurti")
    use_utc: Optional[bool] = Field(False, description="If true, the provided time is already in UTC")

    class Config:
        json_schema_extra = {
            "example": {
                "year": 1998,
                "month": 8,
                "day": 10,
                "hour": 14,
                "minute": 30,
                "second": 0,
                "tz_offset_hours": 5.5,
                "latitude": 19.0760,
                "longitude": 72.8777,
                "ayanamsha": "lahiri"
            }
        }


class MatchRequest(BaseModel):
    person1: KundaliRequest
    person2: KundaliRequest
    person1_name: Optional[str] = Field("Person 1")
    person2_name: Optional[str] = Field("Person 2")


class MatchScoreItem(BaseModel):
    category: str
    score: float
    maxScore: float
    description: str


class MatchResponse(BaseModel):
    chart1: dict
    chart2: dict
    chart1_name: str
    chart2_name: str
    scores: List[MatchScoreItem]
    total_score: float
    total_max: float


class ChartCreateRequest(BaseModel):
    name: str
    birthData: KundaliRequest
    locationName: Optional[str] = None


class ChartUpdateRequest(BaseModel):
    name: str
    birthData: KundaliRequest
    locationName: Optional[str] = None


class ChartImportRequest(BaseModel):
    charts: List[Dict[str, Any]]


class ChartResponse(BaseModel):
    id: str
    name: str
    birthData: Dict[str, Any]
    kundaliData: Dict[str, Any]
    createdAt: str
    locationName: Optional[str] = None
    coordinates: Optional[Dict[str, Any]] = None


def _require_user_id(x_user_id: Optional[str]) -> str:
    uid = (x_user_id or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="Missing X-User-Id header")
    return uid


def _row_to_chart(row: sqlite3.Row) -> Dict[str, Any]:
    birth = {}
    kundali_data = {}
    try:
        birth = json.loads(row["birth_data_json"]) if row["birth_data_json"] else {}
    except Exception:
        birth = {}
    try:
        kundali_data = json.loads(row["kundali_data_json"]) if row["kundali_data_json"] else {}
    except Exception:
        kundali_data = {}

    coords = None
    if row["latitude"] is not None and row["longitude"] is not None and row["timezone"] is not None:
        coords = {"latitude": row["latitude"], "longitude": row["longitude"], "timezone": row["timezone"]}

    return {
        "id": row["id"],
        "name": row["name"],
        "birthData": birth,
        "kundaliData": kundali_data,
        "createdAt": row["created_at"],
        "locationName": row["location_name"],
        "coordinates": coords,
    }


class ReverseGeocodeResponse(BaseModel):
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    display_name: Optional[str] = None



class BalaCalculatorRequest(BaseModel):
    start_year: int = Field(..., ge=1, le=3000, description="Start year")
    end_year: int = Field(..., ge=1, le=3000, description="End year")
    latitude: float = Field(..., ge=-90, le=90, description="Birth place latitude")
    longitude: float = Field(..., ge=-180, le=180, description="Birth place longitude")
    tz_offset_hours: float = Field(..., ge=-12, le=14, description="Timezone offset in hours")
    ayanamsha: Optional[str] = Field("lahiri", description="Ayanamsha system: lahiri, raman, krishnamurti")
    include_hours: Optional[bool] = Field(True, description="Include hourly calculations")
    
    class Config:
        json_schema_extra = {
            "example": {
                "start_year": 2024,
                "end_year": 2024,
                "latitude": 19.0760,
                "longitude": 72.8777,
                "tz_offset_hours": 5.5,
                "ayanamsha": "lahiri",
                "include_hours": True
            }
        }


AYANAMSHA_MAP = {
    "lahiri": swe.SIDM_LAHIRI,
    "raman": swe.SIDM_RAMAN,
    "krishnamurti": swe.SIDM_KRISHNAMURTI,
}


RASHI_LORD = {
    "Aries": "Mars",
    "Taurus": "Venus",
    "Gemini": "Mercury",
    "Cancer": "Moon",
    "Leo": "Sun",
    "Virgo": "Mercury",
    "Libra": "Venus",
    "Scorpio": "Mars",
    "Sagittarius": "Jupiter",
    "Capricorn": "Saturn",
    "Aquarius": "Saturn",
    "Pisces": "Jupiter",
}


PLANET_FRIENDS = {
    "Sun": {"friends": {"Moon", "Mars", "Jupiter"}, "neutrals": {"Mercury"}, "enemies": {"Venus", "Saturn"}},
    "Moon": {"friends": {"Sun", "Mercury"}, "neutrals": {"Mars", "Jupiter", "Venus", "Saturn"}, "enemies": set()},
    "Mars": {"friends": {"Sun", "Moon", "Jupiter"}, "neutrals": {"Venus", "Saturn"}, "enemies": {"Mercury"}},
    "Mercury": {"friends": {"Sun", "Venus"}, "neutrals": {"Mars", "Jupiter", "Saturn"}, "enemies": {"Moon"}},
    "Jupiter": {"friends": {"Sun", "Moon", "Mars"}, "neutrals": {"Saturn"}, "enemies": {"Mercury", "Venus"}},
    "Venus": {"friends": {"Mercury", "Saturn"}, "neutrals": {"Mars", "Jupiter"}, "enemies": {"Sun", "Moon"}},
    "Saturn": {"friends": {"Mercury", "Venus"}, "neutrals": {"Jupiter"}, "enemies": {"Sun", "Moon", "Mars"}},
}


NAKSHATRA_NAMES = [
    "Ashwini", "Bharani", "Krittika", "Rohini", "Mrigashirsha", "Ardra", "Punarvasu",
    "Pushya", "Ashlesha", "Magha", "Purva Phalguni", "Uttara Phalguni", "Hasta", "Chitra",
    "Swati", "Vishakha", "Anuradha", "Jyeshtha", "Mula", "Purva Ashadha", "Uttara Ashadha",
    "Shravana", "Dhanishta", "Shatabhisha", "Purva Bhadrapada", "Uttara Bhadrapada", "Revati",
]


NAKSHATRA_GANA = {
    # Deva
    "Ashwini": "Deva", "Mrigashirsha": "Deva", "Punarvasu": "Deva", "Pushya": "Deva",
    "Hasta": "Deva", "Swati": "Deva", "Anuradha": "Deva", "Shravana": "Deva",
    "Revati": "Deva", "Uttara Phalguni": "Deva", "Uttara Ashadha": "Deva", "Uttara Bhadrapada": "Deva",
    # Manushya
    "Bharani": "Manushya", "Rohini": "Manushya", "Ardra": "Manushya", "Purva Phalguni": "Manushya",
    "Chitra": "Manushya", "Vishakha": "Manushya", "Jyeshtha": "Manushya", "Dhanishta": "Manushya",
    "Shatabhisha": "Manushya", "Purva Bhadrapada": "Manushya", "Purva Ashadha": "Manushya",
    # Rakshasa
    "Krittika": "Rakshasa", "Ashlesha": "Rakshasa", "Magha": "Rakshasa", "Mula": "Rakshasa",
    "Purva Phalguni": "Manushya", "Purva Ashadha": "Manushya", "Purva Bhadrapada": "Manushya",
    "Krittika": "Rakshasa", "Ashlesha": "Rakshasa", "Magha": "Rakshasa", "Mula": "Rakshasa",
    "Uttara Phalguni": "Deva", "Uttara Ashadha": "Deva", "Uttara Bhadrapada": "Deva",
    "Jyeshtha": "Manushya", "Vishakha": "Manushya", "Chitra": "Manushya", "Ardra": "Manushya",
    "Rohini": "Manushya", "Bharani": "Manushya",
    "Shatabhisha": "Manushya", "Dhanishta": "Manushya",
    "Mrigashirsha": "Deva", "Punarvasu": "Deva", "Pushya": "Deva", "Hasta": "Deva", "Swati": "Deva",
    "Anuradha": "Deva", "Shravana": "Deva", "Revati": "Deva",
    "Hasta": "Deva",
    "Chitra": "Manushya",
    "Swati": "Deva",
    "Vishakha": "Manushya",
    "Anuradha": "Deva",
    "Jyeshtha": "Manushya",
    "Mula": "Rakshasa",
    "Purva Ashadha": "Manushya",
    "Uttara Ashadha": "Deva",
    "Shravana": "Deva",
    "Dhanishta": "Manushya",
    "Shatabhisha": "Manushya",
    "Purva Bhadrapada": "Manushya",
    "Uttara Bhadrapada": "Deva",
    "Revati": "Deva",
}


NAKSHATRA_NADI = {
    # Aadi
    "Ashwini": "Aadi", "Ardra": "Aadi", "Punarvasu": "Aadi", "Uttara Phalguni": "Aadi", "Hasta": "Aadi",
    "Jyeshtha": "Aadi", "Mula": "Aadi", "Shravana": "Aadi", "Shatabhisha": "Aadi",
    # Madhya
    "Bharani": "Madhya", "Mrigashirsha": "Madhya", "Pushya": "Madhya", "Purva Phalguni": "Madhya", "Chitra": "Madhya",
    "Anuradha": "Madhya", "Purva Ashadha": "Madhya", "Dhanishta": "Madhya", "Purva Bhadrapada": "Madhya",
    # Antya
    "Krittika": "Antya", "Rohini": "Antya", "Ashlesha": "Antya", "Magha": "Antya", "Swati": "Antya",
    "Vishakha": "Antya", "Uttara Ashadha": "Antya", "Uttara Bhadrapada": "Antya", "Revati": "Antya",
}


NAKSHATRA_YONI = {
    # Common yoni mapping used in many Ashtakoota implementations.
    "Ashwini": "Horse",
    "Bharani": "Elephant",
    "Krittika": "Sheep",
    "Rohini": "Serpent",
    "Mrigashirsha": "Serpent",
    "Ardra": "Dog",
    "Punarvasu": "Cat",
    "Pushya": "Sheep",
    "Ashlesha": "Cat",
    "Magha": "Rat",
    "Purva Phalguni": "Rat",
    "Uttara Phalguni": "Cow",
    "Hasta": "Buffalo",
    "Chitra": "Tiger",
    "Swati": "Buffalo",
    "Vishakha": "Tiger",
    "Anuradha": "Deer",
    "Jyeshtha": "Deer",
    "Mula": "Dog",
    "Purva Ashadha": "Monkey",
    "Uttara Ashadha": "Mongoose",
    "Shravana": "Monkey",
    "Dhanishta": "Lion",
    "Shatabhisha": "Horse",
    "Purva Bhadrapada": "Lion",
    "Uttara Bhadrapada": "Cow",
    "Revati": "Elephant",
}


def _planet_relationship(p1: str, p2: str) -> str:
    rel = PLANET_FRIENDS.get(p1)
    if not rel:
        return "neutral"
    if p2 in rel["friends"]:
        return "friend"
    if p2 in rel["enemies"]:
        return "enemy"
    return "neutral"


def _tara_score(n1: int, n2: int) -> float:
    # n1, n2 are 1-27 (inclusive)
    # Count from n1 to n2 inclusive. Divide by 9.
    count = ((n2 - n1) % 27) + 1
    rem = count % 9
    # Common rule: 0 if remainder in {3,5,7}; else 3.
    return 0.0 if rem in {3, 5, 7} else 3.0


def _bhakoot_score(r1: int, r2: int) -> float:
    # r1,r2 are 0-11. Consider distance between signs.
    dist = ((r2 - r1) % 12) + 1  # 1..12
    # 2/12, 5/9, 6/8 are considered dosha in common systems.
    if dist in {2, 12, 5, 9, 6, 8}:
        return 0.0
    return 7.0


def _nadi_score(nadi1: str, nadi2: str) -> float:
    if not nadi1 or not nadi2:
        return 4.0
    return 0.0 if nadi1 == nadi2 else 8.0


def _gana_score(g1: str, g2: str) -> float:
    if not g1 or not g2:
        return 3.0
    if g1 == g2:
        return 6.0
    pair = {g1, g2}
    if pair == {"Deva", "Manushya"}:
        return 5.0
    if pair == {"Manushya", "Rakshasa"}:
        return 3.0
    # Deva + Rakshasa
    return 1.0


def _yoni_score(y1: str, y2: str) -> float:
    if not y1 or not y2:
        return 2.0
    if y1 == y2:
        return 4.0
    # Simplified compatibility: some pairs are hostile.
    hostile_pairs = {
        ("Cat", "Rat"), ("Rat", "Cat"),
        ("Dog", "Deer"), ("Deer", "Dog"),
        ("Lion", "Elephant"), ("Elephant", "Lion"),
        ("Serpent", "Mongoose"), ("Mongoose", "Serpent"),
        ("Monkey", "Sheep"), ("Sheep", "Monkey"),
        ("Tiger", "Cow"), ("Cow", "Tiger"),
    }
    if (y1, y2) in hostile_pairs:
        return 0.0
    return 3.0


def _varna_from_rashi(sign_name: str) -> str:
    # Common varna mapping used in matchmaking.
    if sign_name in {"Cancer", "Scorpio", "Pisces"}:
        return "Brahmin"
    if sign_name in {"Aries", "Leo", "Sagittarius"}:
        return "Kshatriya"
    if sign_name in {"Taurus", "Virgo", "Capricorn"}:
        return "Vaishya"
    return "Shudra"


def _varna_score(v1: str, v2: str) -> float:
    order = {"Shudra": 1, "Vaishya": 2, "Kshatriya": 3, "Brahmin": 4}
    if v1 not in order or v2 not in order:
        return 0.5
    return 1.0 if order[v2] >= order[v1] else 0.0


def _graha_maitri_score(sign1: str, sign2: str) -> float:
    lord1 = RASHI_LORD.get(sign1)
    lord2 = RASHI_LORD.get(sign2)
    if not lord1 or not lord2:
        return 2.5
    r12 = _planet_relationship(lord1, lord2)
    r21 = _planet_relationship(lord2, lord1)
    if r12 == "friend" and r21 == "friend":
        return 5.0
    if r12 == "enemy" and r21 == "enemy":
        return 0.0
    return 3.0


def _vashya_group(sign_name: str) -> str:
    # Standard-ish 5-group mapping used in many Ashtakoota (Vashya) tables.
    # Note: Classical texts sometimes split Sagittarius/Capricorn by degrees.
    # We only have the Moon sign (not degrees) here, so we use a sign-level mapping.
    if sign_name in {"Aries", "Taurus", "Sagittarius", "Capricorn"}:
        return "Chatushpada"
    if sign_name in {"Gemini", "Virgo", "Libra", "Aquarius"}:
        return "Manava"
    if sign_name in {"Cancer", "Pisces"}:
        return "Jalachara"
    if sign_name == "Leo":
        return "Vanachara"
    # Scorpio
    return "Keeta"


def _vashya_score(g1: str, g2: str) -> float:
    # Vashya max is 2. Classic systems are directional (Bride -> Groom).
    # Since we don't know bride/groom, we apply direction-aware scoring in the caller.
    if not g1 or not g2:
        return 1.0
    table = {
        "Chatushpada": {"Chatushpada": 2.0, "Manava": 1.0, "Jalachara": 1.0, "Vanachara": 1.5, "Keeta": 1.0},
        "Manava": {"Chatushpada": 1.0, "Manava": 2.0, "Jalachara": 1.5, "Vanachara": 0.0, "Keeta": 1.0},
        "Jalachara": {"Chatushpada": 1.0, "Manava": 1.5, "Jalachara": 2.0, "Vanachara": 1.0, "Keeta": 1.0},
        "Vanachara": {"Chatushpada": 0.0, "Manava": 0.0, "Jalachara": 0.0, "Vanachara": 2.0, "Keeta": 0.0},
        "Keeta": {"Chatushpada": 1.0, "Manava": 1.0, "Jalachara": 1.0, "Vanachara": 0.0, "Keeta": 2.0},
    }
    return float(table.get(g1, {}).get(g2, 1.0))


def _extract_moon_info(chart: dict) -> dict:
    moon = (chart.get("planets") or {}).get("Moon") or {}
    dasha = chart.get("dasha") or {}
    moon_sign = moon.get("sign")
    moon_sign_index = moon.get("sign_index")
    naksh_num = dasha.get("moon_nakshatra")
    naksh_name = dasha.get("moon_nakshatra_name")
    if not naksh_name and isinstance(naksh_num, int) and 1 <= naksh_num <= 27:
        naksh_name = NAKSHATRA_NAMES[naksh_num - 1]
    return {
        "moon_sign": moon_sign,
        "moon_sign_index": moon_sign_index,
        "nakshatra_num": naksh_num,
        "nakshatra_name": naksh_name,
    }


def _ashtakoota_scores(chart1: dict, chart2: dict) -> List[MatchScoreItem]:
    m1 = _extract_moon_info(chart1)
    m2 = _extract_moon_info(chart2)

    n1 = int(m1["nakshatra_num"]) if m1.get("nakshatra_num") else 0
    n2 = int(m2["nakshatra_num"]) if m2.get("nakshatra_num") else 0
    s1 = str(m1.get("moon_sign") or "")
    s2 = str(m2.get("moon_sign") or "")
    r1 = int(m1["moon_sign_index"]) if m1.get("moon_sign_index") is not None else -1
    r2 = int(m2["moon_sign_index"]) if m2.get("moon_sign_index") is not None else -1

    nak1 = str(m1.get("nakshatra_name") or "")
    nak2 = str(m2.get("nakshatra_name") or "")

    scores: List[MatchScoreItem] = []

    # Varna (1)
    v1 = _varna_from_rashi(s1)
    v2 = _varna_from_rashi(s2)
    # Varna is traditionally directional (bride vs groom). We average both directions.
    varna = (_varna_score(v1, v2) + _varna_score(v2, v1)) / 2.0
    scores.append(MatchScoreItem(
        category="Varna",
        score=varna,
        maxScore=1.0,
        description=f"Varna from Moon signs (avg both directions: {v1} vs {v2})",
    ))

    # Vashya (2)
    vg1 = _vashya_group(s1)
    vg2 = _vashya_group(s2)
    # Vashya is also commonly treated directionally; average both directions.
    vashya = (_vashya_score(vg1, vg2) + _vashya_score(vg2, vg1)) / 2.0
    scores.append(MatchScoreItem(
        category="Vashya",
        score=vashya,
        maxScore=2.0,
        description=f"Vashya groups (avg both directions: {vg1} vs {vg2})",
    ))

    # Tara (3)
    tara = 0.0
    if 1 <= n1 <= 27 and 1 <= n2 <= 27:
        # Tara is direction-based (counting from one nakshatra to the other).
        # Instead of the strict min() (which is very harsh), average both directions.
        tara = (_tara_score(n1, n2) + _tara_score(n2, n1)) / 2.0
    scores.append(MatchScoreItem(
        category="Tara",
        score=tara,
        maxScore=3.0,
        description=f"Tara based on nakshatras (avg both directions: {nak1} vs {nak2})",
    ))

    # Yoni (4)
    y1 = NAKSHATRA_YONI.get(nak1, "")
    y2 = NAKSHATRA_YONI.get(nak2, "")
    yoni = _yoni_score(y1, y2)
    scores.append(MatchScoreItem(
        category="Yoni",
        score=yoni,
        maxScore=4.0,
        description=f"Yoni animals ({y1 or 'Unknown'} vs {y2 or 'Unknown'})",
    ))

    # Graha Maitri (5)
    maitri = _graha_maitri_score(s1, s2)
    scores.append(MatchScoreItem(
        category="Graha Maitri",
        score=maitri,
        maxScore=5.0,
        description=f"Moon-sign lords friendship ({s1} vs {s2})",
    ))

    # Gana (6)
    g1 = NAKSHATRA_GANA.get(nak1, "")
    g2 = NAKSHATRA_GANA.get(nak2, "")
    gana = _gana_score(g1, g2)
    scores.append(MatchScoreItem(
        category="Gana",
        score=gana,
        maxScore=6.0,
        description=f"Gana ({g1 or 'Unknown'} vs {g2 or 'Unknown'})",
    ))

    # Bhakoot (7)
    bhakoot = 0.0
    if 0 <= r1 <= 11 and 0 <= r2 <= 11:
        bhakoot = _bhakoot_score(r1, r2)
    scores.append(MatchScoreItem(
        category="Bhakoot",
        score=bhakoot,
        maxScore=7.0,
        description=f"Bhakoot based on Moon-sign distance ({s1} vs {s2})",
    ))

    # Nadi (8)
    nd1 = NAKSHATRA_NADI.get(nak1, "")
    nd2 = NAKSHATRA_NADI.get(nak2, "")
    nadi = _nadi_score(nd1, nd2)
    scores.append(MatchScoreItem(
        category="Nadi",
        score=nadi,
        maxScore=8.0,
        description=f"Nadi ({nd1 or 'Unknown'} vs {nd2 or 'Unknown'})",
    ))

    total = sum(s.score for s in scores)
    scores.append(MatchScoreItem(
        category="Overall Compatibility",
        score=total,
        maxScore=36.0,
        description="Ashtakoota total (out of 36)",
    ))

    return scores



@app.get("/")
async def root():
    return {"message": "Kundali API - Vedic Birth Chart Generator", "docs": "/docs"}


@app.get("/health")
async def health():
    try:
        # Test database connection
        with _DB_LOCK:
            conn = _db_connect()
            conn.execute("SELECT 1")
            conn.close()
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "version": "1.0.0",
            "database": "connected"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "error": str(e)
        }


@app.post("/api/kundali")
async def generate_kundali(request: KundaliRequest):
    try:
        ayanamsha_code = AYANAMSHA_MAP.get(request.ayanamsha.lower(), swe.SIDM_LAHIRI)
        
        birth_input = BirthInput(
            year=request.year,
            month=request.month,
            day=request.day,
            hour=request.hour,
            minute=request.minute,
            second=request.second,
            tz_offset_hours=request.tz_offset_hours,
            latitude=request.latitude,
            longitude=request.longitude,
            ephe_path=EPHE_PATH,
            ayanamsha=ayanamsha_code,
            use_utc=request.use_utc or False
        )
        
        result = kundali(birth_input)
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/charts", response_model=List[ChartResponse])
async def list_charts(x_user_id: Optional[str] = Header(default=None, alias="X-User-Id")):
    user_id = _require_user_id(x_user_id)
    with _DB_LOCK:
        conn = _db_connect()
        try:
            cur = conn.execute(
                "SELECT * FROM charts WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            )
            rows = cur.fetchall()
            return [_row_to_chart(r) for r in rows]
        finally:
            conn.close()


@app.post("/api/charts", response_model=ChartResponse)
async def create_chart(
    payload: ChartCreateRequest,
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
):
    user_id = _require_user_id(x_user_id)
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Chart name is required")

    # Compute kundali to store
    ay = AYANAMSHA_MAP.get((payload.birthData.ayanamsha or "lahiri").lower(), swe.SIDM_LAHIRI)
    b = BirthInput(
        year=payload.birthData.year,
        month=payload.birthData.month,
        day=payload.birthData.day,
        hour=payload.birthData.hour,
        minute=payload.birthData.minute,
        second=payload.birthData.second,
        tz_offset_hours=payload.birthData.tz_offset_hours,
        latitude=payload.birthData.latitude,
        longitude=payload.birthData.longitude,
        ephe_path=EPHE_PATH,
        ayanamsha=ay,
        use_utc=payload.birthData.use_utc or False,
    )
    kundali_data = kundali(b)

    chart_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat() + "Z"
    birth_json = payload.birthData.model_dump()
    loc_name = payload.locationName
    lat = float(payload.birthData.latitude)
    lon = float(payload.birthData.longitude)
    tz = float(payload.birthData.tz_offset_hours)

    with _DB_LOCK:
        conn = _db_connect()
        try:
            try:
                conn.execute(
                    """
                    INSERT INTO charts (
                        id, user_id, name, birth_data_json, kundali_data_json, created_at,
                        location_name, latitude, longitude, timezone
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chart_id,
                        user_id,
                        name,
                        json.dumps(birth_json),
                        json.dumps(kundali_data),
                        created_at,
                        loc_name,
                        lat,
                        lon,
                        tz,
                    ),
                )
                conn.commit()
            except sqlite3.IntegrityError:
                raise HTTPException(status_code=409, detail=f'A chart named "{name}" already exists')

            row = {
                "id": chart_id,
                "user_id": user_id,
                "name": name,
                "birth_data_json": json.dumps(birth_json),
                "kundali_data_json": json.dumps(kundali_data),
                "created_at": created_at,
                "location_name": loc_name,
                "latitude": lat,
                "longitude": lon,
                "timezone": tz,
            }
            return _row_to_chart(row)  # type: ignore[arg-type]
        finally:
            conn.close()


@app.put("/api/charts/{chart_id}", response_model=ChartResponse)
async def update_chart(
    chart_id: str,
    payload: ChartUpdateRequest,
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
):
    user_id = _require_user_id(x_user_id)
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Chart name is required")

    ay = AYANAMSHA_MAP.get((payload.birthData.ayanamsha or "lahiri").lower(), swe.SIDM_LAHIRI)
    b = BirthInput(
        year=payload.birthData.year,
        month=payload.birthData.month,
        day=payload.birthData.day,
        hour=payload.birthData.hour,
        minute=payload.birthData.minute,
        second=payload.birthData.second,
        tz_offset_hours=payload.birthData.tz_offset_hours,
        latitude=payload.birthData.latitude,
        longitude=payload.birthData.longitude,
        ephe_path=EPHE_PATH,
        ayanamsha=ay,
        use_utc=payload.birthData.use_utc or False,
    )
    kundali_data = kundali(b)

    birth_json = payload.birthData.model_dump()
    loc_name = payload.locationName
    lat = float(payload.birthData.latitude)
    lon = float(payload.birthData.longitude)
    tz = float(payload.birthData.tz_offset_hours)

    with _DB_LOCK:
        conn = _db_connect()
        try:
            try:
                cur = conn.execute(
                    """
                    UPDATE charts
                    SET name = ?, birth_data_json = ?, kundali_data_json = ?, location_name = ?,
                        latitude = ?, longitude = ?, timezone = ?
                    WHERE id = ? AND user_id = ?
                    """,
                    (
                        name,
                        json.dumps(birth_json),
                        json.dumps(kundali_data),
                        loc_name,
                        lat,
                        lon,
                        tz,
                        chart_id,
                        user_id,
                    ),
                )
                conn.commit()
            except sqlite3.IntegrityError:
                raise HTTPException(status_code=409, detail=f'A chart named "{name}" already exists')

            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Chart not found")

            row = conn.execute("SELECT * FROM charts WHERE id = ? AND user_id = ?", (chart_id, user_id)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Chart not found")
            return _row_to_chart(row)
        finally:
            conn.close()


@app.delete("/api/charts/{chart_id}")
async def delete_chart(
    chart_id: str,
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
):
    user_id = _require_user_id(x_user_id)
    with _DB_LOCK:
        conn = _db_connect()
        try:
            cur = conn.execute("DELETE FROM charts WHERE id = ? AND user_id = ?", (chart_id, user_id))
            conn.commit()
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Chart not found")
            return {"ok": True}
        finally:
            conn.close()


@app.post("/api/charts/import")
async def import_charts(
    payload: ChartImportRequest,
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
):
    """Import charts from client (one-time localStorage migration). Skips duplicates by name."""
    user_id = _require_user_id(x_user_id)
    imported = 0
    skipped = 0

    with _DB_LOCK:
        conn = _db_connect()
        try:
            for c in payload.charts:
                try:
                    name = str(c.get("name", "")).strip()
                    birth_data = c.get("birthData")
                    kundali_data = c.get("kundaliData")
                    created_at = str(c.get("createdAt") or datetime.utcnow().isoformat() + "Z")
                    location_name = c.get("locationName")
                    coords = c.get("coordinates") or {}

                    if not name or not isinstance(birth_data, dict) or not isinstance(kundali_data, dict):
                        skipped += 1
                        continue

                    chart_id = str(uuid.uuid4())
                    lat = coords.get("latitude")
                    lon = coords.get("longitude")
                    tz = coords.get("timezone")

                    try:
                        conn.execute(
                            """
                            INSERT INTO charts (
                                id, user_id, name, birth_data_json, kundali_data_json, created_at,
                                location_name, latitude, longitude, timezone
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                chart_id,
                                user_id,
                                name,
                                json.dumps(birth_data),
                                json.dumps(kundali_data),
                                created_at,
                                location_name,
                                lat,
                                lon,
                                tz,
                            ),
                        )
                        imported += 1
                    except sqlite3.IntegrityError:
                        skipped += 1
                except Exception:
                    skipped += 1
            conn.commit()
        finally:
            conn.close()

    return {"imported": imported, "skipped": skipped}


@app.post("/api/match", response_model=MatchResponse)
async def match_kundalis(request: MatchRequest):
    try:
        # Generate charts
        ay1 = AYANAMSHA_MAP.get((request.person1.ayanamsha or "lahiri").lower(), swe.SIDM_LAHIRI)
        ay2 = AYANAMSHA_MAP.get((request.person2.ayanamsha or "lahiri").lower(), swe.SIDM_LAHIRI)

        b1 = BirthInput(
            year=request.person1.year,
            month=request.person1.month,
            day=request.person1.day,
            hour=request.person1.hour,
            minute=request.person1.minute,
            second=request.person1.second,
            tz_offset_hours=request.person1.tz_offset_hours,
            latitude=request.person1.latitude,
            longitude=request.person1.longitude,
            ephe_path=EPHE_PATH,
            ayanamsha=ay1,
            use_utc=request.person1.use_utc or False,
        )

        b2 = BirthInput(
            year=request.person2.year,
            month=request.person2.month,
            day=request.person2.day,
            hour=request.person2.hour,
            minute=request.person2.minute,
            second=request.person2.second,
            tz_offset_hours=request.person2.tz_offset_hours,
            latitude=request.person2.latitude,
            longitude=request.person2.longitude,
            ephe_path=EPHE_PATH,
            ayanamsha=ay2,
            use_utc=request.person2.use_utc or False,
        )

        chart1 = kundali(b1)
        chart2 = kundali(b2)

        scores = _ashtakoota_scores(chart1, chart2)
        total_score = float(sum(s.score for s in scores if s.category != "Overall Compatibility"))
        total_max = float(sum(s.maxScore for s in scores if s.category != "Overall Compatibility"))

        return MatchResponse(
            chart1=chart1,
            chart2=chart2,
            chart1_name=request.person1_name or "Person 1",
            chart2_name=request.person2_name or "Person 2",
            scores=scores,
            total_score=total_score,
            total_max=total_max,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/api/bala-calculator")
async def calculate_bala_range(request: BalaCalculatorRequest):
    """Calculate Shad Bala and Bhava Bala for each hour in a given year range."""
    try:
        ayanamsha_code = AYANAMSHA_MAP.get(request.ayanamsha.lower(), swe.SIDM_LAHIRI)
        results = []
        
        start_date = datetime(request.start_year, 1, 1)
        end_date = datetime(request.end_year, 12, 31, 23, 59, 59)
        
        current_date = start_date
        total_hours = 0
        
        while current_date <= end_date:
            if request.include_hours:
                # Calculate for every hour of the day
                for hour in range(0, 24, 1):
                    try:
                        birth_input = BirthInput(
                            year=current_date.year,
                            month=current_date.month,
                            day=current_date.day,
                            hour=hour,
                            minute=0,
                            second=0,
                            tz_offset_hours=request.tz_offset_hours,
                            latitude=request.latitude,
                            longitude=request.longitude,
                            ephe_path=EPHE_PATH,
                            ayanamsha=ayanamsha_code
                        )
                        
                        result = kundali(birth_input)
                        
                        # Extract Shad Bala totals (use Rupas if available)
                        shad_bala_totals = {}
                        total_shad_bala = 0.0
                        for planet, bala in result.get('shad_bala', {}).items():
                            val_rupas = (
                                bala.get('total_rupas')
                                if isinstance(bala, dict) and bala.get('total_rupas') is not None
                                else (
                                    (bala.get('total_shashtiamsas', 0.0) / 60.0)
                                    if isinstance(bala, dict)
                                    else 0.0
                                )
                            )
                            shad_bala_totals[planet] = round(val_rupas, 2)
                            total_shad_bala += val_rupas
                        
                        # Extract Bhava Bala totals (use Rupas if available)
                        bhava_bala_totals = {}
                        total_bhava_bala = 0.0
                        for house, bala in result.get('bhava_bala', {}).items():
                            val_rupas = (
                                bala.get('total_rupas')
                                if isinstance(bala, dict) and bala.get('total_rupas') is not None
                                else (
                                    (bala.get('total_shashtiamsas', 0.0) / 60.0)
                                    if isinstance(bala, dict)
                                    else 0.0
                                )
                            )
                            bhava_bala_totals[f"House_{house}"] = round(val_rupas, 2)
                            total_bhava_bala += val_rupas
                        
                        results.append({
                            "datetime": current_date.replace(hour=hour).isoformat(),
                            "shad_bala": {
                                "totals": shad_bala_totals,
                                "total": total_shad_bala
                            },
                            "bhava_bala": {
                                "totals": bhava_bala_totals,
                                "total": total_bhava_bala
                            }
                        })
                        
                        total_hours += 1
                        
                                                    
                    except Exception as e:
                        # Skip problematic hours but continue
                        continue
            else:
                # Calculate only once per day (noon)
                try:
                    birth_input = BirthInput(
                        year=current_date.year,
                        month=current_date.month,
                        day=current_date.day,
                        hour=12,
                        minute=0,
                        second=0,
                        tz_offset_hours=request.tz_offset_hours,
                        latitude=request.latitude,
                        longitude=request.longitude,
                        ephe_path=EPHE_PATH,
                        ayanamsha=ayanamsha_code
                    )
                    
                    result = kundali(birth_input)
                    
                    # Extract Shad Bala totals (use Rupas if available)
                    shad_bala_totals = {}
                    total_shad_bala = 0.0
                    for planet, bala in result.get('shad_bala', {}).items():
                        val_rupas = (
                            bala.get('total_rupas')
                            if isinstance(bala, dict) and bala.get('total_rupas') is not None
                            else (
                                (bala.get('total_shashtiamsas', 0.0) / 60.0)
                                if isinstance(bala, dict)
                                else 0.0
                            )
                        )
                        shad_bala_totals[planet] = round(val_rupas, 2)
                        total_shad_bala += val_rupas
                    
                    # Extract Bhava Bala totals (use Rupas if available)
                    bhava_bala_totals = {}
                    total_bhava_bala = 0.0
                    for house, bala in result.get('bhava_bala', {}).items():
                        val_rupas = (
                            bala.get('total_rupas')
                            if isinstance(bala, dict) and bala.get('total_rupas') is not None
                            else (
                                (bala.get('total_shashtiamsas', 0.0) / 60.0)
                                if isinstance(bala, dict)
                                else 0.0
                            )
                        )
                        bhava_bala_totals[f"House_{house}"] = round(val_rupas, 2)
                        total_bhava_bala += val_rupas
                    
                    results.append({
                        "datetime": current_date.isoformat(),
                        "shad_bala": {
                            "totals": shad_bala_totals,
                            "total": total_shad_bala
                        },
                        "bhava_bala": {
                            "totals": bhava_bala_totals,
                            "total": total_bhava_bala
                        }
                    })
                    
                except Exception as e:
                    # Skip problematic days but continue
                    pass
            
            current_date += timedelta(days=1)
        
        return {
            "request_params": {
                "start_year": request.start_year,
                "end_year": request.end_year,
                "latitude": request.latitude,
                "longitude": request.longitude,
                "tz_offset_hours": request.tz_offset_hours,
                "ayanamsha": request.ayanamsha,
                "include_hours": request.include_hours
            },
            "total_calculations": len(results),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class TimeConvertRequest(BaseModel):
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int = 0
    tz_offset_hours: float
    direction: str = Field("to_utc", description="'to_utc' or 'to_local'")


@app.post("/api/convert-time")
async def convert_time(request: TimeConvertRequest):
    """Convert time between local and UTC."""
    try:
        if request.direction == "to_utc":
            result = local_to_utc(
                request.year, request.month, request.day,
                request.hour, request.minute, request.second,
                request.tz_offset_hours
            )
            return {
                "input": {
                    "year": request.year, "month": request.month, "day": request.day,
                    "hour": request.hour, "minute": request.minute, "second": request.second,
                    "tz_offset_hours": request.tz_offset_hours,
                    "type": "local"
                },
                "output": {**result, "type": "utc"}
            }
        else:
            result = utc_to_local(
                request.year, request.month, request.day,
                request.hour, request.minute, request.second,
                request.tz_offset_hours
            )
            return {
                "input": {
                    "year": request.year, "month": request.month, "day": request.day,
                    "hour": request.hour, "minute": request.minute, "second": request.second,
                    "tz_offset_hours": request.tz_offset_hours,
                    "type": "utc"
                },
                "output": {**result, "type": "local"}
            }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/timezones")
async def get_common_timezones():
    """Return common timezone offsets for reference"""
    return {
        "timezones": [
            {"name": "IST (India)", "offset": 5.5},
            {"name": "UTC", "offset": 0},
            {"name": "EST (US Eastern)", "offset": -5},
            {"name": "PST (US Pacific)", "offset": -8},
            {"name": "GMT", "offset": 0},
            {"name": "CET (Central Europe)", "offset": 1},
            {"name": "JST (Japan)", "offset": 9},
            {"name": "AEST (Australia Eastern)", "offset": 10},
        ]
    }


@app.get("/api/reverse-geocode", response_model=ReverseGeocodeResponse)
async def reverse_geocode(lat: float, lon: float):
    """Reverse geocode coordinates using Nominatim.

    Done on backend to avoid browser CORS and improve reliability.
    """
    try:
        query = urllib.parse.urlencode({"format": "json", "lat": str(lat), "lon": str(lon)})
        url = f"https://nominatim.openstreetmap.org/reverse?{query}"
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "kundali-app/1.0 (local)",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            raw = resp.read().decode("utf-8")
        data = json.loads(raw) if raw else {}
        address = data.get("address", {}) if isinstance(data, dict) else {}
        city = address.get("city") or address.get("town") or address.get("village")
        return ReverseGeocodeResponse(
            city=city,
            state=address.get("state"),
            country=address.get("country"),
            display_name=data.get("display_name") if isinstance(data, dict) else None,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
