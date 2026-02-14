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
    # Deva (divine temperament)
    "Ashwini": "Deva",
    "Mrigashirsha": "Deva",
    "Punarvasu": "Deva",
    "Pushya": "Deva",
    "Hasta": "Deva",
    "Swati": "Deva",
    "Anuradha": "Deva",
    "Shravana": "Deva",
    "Revati": "Deva",
    # Manushya (human temperament)
    "Bharani": "Manushya",
    "Rohini": "Manushya",
    "Ardra": "Manushya",
    "Purva Phalguni": "Manushya",
    "Uttara Phalguni": "Manushya",
    "Chitra": "Manushya",
    "Vishakha": "Manushya",
    "Jyeshtha": "Manushya",
    "Purva Ashadha": "Manushya",
    "Uttara Ashadha": "Manushya",
    "Dhanishta": "Manushya",
    "Shatabhisha": "Manushya",
    "Purva Bhadrapada": "Manushya",
    "Uttara Bhadrapada": "Manushya",
    # Rakshasa (demon temperament)
    "Krittika": "Rakshasa",
    "Ashlesha": "Rakshasa",
    "Magha": "Rakshasa",
    "Mula": "Rakshasa",
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


class GeocodeResponse(BaseModel):
    display_name: str
    lat: float
    lon: float


@app.get("/api/geocode", response_model=List[GeocodeResponse])
async def geocode(query: str):
    """Forward geocode location name using Nominatim.
    
    Done on backend to avoid browser CORS and improve reliability.
    """
    try:
        # Use Nominatim API for forward geocoding
        url = f"https://nominatim.openstreetmap.org/search?format=json&q={encodeURIComponent(query)}&limit=5"
        headers = {
            "User-Agent": "Astrova Kundali App (https://astrova.magnova.ai)"
        }
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            results = []
            for item in data:
                results.append(GeocodeResponse(
                    display_name=item.get("display_name", ""),
                    lat=float(item["lat"]),
                    lon=float(item["lon"])
                ))
            
            return results
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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


class ChatRequest(BaseModel):
    message: str
    kundali_data: Optional[Dict[str, Any]] = None
    chart_name: Optional[str] = None
    conversation_history: Optional[List[Dict[str, str]]] = None


OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")

ASTRO_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_planet_positions",
            "description": "Get all planet positions with sign, house, degree, and status (retrograde, exalted, debilitated, combust, vargottama). Use this to answer questions about planets, their placements, and planetary combinations (yogas).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dasha_periods",
            "description": "Get Vimshottari Dasha periods including current Mahadasha, Antardasha, and Pratyantardasha with start/end dates. Use this for timing predictions, current period analysis, and future forecasts.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_house_analysis",
            "description": "Get Bhava Bala (house strengths) with ratings, lords, and scores for all 12 houses. Use this for questions about specific life areas like career (10th), marriage (7th), wealth (2nd/11th), health (6th), etc.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_planetary_strengths",
            "description": "Get Shad Bala (six-fold strength) for each planet including total strength, required strength, strength ratio, and whether the planet is strong or weak. Use this for questions about planetary power and influence.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_ascendant_info",
            "description": "Get Lagna (Ascendant) details including sign, degree, nakshatra, and navamsa. Use this for personality, appearance, and general life direction questions.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_nakshatra_and_birth_info",
            "description": "Get birth nakshatra, Moon sign, birth details (date, time, location), and basic chart identification. Use this for questions about nakshatra characteristics, birth star, and general identity.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_matching_compatibility",
            "description": "Get Ashtakoota matching scores if two charts are being compared. Returns Varna, Vashya, Tara, Yoni, Graha Maitri, Gana, Bhakoot, and Nadi scores. Use this only for compatibility/matching questions.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]


def _execute_tool(tool_name: str, kundali_data: dict) -> str:
    """Execute a tool call and return the relevant kundali data as JSON string."""
    if tool_name == "get_planet_positions":
        planets = kundali_data.get("planets", {})
        result = {}
        for name, info in planets.items():
            if isinstance(info, dict):
                result[name] = {
                    "sign": info.get("sign"),
                    "house": info.get("house_whole_sign"),
                    "degree": f"{info.get('deg', 0)}°{info.get('min', 0)}'",
                    "retrograde": info.get("retrograde", False),
                    "exalted": info.get("exalted", False),
                    "debilitated": info.get("debilitated", False),
                    "combust": info.get("combust", False),
                    "vargottama": info.get("vargottama", False),
                    "navamsa_sign": info.get("navamsa_sign"),
                    "nakshatra": info.get("nakshatra"),
                }
        upagrahas = kundali_data.get("upagrahas", {})
        for name, info in upagrahas.items():
            if isinstance(info, dict):
                result[name] = {
                    "sign": info.get("sign"),
                    "house": info.get("house_whole_sign"),
                    "degree": f"{info.get('deg', 0)}°{info.get('min', 0)}'",
                    "type": "upagraha",
                }
        return json.dumps(result, indent=2)

    elif tool_name == "get_dasha_periods":
        dasha = kundali_data.get("dasha", {})
        result = {
            "current_dasha": dasha.get("current_dasha"),
            "moon_nakshatra": dasha.get("moon_nakshatra_name"),
            "moon_nakshatra_pada": dasha.get("moon_nakshatra_pada"),
            "periods": [],
        }
        for p in dasha.get("periods", []):
            if isinstance(p, dict):
                period = {
                    "planet": p.get("planet"),
                    "start_date": p.get("start_date"),
                    "end_date": p.get("end_date"),
                    "is_current": p.get("is_current", False),
                }
                antardashas = []
                for ad in p.get("antardashas", []):
                    if isinstance(ad, dict):
                        antardashas.append({
                            "planet": ad.get("planet"),
                            "start_date": ad.get("start_date"),
                            "end_date": ad.get("end_date"),
                            "is_current": ad.get("is_current", False),
                        })
                period["antardashas"] = antardashas
                result["periods"].append(period)
        return json.dumps(result, indent=2)

    elif tool_name == "get_house_analysis":
        bhava = kundali_data.get("bhava_bala", {})
        result = {}
        for house, info in bhava.items():
            if isinstance(info, dict):
                result[f"House {house}"] = {
                    "lord": info.get("lord"),
                    "total_strength": info.get("total"),
                    "rating": info.get("rating"),
                    "dig_bala": info.get("dig_bala"),
                    "drishti_bala": info.get("drishti_bala"),
                }
        return json.dumps(result, indent=2)

    elif tool_name == "get_planetary_strengths":
        shad = kundali_data.get("shad_bala", {})
        result = {}
        for name, info in shad.items():
            if isinstance(info, dict):
                result[name] = {
                    "total_rupas": info.get("total"),
                    "required_rupas": info.get("required"),
                    "strength_ratio": info.get("ratio"),
                    "is_strong": info.get("is_strong"),
                    "strength_label": info.get("strength"),
                    "components": {
                        "sthana_bala": info.get("sthana_bala"),
                        "dig_bala": info.get("dig_bala"),
                        "kala_bala": info.get("kala_bala"),
                        "chesta_bala": info.get("chesta_bala"),
                        "naisargika_bala": info.get("naisargika_bala"),
                        "drik_bala": info.get("drik_bala"),
                    },
                }
        return json.dumps(result, indent=2)

    elif tool_name == "get_ascendant_info":
        lagna = kundali_data.get("lagna", {})
        return json.dumps({
            "sign": lagna.get("sign"),
            "degree": f"{lagna.get('deg', 0)}°{lagna.get('min', 0)}'",
            "nakshatra": lagna.get("nakshatra"),
            "navamsa_sign": lagna.get("navamsa_sign"),
            "house": lagna.get("house_whole_sign"),
        }, indent=2)

    elif tool_name == "get_nakshatra_and_birth_info":
        dasha = kundali_data.get("dasha", {})
        birth = kundali_data.get("birth", {})
        moon = kundali_data.get("planets", {}).get("Moon", {})
        return json.dumps({
            "moon_nakshatra": dasha.get("moon_nakshatra_name"),
            "moon_nakshatra_pada": dasha.get("moon_nakshatra_pada"),
            "moon_sign": moon.get("sign") if isinstance(moon, dict) else None,
            "birth_date": birth.get("date"),
            "birth_time": birth.get("time"),
            "birth_location": birth.get("location"),
        }, indent=2)

    elif tool_name == "get_matching_compatibility":
        return json.dumps({"note": "No matching data available in current chart. Load two charts and use the Kundali Matcher for compatibility analysis."})

    return json.dumps({"error": f"Unknown tool: {tool_name}"})


def _call_openrouter(messages: list, tools: list) -> dict:
    """Call OpenRouter API with messages and tools."""
    payload = json.dumps({
        "model": "stepfun/step-2-16k",
        "messages": messages,
        "tools": tools,
        "tool_choice": "auto",
        "temperature": 0.7,
        "max_tokens": 2048,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://astrova.app",
            "X-Title": "Astrova Vedic Astrologer",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


SYSTEM_PROMPT = """You are an expert Vedic astrologer AI for the Astrova app. You have deep knowledge of:
- Jyotish Shastra (Vedic Astrology) including Parashari and Jaimini systems
- Vimshottari Dasha system (Mahadasha, Antardasha, Pratyantardasha)
- Shad Bala (six-fold planetary strength) and Bhava Bala (house strength)
- Ashtakoota matching system for compatibility
- Nakshatras, their padas, and characteristics
- Planetary yogas, aspects, and combinations
- Remedial measures (mantras, gemstones, charity, rituals)

IMPORTANT RULES:
1. ALWAYS use the available tools to read the actual chart data before answering. Never guess placements.
2. Provide specific, personalized readings based on the actual chart data.
3. Reference specific planets, houses, signs, and degrees from the data.
4. Explain astrological concepts in an accessible way.
5. Be encouraging but honest about challenges shown in the chart.
6. For timing questions, reference specific Dasha periods with dates.
7. Format responses with **bold** headers and clear structure.
8. When discussing remedies, mention they should be done after consulting a qualified astrologer.
9. Use the person's chart name when addressing them.
"""


def _generate_astro_insights(kundali_data: dict) -> dict:
    """Extract key astrological insights from kundali data."""
    insights = {}

    # Lagna
    lagna = kundali_data.get("lagna", {})
    insights["ascendant"] = lagna.get("sign", "Unknown")
    insights["ascendant_degree"] = f"{lagna.get('deg', 0)}°{lagna.get('min', 0)}'"

    # Planets
    planets = kundali_data.get("planets", {})
    planet_summary = []
    for name, info in planets.items():
        if not isinstance(info, dict):
            continue
        status = []
        if info.get("retrograde"):
            status.append("retrograde")
        if info.get("exalted"):
            status.append("exalted")
        if info.get("debilitated"):
            status.append("debilitated")
        if info.get("combust"):
            status.append("combust")
        if info.get("vargottama"):
            status.append("vargottama")
        planet_summary.append({
            "name": name,
            "sign": info.get("sign", ""),
            "house": info.get("house_whole_sign", 0),
            "degree": f"{info.get('deg', 0)}°{info.get('min', 0)}'",
            "status": status,
            "navamsa_sign": info.get("navamsa_sign", ""),
        })
    insights["planets"] = planet_summary

    # Moon info
    moon = planets.get("Moon", {})
    insights["moon_sign"] = moon.get("sign", "Unknown")
    insights["moon_house"] = moon.get("house_whole_sign", 0)

    # Sun info
    sun = planets.get("Sun", {})
    insights["sun_sign"] = sun.get("sign", "Unknown")

    # Dasha
    dasha = kundali_data.get("dasha", {})
    insights["current_dasha"] = dasha.get("current_dasha", "Unknown")
    insights["nakshatra"] = dasha.get("moon_nakshatra_name", "Unknown")
    insights["nakshatra_pada"] = dasha.get("moon_nakshatra_pada", 0)

    # Current dasha period details
    periods = dasha.get("periods", [])
    current_period = None
    for p in periods:
        if isinstance(p, dict) and p.get("is_current"):
            current_period = p
            break
    if current_period:
        insights["current_mahadasha"] = current_period.get("planet", "")
        insights["mahadasha_end"] = current_period.get("end_date", "")
        # Current antardasha
        antardashas = current_period.get("antardashas", [])
        for ad in antardashas:
            if isinstance(ad, dict):
                # Find current antardasha by checking dates
                insights["antardashas_count"] = len(antardashas)

    # Shad Bala summary
    shad_bala = kundali_data.get("shad_bala", {})
    strong_planets = []
    weak_planets = []
    for name, bala in shad_bala.items():
        if not isinstance(bala, dict):
            continue
        if bala.get("is_strong"):
            strong_planets.append(name)
        elif bala.get("strength") == "Weak":
            weak_planets.append(name)
    insights["strong_planets"] = strong_planets
    insights["weak_planets"] = weak_planets

    # Bhava Bala summary
    bhava_bala = kundali_data.get("bhava_bala", {})
    strong_houses = []
    weak_houses = []
    for house, bala in bhava_bala.items():
        if not isinstance(bala, dict):
            continue
        rating = bala.get("rating", "")
        if rating in ("Very Strong", "Strong"):
            strong_houses.append({"house": house, "lord": bala.get("lord", ""), "rating": rating})
        elif rating == "Weak":
            weak_houses.append({"house": house, "lord": bala.get("lord", ""), "rating": rating})
    insights["strong_houses"] = strong_houses
    insights["weak_houses"] = weak_houses

    return insights


def _build_astro_response(message: str, insights: dict, chart_name: str) -> str:
    """Build a comprehensive astrological response based on the message and chart data."""
    msg = message.lower().strip()
    name = chart_name or "this person"
    asc = insights.get("ascendant", "Unknown")
    moon_sign = insights.get("moon_sign", "Unknown")
    sun_sign = insights.get("sun_sign", "Unknown")
    nakshatra = insights.get("nakshatra", "Unknown")
    current_dasha = insights.get("current_dasha", "Unknown")
    strong = insights.get("strong_planets", [])
    weak = insights.get("weak_planets", [])

    # Personality descriptions by ascendant
    asc_traits = {
        "Aries": "bold, pioneering, and action-oriented. Natural leaders with strong willpower and competitive spirit.",
        "Taurus": "grounded, patient, and value-driven. They seek stability, comfort, and have a strong aesthetic sense.",
        "Gemini": "intellectually curious, communicative, and adaptable. Quick-witted with diverse interests.",
        "Cancer": "nurturing, emotionally intuitive, and deeply connected to family. Strong protective instincts.",
        "Leo": "charismatic, creative, and confident. Natural performers who seek recognition and self-expression.",
        "Virgo": "analytical, detail-oriented, and service-minded. Perfectionists with strong practical intelligence.",
        "Libra": "diplomatic, harmony-seeking, and relationship-oriented. Strong sense of justice and beauty.",
        "Scorpio": "intense, transformative, and deeply perceptive. Powerful emotional depth and investigative nature.",
        "Sagittarius": "optimistic, philosophical, and freedom-loving. Seekers of truth and higher knowledge.",
        "Capricorn": "disciplined, ambitious, and responsible. Strong work ethic with long-term vision.",
        "Aquarius": "innovative, humanitarian, and independent. Progressive thinkers who value individuality.",
        "Pisces": "compassionate, intuitive, and spiritually inclined. Deeply empathetic with artistic sensibilities.",
    }

    # Moon sign emotional nature
    moon_traits = {
        "Aries": "emotionally impulsive and passionate. Quick to react but also quick to forgive.",
        "Taurus": "emotionally stable and comfort-seeking. Needs security and routine for inner peace.",
        "Gemini": "emotionally versatile and intellectually driven. Processes feelings through communication.",
        "Cancer": "deeply emotional and nurturing. Highly sensitive to the moods of others.",
        "Leo": "emotionally warm and generous. Needs appreciation and creative expression.",
        "Virgo": "emotionally reserved and analytical. Processes feelings through practical action.",
        "Libra": "emotionally balanced and relationship-focused. Seeks harmony in all interactions.",
        "Scorpio": "emotionally intense and transformative. Deep feelings that run beneath the surface.",
        "Sagittarius": "emotionally optimistic and freedom-loving. Needs space and adventure.",
        "Capricorn": "emotionally disciplined and reserved. Takes time to open up but deeply loyal.",
        "Aquarius": "emotionally detached yet humanitarian. Values intellectual connection over emotional.",
        "Pisces": "emotionally empathetic and intuitive. Absorbs the feelings of those around them.",
    }

    # Dasha interpretations
    dasha_meanings = {
        "Sun": "a period of authority, self-expression, and career advancement. Government and father-related matters are highlighted.",
        "Moon": "a period of emotional growth, public life, and maternal influences. Travel and mental peace are key themes.",
        "Mars": "a period of energy, courage, and action. Property matters, siblings, and technical pursuits are highlighted.",
        "Mercury": "a period of intellect, communication, and business. Education, writing, and analytical skills flourish.",
        "Jupiter": "a period of wisdom, expansion, and spiritual growth. Higher education, children, and fortune are favored.",
        "Venus": "a period of love, luxury, and artistic expression. Relationships, vehicles, and comforts are highlighted.",
        "Saturn": "a period of discipline, hard work, and karmic lessons. Patience and perseverance bring lasting rewards.",
        "Rahu": "a period of ambition, unconventional paths, and material desires. Foreign connections and sudden changes.",
        "Ketu": "a period of spiritual awakening, detachment, and past-life karma. Introspection and liberation themes.",
    }

    # Build response based on query type
    if any(w in msg for w in ["overview", "summary", "tell me about", "analyze", "reading", "what does my chart"]):
        planets_info = insights.get("planets", [])
        retro_planets = [p["name"] for p in planets_info if "retrograde" in p.get("status", [])]
        exalted_planets = [p["name"] for p in planets_info if "exalted" in p.get("status", [])]
        debilitated_planets = [p["name"] for p in planets_info if "debilitated" in p.get("status", [])]

        resp = f"**Birth Chart Overview for {name}**\n\n"
        resp += f"**Ascendant (Lagna):** {asc} — {asc_traits.get(asc, 'A unique blend of qualities.')}\n\n"
        resp += f"**Moon Sign:** {moon_sign} — {moon_traits.get(moon_sign, 'Complex emotional nature.')}\n\n"
        resp += f"**Sun Sign:** {sun_sign}\n\n"
        resp += f"**Birth Nakshatra:** {nakshatra} (Pada {insights.get('nakshatra_pada', '')})\n\n"
        resp += f"**Current Dasha:** {current_dasha} — {dasha_meanings.get(current_dasha.split('-')[0].strip() if '-' in current_dasha else current_dasha, 'A significant planetary period.')}\n\n"

        if strong:
            resp += f"**Strong Planets:** {', '.join(strong)} — These planets give you natural advantages in their significations.\n\n"
        if weak:
            resp += f"**Weak Planets:** {', '.join(weak)} — These areas may require more conscious effort and remedial measures.\n\n"
        if exalted_planets:
            resp += f"**Exalted Planets:** {', '.join(exalted_planets)} — Exceptionally powerful placements bringing blessings.\n\n"
        if debilitated_planets:
            resp += f"**Debilitated Planets:** {', '.join(debilitated_planets)} — Challenging placements that offer growth through struggle.\n\n"
        if retro_planets:
            resp += f"**Retrograde Planets:** {', '.join(retro_planets)} — Internalized energies requiring introspection.\n\n"

        return resp

    elif any(w in msg for w in ["career", "job", "work", "profession", "business"]):
        resp = f"**Career Analysis for {name}**\n\n"
        resp += f"With **{asc} Ascendant**, "
        career_hints = {
            "Aries": "you're suited for leadership roles, entrepreneurship, military, sports, or engineering.",
            "Taurus": "you excel in finance, banking, agriculture, arts, hospitality, or luxury goods.",
            "Gemini": "you thrive in communication, media, writing, teaching, or technology.",
            "Cancer": "you're drawn to caregiving, real estate, food industry, or public service.",
            "Leo": "you shine in management, entertainment, politics, or creative leadership.",
            "Virgo": "you excel in healthcare, accounting, research, editing, or quality control.",
            "Libra": "you're suited for law, diplomacy, fashion, design, or counseling.",
            "Scorpio": "you thrive in research, investigation, psychology, surgery, or occult sciences.",
            "Sagittarius": "you're drawn to education, philosophy, travel industry, or spiritual teaching.",
            "Capricorn": "you excel in administration, engineering, mining, or corporate leadership.",
            "Aquarius": "you thrive in technology, social work, innovation, or humanitarian causes.",
            "Pisces": "you're drawn to healing, arts, spirituality, marine fields, or charitable work.",
        }
        resp += career_hints.get(asc, "your career path is unique and multifaceted.") + "\n\n"

        # 10th house analysis
        planets_info = insights.get("planets", [])
        tenth_house_planets = [p for p in planets_info if p.get("house") == 10]
        if tenth_house_planets:
            names = [p["name"] for p in tenth_house_planets]
            resp += f"**Planets in 10th House:** {', '.join(names)} — This strongly influences your professional life and public image.\n\n"

        if strong:
            resp += f"Your strong planets ({', '.join(strong)}) support career success in their respective domains.\n\n"

        resp += f"**Current Dasha ({current_dasha}):** {dasha_meanings.get(current_dasha.split('-')[0].strip() if '-' in current_dasha else current_dasha, 'This period shapes your current career trajectory.')}\n"
        return resp

    elif any(w in msg for w in ["love", "marriage", "relationship", "partner", "spouse"]):
        resp = f"**Relationship Analysis for {name}**\n\n"
        planets_info = insights.get("planets", [])
        venus = next((p for p in planets_info if p["name"] == "Venus"), None)
        seventh_house = [p for p in planets_info if p.get("house") == 7]

        resp += f"With **{asc} Ascendant** and **Moon in {moon_sign}**, "
        resp += "your emotional needs and relationship style are shaped by these core energies.\n\n"

        if venus:
            resp += f"**Venus** is in **{venus['sign']}** (House {venus['house']})"
            if venus.get("status"):
                resp += f" — {', '.join(venus['status'])}"
            resp += ". Venus governs love, beauty, and partnerships.\n\n"

        if seventh_house:
            names = [p["name"] for p in seventh_house]
            resp += f"**Planets in 7th House (Marriage):** {', '.join(names)} — These directly influence your partnership dynamics.\n\n"

        resp += f"**Current Dasha ({current_dasha})** influences your relationship timeline and experiences.\n"
        return resp

    elif any(w in msg for w in ["health", "body", "physical", "wellness", "disease"]):
        resp = f"**Health Indicators for {name}**\n\n"
        resp += f"With **{asc} Ascendant**, "
        health_hints = {
            "Aries": "watch for head-related issues, fevers, and inflammation. Stay active but avoid recklessness.",
            "Taurus": "throat, thyroid, and neck areas need attention. Maintain balanced diet and avoid excess.",
            "Gemini": "respiratory system and nervous system are sensitive. Mental health and breathing exercises help.",
            "Cancer": "digestive system and chest area need care. Emotional health directly impacts physical wellbeing.",
            "Leo": "heart and spine health are important. Regular cardio and maintaining vitality are key.",
            "Virgo": "digestive and intestinal health need attention. Stress management and clean diet are essential.",
            "Libra": "kidney and lower back areas need care. Balance in all things supports health.",
            "Scorpio": "reproductive and excretory systems need attention. Emotional detox is as important as physical.",
            "Sagittarius": "liver, hips, and thighs need care. Stay active and avoid excess indulgence.",
            "Capricorn": "bones, joints, and knees need attention. Calcium intake and regular exercise are important.",
            "Aquarius": "circulatory system and ankles need care. Unconventional healing methods may benefit you.",
            "Pisces": "feet and lymphatic system need attention. Adequate rest and spiritual practices support health.",
        }
        resp += health_hints.get(asc, "maintain a balanced lifestyle for optimal health.") + "\n\n"

        if weak:
            resp += f"**Weak planets ({', '.join(weak)})** may indicate areas requiring extra health attention.\n\n"
        if strong:
            resp += f"**Strong planets ({', '.join(strong)})** provide natural vitality in their domains.\n"
        return resp

    elif any(w in msg for w in ["dasha", "period", "current", "timing", "prediction", "future"]):
        resp = f"**Dasha Analysis for {name}**\n\n"
        resp += f"**Current Mahadasha:** {current_dasha}\n\n"
        dasha_planet = current_dasha.split("-")[0].strip() if "-" in current_dasha else current_dasha
        resp += f"**Interpretation:** {dasha_meanings.get(dasha_planet, 'This is a significant period of transformation.')}\n\n"

        mahadasha_end = insights.get("mahadasha_end", "")
        if mahadasha_end:
            resp += f"**Mahadasha ends:** {mahadasha_end}\n\n"

        resp += "The dasha system reveals the unfolding of your karma through planetary periods. Each period activates different areas of life based on the ruling planet's placement and strength in your chart.\n"
        return resp

    elif any(w in msg for w in ["planet", "strength", "strong", "weak", "bala"]):
        resp = f"**Planetary Strength Analysis for {name}**\n\n"
        planets_info = insights.get("planets", [])

        for p in planets_info:
            status_str = ""
            if p.get("status"):
                status_str = f" ({', '.join(p['status'])})"
            strength = "Strong" if p["name"] in strong else ("Weak" if p["name"] in weak else "Medium")
            emoji = "+" if strength == "Strong" else ("-" if strength == "Weak" else "~")
            resp += f"**{p['name']}** in {p['sign']} (House {p['house']}){status_str} — [{emoji} {strength}]\n\n"

        return resp

    elif any(w in msg for w in ["house", "bhava", "area"]):
        resp = f"**House Analysis for {name}**\n\n"
        strong_h = insights.get("strong_houses", [])
        weak_h = insights.get("weak_houses", [])

        house_meanings = {
            "1": "Self, personality, physical body",
            "2": "Wealth, family, speech, values",
            "3": "Siblings, courage, communication",
            "4": "Home, mother, comfort, education",
            "5": "Children, creativity, intelligence",
            "6": "Enemies, health, service, debts",
            "7": "Marriage, partnerships, business",
            "8": "Transformation, longevity, occult",
            "9": "Fortune, dharma, higher learning",
            "10": "Career, status, public image",
            "11": "Gains, income, aspirations",
            "12": "Losses, spirituality, foreign lands",
        }

        if strong_h:
            resp += "**Strong Houses:**\n"
            for h in strong_h:
                hnum = str(h["house"])
                resp += f"- House {hnum} ({house_meanings.get(hnum, '')}) — Lord: {h['lord']} [{h['rating']}]\n"
            resp += "\n"

        if weak_h:
            resp += "**Weak Houses:**\n"
            for h in weak_h:
                hnum = str(h["house"])
                resp += f"- House {hnum} ({house_meanings.get(hnum, '')}) — Lord: {h['lord']} [{h['rating']}]\n"
            resp += "\n"

        return resp

    elif any(w in msg for w in ["remedy", "remedies", "solution", "fix", "improve"]):
        resp = f"**Remedial Suggestions for {name}**\n\n"
        if weak:
            for planet in weak:
                remedies = {
                    "Sun": "Offer water to the Sun at sunrise. Wear ruby (after consultation). Chant Surya mantra. Respect father figures.",
                    "Moon": "Wear pearl or moonstone. Drink water from silver vessel. Chant Chandra mantra. Serve your mother.",
                    "Mars": "Wear red coral. Chant Mangal mantra. Practice physical exercise. Donate red items on Tuesdays.",
                    "Mercury": "Wear emerald. Chant Budha mantra. Study and read regularly. Feed green vegetables to cows.",
                    "Jupiter": "Wear yellow sapphire. Chant Guru mantra. Respect teachers and elders. Donate yellow items on Thursdays.",
                    "Venus": "Wear diamond or white sapphire. Chant Shukra mantra. Appreciate arts and beauty. Donate white items on Fridays.",
                    "Saturn": "Wear blue sapphire (with caution). Chant Shani mantra. Serve the elderly and underprivileged. Practice patience.",
                    "Rahu": "Wear hessonite garnet. Chant Rahu mantra. Avoid shortcuts and deception. Donate to the needy.",
                    "Ketu": "Wear cat's eye. Chant Ketu mantra. Practice meditation and spirituality. Donate blankets to the poor.",
                }
                resp += f"**For weak {planet}:**\n{remedies.get(planet, 'Consult an astrologer for specific remedies.')}\n\n"
        else:
            resp += "Your chart shows generally strong planetary positions. Focus on maintaining balance through:\n"
            resp += "- Regular meditation and spiritual practice\n"
            resp += "- Charity and service to others\n"
            resp += "- Respecting planetary days and their significations\n"

        return resp

    else:
        # General greeting or unknown query
        resp = f"**Astrological Insights for {name}**\n\n"
        resp += f"Your chart shows **{asc} Ascendant** with **Moon in {nakshatra}** nakshatra.\n\n"
        resp += f"**Current Dasha:** {current_dasha}\n\n"
        resp += "I can help you with:\n"
        resp += "- **Overview** — Full chart summary\n"
        resp += "- **Career** — Professional guidance\n"
        resp += "- **Relationships** — Love and marriage insights\n"
        resp += "- **Health** — Physical wellness indicators\n"
        resp += "- **Dasha/Timing** — Current planetary period analysis\n"
        resp += "- **Planets** — Planetary strength breakdown\n"
        resp += "- **Houses** — Bhava (house) analysis\n"
        resp += "- **Remedies** — Suggestions for weak planets\n\n"
        resp += "Ask me anything about your birth chart!\n"
        return resp


@app.post("/api/chat")
async def astro_chat(request: ChatRequest):
    """AI Astrologer chat endpoint using OpenRouter with tool-calling."""
    try:
        if not request.kundali_data:
            return {
                "response": "Please generate or load a birth chart first so I can provide personalized astrological insights. I need your kundali data to give accurate readings.",
                "has_chart": False,
            }

        if not OPENROUTER_API_KEY:
            # Fallback to rule-based if no API key
            insights = _generate_astro_insights(request.kundali_data)
            response_text = _build_astro_response(
                request.message, insights, request.chart_name or "your chart"
            )
            return {"response": response_text, "has_chart": True}

        # Build messages for OpenRouter
        chart_name = request.chart_name or "this person"
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT + f"\nThe current chart belongs to: {chart_name}"},
        ]

        # Add conversation history if provided
        if request.conversation_history:
            for msg in request.conversation_history[-10:]:  # Last 10 messages
                if msg.get("role") in ("user", "assistant"):
                    messages.append({"role": msg["role"], "content": msg["content"]})

        messages.append({"role": "user", "content": request.message})

        # Call OpenRouter with tools
        max_tool_rounds = 5
        for _ in range(max_tool_rounds):
            result = _call_openrouter(messages, ASTRO_TOOLS)
            choice = result.get("choices", [{}])[0]
            msg = choice.get("message", {})

            # Check if the model wants to call tools
            tool_calls = msg.get("tool_calls")
            if not tool_calls:
                # No tool calls — we have the final response
                return {
                    "response": msg.get("content", "I couldn't generate a response. Please try again."),
                    "has_chart": True,
                }

            # Execute tool calls and add results
            messages.append(msg)  # Add assistant message with tool_calls
            for tc in tool_calls:
                fn_name = tc.get("function", {}).get("name", "")
                tool_result = _execute_tool(fn_name, request.kundali_data)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.get("id", ""),
                    "content": tool_result,
                })

        # If we exhausted tool rounds, make one final call without tools
        final = _call_openrouter(messages, [])
        final_msg = final.get("choices", [{}])[0].get("message", {})
        return {
            "response": final_msg.get("content", "I analyzed your chart but couldn't formulate a complete response. Please try a more specific question."),
            "has_chart": True,
        }

    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if hasattr(e, "read") else str(e)
        raise HTTPException(status_code=502, detail=f"AI service error: {error_body}")
    except urllib.error.URLError as e:
        raise HTTPException(status_code=503, detail=f"AI service unavailable: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
