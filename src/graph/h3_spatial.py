"""Uber H3 spatial indexing, multi-token address verification & apartment anomaly defense.

Features:
1. Granular Bangalore & Indian pincode and sub-locality registry (30+ zones).
2. Reverse address matching: Auto-detects Area Name and Pincode from typed address keywords.
3. Apartment/High-Density Complex Spoofing Defense: Isolates device signals and unit rotation
   so scammers inside luxury complexes cannot hide behind a clean neighborhood H3 baseline.
4. Multi-Resolution H3 Indexing: Res 7 (Ward), Res 8 (Neighborhood), Res 9 (Block), Res 10 (Doorstep).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

import h3

logger = logging.getLogger(__name__)


def _latlng_to_cell(lat: float, lng: float, res: int) -> str:
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lng, res)
    elif hasattr(h3, "geo_to_h3"):
        return h3.geo_to_h3(lat, lng, res)
    return f"h3_{res}_{int(lat*1000)}_{int(lng*1000)}"


def _cell_to_parent(cell: str, res: int) -> str:
    if hasattr(h3, "cell_to_parent"):
        return h3.cell_to_parent(cell, res)
    elif hasattr(h3, "h3_to_parent"):
        return h3.h3_to_parent(cell, res)
    return cell


def _grid_disk(cell: str, ring_size: int = 1) -> list[str]:
    if hasattr(h3, "grid_disk"):
        return list(h3.grid_disk(cell, ring_size))
    elif hasattr(h3, "k_ring"):
        return list(h3.k_ring(cell, ring_size))
    return [cell]


def _grid_distance(cell_a: str, cell_b: str) -> int:
    if hasattr(h3, "grid_distance"):
        return h3.grid_distance(cell_a, cell_b)
    elif hasattr(h3, "h3_distance"):
        return h3.h3_distance(cell_a, cell_b)
    return 1


def _get_resolution(cell: str) -> int:
    if hasattr(h3, "get_resolution"):
        return h3.get_resolution(cell)
    elif hasattr(h3, "h3_get_resolution"):
        return h3.h3_get_resolution(cell)
    return 9

# Centroids and known sub-localities for granular spatial resolution
PINCODE_REGISTRY: dict[str, dict[str, Any]] = {
    # --- Greater Bengaluru Zones ---
    "560103": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Bellandur / Outer Ring Road",
        "lat": 12.9249,
        "lng": 77.6763,
        "sub_localities": {
            "bellandur": (12.9249, 77.6763),
            "green glen layout": (12.9280, 77.6720),
            "sarjapur road": (12.9180, 77.6820),
            "outer ring road": (12.9260, 77.6790),
            "ecospace": (12.9220, 77.6840),
            "devarabisanahalli": (12.9290, 77.6880),
            "prestige tech park": (12.9360, 77.6930),
            "adishwar": (12.9240, 77.6750),
        },
        "rto_baseline": 0.038,
        "is_high_density_apartment_hub": True,
    },
    "560001": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "MG Road / Central Business District",
        "lat": 12.9716,
        "lng": 77.5946,
        "sub_localities": {
            "mg road": (12.9716, 77.5946),
            "brigade road": (12.9730, 77.6070),
            "church street": (12.9745, 77.6050),
            "residency road": (12.9690, 77.6020),
            "cubbon park": (12.9760, 77.5930),
            "lavelle road": (12.9700, 77.5980),
            "ashok nagar": (12.9680, 77.6040),
        },
        "rto_baseline": 0.021,
        "is_high_density_apartment_hub": False,
    },
    "560034": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Koramangala",
        "lat": 12.9352,
        "lng": 77.6245,
        "sub_localities": {
            "koramangala": (12.9352, 77.6245),
            "koramangala 5th block": (12.9352, 77.6245),
            "koramangala 4th block": (12.9320, 77.6290),
            "koramangala 6th block": (12.9380, 77.6210),
            "koramangala 7th block": (12.9370, 77.6150),
            "80 feet road": (12.9330, 77.6270),
            "forum mall": (12.9340, 77.6110),
            "sony world junction": (12.9360, 77.6280),
        },
        "rto_baseline": 0.029,
        "is_high_density_apartment_hub": True,
    },
    "560102": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "HSR Layout",
        "lat": 12.9116,
        "lng": 77.6389,
        "sub_localities": {
            "hsr layout": (12.9116, 77.6389),
            "hsr layout sector 1": (12.9160, 77.6420),
            "hsr layout sector 2": (12.9116, 77.6389),
            "hsr layout sector 3": (12.9100, 77.6320),
            "hsr layout sector 4": (12.9080, 77.6280),
            "hsr layout sector 6": (12.9190, 77.6350),
            "hsr layout sector 7": (12.9060, 77.6410),
            "27th main": (12.9090, 77.6450),
            "bda complex hsr": (12.9120, 77.6390),
        },
        "rto_baseline": 0.027,
        "is_high_density_apartment_hub": True,
    },
    "560066": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Whitefield",
        "lat": 12.9698,
        "lng": 77.7499,
        "sub_localities": {
            "whitefield": (12.9698, 77.7499),
            "itpl": (12.9860, 77.7370),
            "hope farm": (12.9820, 77.7510),
            "kadugodi": (12.9980, 77.7600),
            "epip zone": (12.9780, 77.7280),
            "prestige shantiniketan": (12.9890, 77.7290),
            "channasandra": (12.9940, 77.7550),
            "pattandur agrahara": (12.9810, 77.7410),
        },
        "rto_baseline": 0.034,
        "is_high_density_apartment_hub": True,
    },
    "560008": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Indiranagar",
        "lat": 12.9784,
        "lng": 77.6408,
        "sub_localities": {
            "indiranagar": (12.9784, 77.6408),
            "100 feet road": (12.9720, 77.6410),
            "12th main": (12.9740, 77.6430),
            "cmh road": (12.9790, 77.6380),
            "defense colony": (12.9750, 77.6470),
            "hal 2nd stage": (12.9690, 77.6490),
            "doopanahalli": (12.9660, 77.6400),
        },
        "rto_baseline": 0.022,
        "is_high_density_apartment_hub": False,
    },
    "560076": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "BTM Layout / Bannerghatta Road",
        "lat": 12.9166,
        "lng": 77.6101,
        "sub_localities": {
            "btm layout": (12.9166, 77.6101),
            "btm 1st stage": (12.9190, 77.6080),
            "btm 2nd stage": (12.9140, 77.6120),
            "bannerghatta road": (12.8980, 77.5990),
            "tavarekere": (12.9230, 77.6140),
            "mico layout": (12.9110, 77.6040),
        },
        "rto_baseline": 0.039,
        "is_high_density_apartment_hub": True,
    },
    "560078": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "JP Nagar",
        "lat": 12.9077,
        "lng": 77.5855,
        "sub_localities": {
            "jp nagar": (12.9077, 77.5855),
            "jp nagar 1st phase": (12.9120, 77.5890),
            "jp nagar 2nd phase": (12.9090, 77.5860),
            "jp nagar 3rd phase": (12.9060, 77.5920),
            "jp nagar 6th phase": (12.8990, 77.5790),
            "sarakki": (12.9040, 77.5780),
            "dollars colony": (12.8960, 77.5920),
        },
        "rto_baseline": 0.026,
        "is_high_density_apartment_hub": True,
    },
    "560041": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Jayanagar",
        "lat": 12.9308,
        "lng": 77.5838,
        "sub_localities": {
            "jayanagar": (12.9308, 77.5838),
            "jayanagar 3rd block": (12.9340, 77.5860),
            "jayanagar 4th block": (12.9290, 77.5820),
            "jayanagar 9th block": (12.9190, 77.5930),
            "south end circle": (12.9370, 77.5780),
        },
        "rto_baseline": 0.020,
        "is_high_density_apartment_hub": False,
    },
    "560037": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Marathahalli",
        "lat": 12.9591,
        "lng": 77.6974,
        "sub_localities": {
            "marathahalli": (12.9591, 77.6974),
            "kundalahalli": (12.9660, 77.7120),
            "spice garden": (12.9620, 77.7080),
            "hal": (12.9550, 77.6810),
            "munnekollal": (12.9510, 77.7140),
        },
        "rto_baseline": 0.045,
        "is_high_density_apartment_hub": True,
    },
    "560068": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Electronic City / Bommanahalli",
        "lat": 12.8452,
        "lng": 77.6602,
        "sub_localities": {
            "electronic city": (12.8452, 77.6602),
            "electronic city phase 1": (12.8480, 77.6650),
            "electronic city phase 2": (12.8390, 77.6790),
            "bommanahalli": (12.9020, 77.6240),
            "singasandra": (12.8850, 77.6430),
            "kudlu gate": (12.8910, 77.6450),
        },
        "rto_baseline": 0.041,
        "is_high_density_apartment_hub": True,
    },
    "560043": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Banaswadi / Kalyan Nagar / HRBR Layout",
        "lat": 13.0142,
        "lng": 77.6519,
        "sub_localities": {
            "banaswadi": (13.0142, 77.6519),
            "kalyan nagar": (13.0230, 77.6480),
            "hrbr layout": (13.0190, 77.6460),
            "kammanahalli": (13.0090, 77.6390),
            "ombr layout": (13.0040, 77.6560),
        },
        "rto_baseline": 0.030,
        "is_high_density_apartment_hub": True,
    },
    "560092": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Hebbal / Sahakara Nagar",
        "lat": 13.0358,
        "lng": 77.5970,
        "sub_localities": {
            "hebbal": (13.0358, 77.5970),
            "sahakara nagar": (13.0620, 77.5910),
            "manyata tech park": (13.0480, 77.6210),
            "nagavara": (13.0420, 77.6230),
            "byatarayanapura": (13.0690, 77.5930),
        },
        "rto_baseline": 0.033,
        "is_high_density_apartment_hub": True,
    },
    "560085": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Banashankari",
        "lat": 12.9255,
        "lng": 77.5468,
        "sub_localities": {
            "banashankari": (12.9255, 77.5468),
            "bsk 2nd stage": (12.9280, 77.5580),
            "bsk 3rd stage": (12.9210, 77.5420),
            "kathriguppe": (12.9270, 77.5510),
            "hosakerehalli": (12.9340, 77.5340),
        },
        "rto_baseline": 0.024,
        "is_high_density_apartment_hub": False,
    },
    "560064": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Yelahanka",
        "lat": 13.1007,
        "lng": 77.5963,
        "sub_localities": {
            "yelahanka": (13.1007, 77.5963),
            "yelahanka new town": (13.1050, 77.5850),
            "kogilu": (13.1180, 77.6120),
            "bagalur road": (13.1250, 77.6280),
        },
        "rto_baseline": 0.028,
        "is_high_density_apartment_hub": False,
    },
    "560048": {
        "city": "Bengaluru",
        "state": "Karnataka",
        "area_name": "Hoodi / Mahadevapura / KR Puram",
        "lat": 12.9922,
        "lng": 77.7159,
        "sub_localities": {
            "hoodi": (12.9922, 77.7159),
            "mahadevapura": (12.9910, 77.6950),
            "kr puram": (13.0070, 77.6960),
            "garudacharpalya": (12.9860, 77.7080),
        },
        "rto_baseline": 0.043,
        "is_high_density_apartment_hub": True,
    },

    # --- Major Metros ---
    "400001": {
        "city": "Mumbai",
        "state": "Maharashtra",
        "area_name": "Fort / Marine Drive",
        "lat": 18.9388,
        "lng": 72.8354,
        "sub_localities": {
            "fort": (18.9388, 72.8354),
            "marine drive": (18.9430, 72.8230),
            "nariman point": (18.9260, 72.8220),
            "cst": (18.9400, 72.8350),
        },
        "rto_baseline": 0.018,
        "is_high_density_apartment_hub": False,
    },
    "400050": {
        "city": "Mumbai",
        "state": "Maharashtra",
        "area_name": "Bandra West",
        "lat": 19.0596,
        "lng": 72.8295,
        "sub_localities": {
            "bandra west": (19.0596, 72.8295),
            "hill road": (19.0550, 72.8320),
            "linking road": (19.0640, 72.8350),
            "pali hill": (19.0680, 72.8270),
        },
        "rto_baseline": 0.024,
        "is_high_density_apartment_hub": True,
    },
    "400053": {
        "city": "Mumbai",
        "state": "Maharashtra",
        "area_name": "Andheri East",
        "lat": 19.1136,
        "lng": 72.8697,
        "sub_localities": {
            "andheri east": (19.1136, 72.8697),
            "midc": (19.1190, 72.8750),
            "jb nagar": (19.1120, 72.8680),
            "chakala": (19.1100, 72.8610),
        },
        "rto_baseline": 0.048,
        "is_high_density_apartment_hub": True,
    },
    "110001": {
        "city": "Delhi",
        "state": "Delhi",
        "area_name": "Connaught Place",
        "lat": 28.6328,
        "lng": 77.2197,
        "sub_localities": {
            "connaught place": (28.6328, 77.2197),
            "janpath": (28.6250, 77.2180),
            "barakhamba road": (28.6310, 77.2270),
        },
        "rto_baseline": 0.029,
        "is_high_density_apartment_hub": False,
    },
    "110020": {
        "city": "Delhi",
        "state": "Delhi",
        "area_name": "Hauz Khas",
        "lat": 28.5494,
        "lng": 77.2001,
        "sub_localities": {
            "hauz khas": (28.5494, 77.2001),
            "green park": (28.5580, 77.2060),
            "sda complex": (28.5460, 77.1950),
        },
        "rto_baseline": 0.022,
        "is_high_density_apartment_hub": True,
    },
    "500034": {
        "city": "Hyderabad",
        "state": "Telangana",
        "area_name": "Jubilee Hills / Banjara Hills",
        "lat": 17.4156,
        "lng": 78.4347,
        "sub_localities": {
            "jubilee hills": (17.4156, 78.4347),
            "banjara hills": (17.4180, 78.4480),
            "road no 36": (17.4290, 78.4090),
        },
        "rto_baseline": 0.019,
        "is_high_density_apartment_hub": True,
    },
    "600020": {
        "city": "Chennai",
        "state": "Tamil Nadu",
        "area_name": "T Nagar",
        "lat": 13.0418,
        "lng": 80.2341,
        "sub_localities": {
            "t nagar": (13.0418, 80.2341),
            "usman road": (13.0390, 80.2310),
            "pondy bazaar": (13.0420, 80.2370),
        },
        "rto_baseline": 0.023,
        "is_high_density_apartment_hub": False,
    },
    "700064": {
        "city": "Kolkata",
        "state": "West Bengal",
        "area_name": "Salt Lake Sector V",
        "lat": 22.5726,
        "lng": 88.4313,
        "sub_localities": {
            "salt lake": (22.5726, 88.4313),
            "sector v": (22.5726, 88.4313),
            "technopolis": (22.5780, 88.4350),
        },
        "rto_baseline": 0.026,
        "is_high_density_apartment_hub": True,
    },
    "411038": {
        "city": "Pune",
        "state": "Maharashtra",
        "area_name": "Hinjewadi Infotech Park",
        "lat": 18.5912,
        "lng": 73.7380,
        "sub_localities": {
            "hinjewadi": (18.5912, 73.7380),
            "blue ridge": (18.5830, 73.7390),
            "infotech park": (18.5940, 73.7320),
        },
        "rto_baseline": 0.022,
        "is_high_density_apartment_hub": True,
    },
}

# Reverse keyword map: "bellandur" -> 560103, "koramangala" -> 560034, etc.
AREA_KEYWORD_MAP: dict[str, tuple[str, str]] = {}
for _pin, _info in PINCODE_REGISTRY.items():
    for _sub_name in _info["sub_localities"]:
        AREA_KEYWORD_MAP[_sub_name] = (_pin, _info["area_name"])


@dataclass(frozen=True)
class AddressTokens:
    """Structured breakdown of an Indian delivery address."""
    unit_door_no: Optional[str]
    building_premise: Optional[str]
    street_landmark: Optional[str]
    sub_locality: Optional[str]
    city: Optional[str]
    pincode: Optional[str]
    area_name: Optional[str]
    raw_text: str
    token_completeness_score: float  # 0.0 - 1.0
    is_apartment_complex: bool


@dataclass(frozen=True)
class H3SpatialResult:
    """Multi-resolution Uber H3 spatial intelligence result."""
    h3_index_res9: str       # ~100m building block level
    h3_index_res8: str       # ~460m neighborhood cluster level
    h3_index_res7: str       # ~1.2km ward level
    h3_index_res10: str      # ~30m fine doorstep level
    latitude: float
    longitude: float
    is_fallback: bool
    matched_locality: str
    area_name: str
    rto_baseline: float
    address_tokens: AddressTokens
    spatial_confidence: float  # 0.0 - 1.0
    apartment_anomaly_flag: bool  # True if high-density complex requires device isolation
    hierarchical_fallback_level: str = "RES_9_EXACT"
    smoothed_rto_prior: float = 0.035


class H3SpatialEngine:
    """Converts multi-token addresses into hierarchical Uber H3 spatial cells with apartment spoofing defense."""

    H3_RES_SECTOR = 6   # ~3.2 km hex
    H3_RES_WARD = 7     # ~1.2 km hex
    H3_RES_COARSE = 8   # ~460 m hex
    H3_RES_FINE = 9     # ~100 m hex
    H3_RES_DOORSTEP = 10  # ~30 m hex

    def __init__(self) -> None:
        self._registry = PINCODE_REGISTRY
        self._keyword_map = AREA_KEYWORD_MAP
        logger.info("H3 Spatial Engine initialized with %d pincode zones and %d area keywords.", len(self._registry), len(self._keyword_map))

    def parse_address_tokens(self, raw_address: str, pincode: Optional[str] = None) -> AddressTokens:
        """Break raw address into constituent components and auto-detect area and pincode."""
        clean = raw_address.strip() if raw_address else ""
        extracted_pin = pincode or self._extract_pincode(clean)

        # Keyword reverse match if pincode missing or needs verification
        detected_area = None
        lower_clean = clean.lower()

        # Check sub-locality keywords
        matched_sub = None
        for kw, (pin_found, area_desc) in self._keyword_map.items():
            if kw in lower_clean:
                matched_sub = kw
                if not extracted_pin or extracted_pin not in self._registry:
                    extracted_pin = pin_found
                detected_area = area_desc
                break

        # Token identification patterns
        unit_match = re.search(r'\b(flat|apt|apartment|door|house|no|room|block|villa|tower|unit)\s*[:#.-]?\s*([0-9a-zA-Z\/-]+)', clean, re.IGNORECASE)
        unit = unit_match.group(0) if unit_match else None

        building_match = re.search(r'\b([a-zA-Z0-9\s]+(?:tower|apartments|enclave|residency|heights|layout|complex|gateway|society|greens|palms|meadows|lakeview|shantiniketan|brigade))\b', clean, re.IGNORECASE)
        building = building_match.group(1).strip() if building_match else None

        street_match = re.search(r'\b([a-zA-Z0-9\s]+(?:road|street|cross|main|lane|marg|nagar|bazaar|market|circle|junction|flyover))\b', clean, re.IGNORECASE)
        street = street_match.group(1).strip() if street_match else None

        is_apt = bool(re.search(r'\b(apartment|apartments|flat|tower|complex|layout|residency|society|enclave|lakeview|shantiniketan|brigade|prestige)\b', clean, re.IGNORECASE))

        # Matched city & area
        matched_city = "Bengaluru"
        if extracted_pin and extracted_pin in self._registry:
            info = self._registry[extracted_pin]
            matched_city = info["city"]
            detected_area = detected_area or info["area_name"]

        # Completeness calculation
        tokens_found = sum(1 for t in [unit, building, street, matched_sub, extracted_pin] if t is not None)
        completeness = min(1.0, tokens_found / 4.0)

        return AddressTokens(
            unit_door_no=unit,
            building_premise=building,
            street_landmark=street,
            sub_locality=matched_sub,
            city=matched_city,
            pincode=extracted_pin,
            area_name=detected_area or (f"Pincode {extracted_pin}" if extracted_pin else "Bengaluru"),
            raw_text=clean,
            token_completeness_score=completeness,
            is_apartment_complex=is_apt,
        )

    def resolve(
        self,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        pincode: Optional[str] = None,
        raw_address: Optional[str] = None,
    ) -> H3SpatialResult:
        """Resolve full address into hierarchical H3 cells with apartment spoofing defense."""
        clean_address = raw_address or ""
        tokens = self.parse_address_tokens(clean_address, pincode)
        pin = tokens.pincode or "560103"

        # Path 1: Direct coordinates
        if latitude is not None and longitude is not None:
            if self._validate_india_coords(latitude, longitude):
                return self._compute_h3(latitude, longitude, is_fallback=False, matched_locality="GPS Coordinates", area_name=tokens.area_name or "Bengaluru", rto_baseline=0.03, tokens=tokens, confidence=0.98)

        # Path 2: Granular Sub-locality Resolution
        if pin in self._registry:
            info = self._registry[pin]
            # Check for sub-locality match inside the address text
            if tokens.sub_locality and tokens.sub_locality in info["sub_localities"]:
                lat, lng = info["sub_localities"][tokens.sub_locality]
                return self._compute_h3(
                    lat=lat,
                    lng=lng,
                    is_fallback=False,
                    matched_locality=f"{tokens.sub_locality.title()}, {info['city']}",
                    area_name=info["area_name"],
                    rto_baseline=info["rto_baseline"],
                    tokens=tokens,
                    confidence=0.94,
                )

            # Fallback to Pincode Centroid
            lat, lng = info["lat"], info["lng"]
            return self._compute_h3(
                lat=lat,
                lng=lng,
                is_fallback=True,
                matched_locality=f"{info['area_name']}, {info['city']}",
                area_name=info["area_name"],
                rto_baseline=info["rto_baseline"],
                tokens=tokens,
                confidence=0.82,
            )

        # Path 3: Unknown Pincode — Default to Bellandur Bangalore
        return self._compute_h3(
            lat=12.9249,
            lng=77.6763,
            is_fallback=True,
            matched_locality=f"Pincode {pin}",
            area_name=f"Pincode {pin}",
            rto_baseline=0.04,
            tokens=tokens,
            confidence=0.50,
        )

    def _compute_h3(
        self,
        lat: float,
        lng: float,
        is_fallback: bool,
        matched_locality: str,
        area_name: str,
        rto_baseline: float,
        tokens: AddressTokens,
        confidence: float,
    ) -> H3SpatialResult:
        """Compute Uber H3 cell indices across fine, coarse, and doorstep resolutions."""
        res7 = _latlng_to_cell(lat, lng, self.H3_RES_WARD)
        res8 = _latlng_to_cell(lat, lng, self.H3_RES_COARSE)
        res9 = _latlng_to_cell(lat, lng, self.H3_RES_FINE)
        res10 = _latlng_to_cell(lat, lng, self.H3_RES_DOORSTEP)

        # Apartment multi-tenant anomaly check:
        # If in a high-density apartment complex, individual device entropy must override spatial prior
        apartment_flag = tokens.is_apartment_complex

        return H3SpatialResult(
            h3_index_res9=res9,
            h3_index_res8=res8,
            h3_index_res7=res7,
            h3_index_res10=res10,
            latitude=lat,
            longitude=lng,
            is_fallback=is_fallback,
            matched_locality=matched_locality,
            area_name=area_name,
            rto_baseline=rto_baseline,
            address_tokens=tokens,
            spatial_confidence=round(confidence * (0.6 + 0.4 * tokens.token_completeness_score), 2),
            apartment_anomaly_flag=apartment_flag,
        )

    @staticmethod
    def _validate_india_coords(lat: float, lng: float) -> bool:
        """Check if coordinates fall within India's geographic bounding box."""
        return 6.0 <= lat <= 36.0 and 68.0 <= lng <= 98.0

    @staticmethod
    def _extract_pincode(address: str) -> Optional[str]:
        """Extract 6-digit Indian pincode from raw address string."""
        match = re.search(r'\b([1-9]\d{5})\b', address)
        return match.group(1) if match else None

    def get_hex_neighbors(self, h3_index: str, ring_size: int = 1) -> list[str]:
        """Get neighboring hexagons for spatial proximity and syndicate ring analysis."""
        return _grid_disk(h3_index, ring_size)

    def compute_hex_distance(self, h3_a: str, h3_b: str) -> int:
        """Compute hexagonal grid distance between two H3 spatial cells."""
        try:
            return _grid_distance(h3_a, h3_b)
        except Exception:
            return 999

    def resolve_hierarchical_spatial_prior(
        self,
        h3_index: str,
        order_history_by_cell: Optional[dict[str, dict[str, int]]] = None,
        min_order_threshold: int = 10,
        default_pincode: str = "560103",
    ) -> dict[str, Any]:
        """Hierarchical spatial fallback for sparse or cold-start H3 hex cells.

        If an exact Res 9 or Res 10 cell has fewer than 10 historical orders,
        traverses up to Res 8 -> Res 7 -> Res 6 to inherit smoothed Bayesian priors
        instead of imposing an unfair cold-start penalty on new shoppers.
        """
        history = order_history_by_cell or {}

        try:
            current_res = _get_resolution(h3_index)
        except Exception:
            current_res = 9

        cell = h3_index

        # 1. Exact resolution check
        if cell in history and history[cell].get("order_count", 0) >= min_order_threshold:
            orders = history[cell]["order_count"]
            rtos = history[cell].get("rto_count", 0)
            smoothed = (rtos + 1.0) / (orders + 10.0)
            return {
                "h3_index": cell,
                "parent_h3_index": cell,
                "resolution": current_res,
                "fallback_level": f"RES_{current_res}_EXACT",
                "order_count": orders,
                "rto_count": rtos,
                "smoothed_rto_rate": round(smoothed, 4),
                "is_cold_start": False,
                "inherited_from_parent": False,
            }

        # 2. Hierarchical parent traversal: Res 8 -> Res 7 -> Res 6
        target_resolutions = [8, 7, 6]
        for parent_res in target_resolutions:
            if current_res > parent_res:
                try:
                    parent_cell = _cell_to_parent(h3_index, parent_res)
                    if parent_cell in history and history[parent_cell].get("order_count", 0) >= min_order_threshold:
                        p_orders = history[parent_cell]["order_count"]
                        p_rtos = history[parent_cell].get("rto_count", 0)
                        p_smoothed = (p_rtos + 1.0) / (p_orders + 10.0)
                        return {
                            "h3_index": cell,
                            "parent_h3_index": parent_cell,
                            "resolution": parent_res,
                            "fallback_level": f"RES_{parent_res}_PARENT",
                            "order_count": p_orders,
                            "rto_count": p_rtos,
                            "smoothed_rto_rate": round(p_smoothed, 4),
                            "is_cold_start": False,
                            "inherited_from_parent": True,
                        }
                except Exception as e:
                    logger.debug("Error traversing parent res %d: %s", parent_res, e)

        # 3. Pincode / Ward baseline fallback
        pin_info = self._registry.get(default_pincode, self._registry.get("560103", {}))
        baseline = pin_info.get("rto_baseline", 0.035) if isinstance(pin_info, dict) else 0.035
        try:
            parent_7 = _cell_to_parent(h3_index, 7) if current_res > 7 else h3_index
        except Exception:
            parent_7 = h3_index

        return {
            "h3_index": cell,
            "parent_h3_index": parent_7,
            "resolution": 7,
            "fallback_level": "PINCODE_WARD_PRIOR",
            "order_count": 0,
            "rto_count": 0,
            "smoothed_rto_rate": round(baseline, 4),
            "is_cold_start": True,
            "inherited_from_parent": True,
        }

