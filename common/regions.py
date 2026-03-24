"""
Canonical region definitions shared across all layers.

Each region provides multiple geometry formats so any layer can use
the appropriate one without conversion:
  - bbox:    FIRMS-style "west,south,east,north" string (or "world")
  - area:    CDS/Copernicus [N, W, S, E] list (None for global)
  - bounds:  (south, west, north, east) tuple for generic use
  - center:  [lat, lon] for map centering
  - zoom:    Folium zoom level
  - cam_*:   Cesium 3-D camera defaults
"""

REGIONS = {
    "world": {
        "bbox": "world",
        "area": None,
        "bounds": (-90, -180, 90, 180),
        "center": [20, 0], "zoom": 2,
        "cam_lon": 25, "cam_lat": 10, "cam_alt": 6_000_000,
        "default_days": 2,
        "label": "World",
    },
    "iran": {
        "bbox": "44,25,63.5,40",
        "area": [40, 44, 25, 63.5],
        "bounds": (25, 44, 40, 63.5),
        "center": [32, 53], "zoom": 6,
        "cam_lon": 53, "cam_lat": 28, "cam_alt": 1_800_000,
        "default_days": 10,
        "label": "Iran",
    },
    "middle_east": {
        "bbox": "25,12,65,42",
        "area": [42, 25, 12, 65],
        "bounds": (12, 25, 42, 65),
        "center": [28, 45], "zoom": 5,
        "cam_lon": 45, "cam_lat": 24, "cam_alt": 3_000_000,
        "default_days": 5,
        "label": "Middle East",
    },
    "north_america": {
        "bbox": "-170,10,-50,75",
        "area": [75, -170, 10, -50],
        "bounds": (10, -170, 75, -50),
        "center": [45, -100], "zoom": 3,
        "cam_lon": -100, "cam_lat": 38, "cam_alt": 8_000_000,
        "default_days": 3,
        "label": "North America",
    },
    "south_america": {
        "bbox": "-85,-57,-32,14",
        "area": [14, -85, -57, -32],
        "bounds": (-57, -85, 14, -32),
        "center": [-15, -60], "zoom": 3,
        "cam_lon": -60, "cam_lat": -20, "cam_alt": 8_000_000,
        "default_days": 3,
        "label": "South America",
    },
    "europe": {
        "bbox": "-15,35,45,72",
        "area": [72, -15, 35, 45],
        "bounds": (35, -15, 72, 45),
        "center": [50, 15], "zoom": 4,
        "cam_lon": 15, "cam_lat": 45, "cam_alt": 4_000_000,
        "default_days": 3,
        "label": "Europe",
    },
    "africa": {
        "bbox": "-20,-37,55,40",
        "area": [40, -20, -37, 55],
        "bounds": (-37, -20, 40, 55),
        "center": [5, 20], "zoom": 3,
        "cam_lon": 20, "cam_lat": 0, "cam_alt": 8_000_000,
        "default_days": 3,
        "label": "Africa",
    },
    "south_asia": {
        "bbox": "60,5,100,40",
        "area": [40, 60, 5, 100],
        "bounds": (5, 60, 40, 100),
        "center": [22, 80], "zoom": 4,
        "cam_lon": 80, "cam_lat": 18, "cam_alt": 4_000_000,
        "default_days": 5,
        "label": "South Asia",
    },
    "southeast_asia": {
        "bbox": "90,-15,155,30",
        "area": [30, 90, -15, 155],
        "bounds": (-15, 90, 30, 155),
        "center": [10, 120], "zoom": 4,
        "cam_lon": 120, "cam_lat": 5, "cam_alt": 5_000_000,
        "default_days": 3,
        "label": "Southeast Asia",
    },
    "australia": {
        "bbox": "110,-50,180,-5",
        "area": [-5, 110, -50, 180],
        "bounds": (-50, 110, -5, 180),
        "center": [-25, 135], "zoom": 4,
        "cam_lon": 135, "cam_lat": -30, "cam_alt": 5_000_000,
        "default_days": 3,
        "label": "Australia / Oceania",
    },
    "usa_west": {
        "bbox": "-130,30,-100,50",
        "area": [50, -130, 30, -100],
        "bounds": (30, -130, 50, -100),
        "center": [40, -115], "zoom": 5,
        "cam_lon": -115, "cam_lat": 36, "cam_alt": 2_500_000,
        "default_days": 5,
        "label": "USA West Coast",
    },
    "amazon": {
        "bbox": "-75,-20,-45,5",
        "area": [5, -75, -20, -45],
        "bounds": (-20, -75, 5, -45),
        "center": [-8, -60], "zoom": 5,
        "cam_lon": -60, "cam_lat": -10, "cam_alt": 3_000_000,
        "default_days": 5,
        "label": "Amazon Basin",
    },
}

INFRASTRUCTURE = {
    "Tehran Oil Refineries": {"lat": 35.6892, "lon": 51.3890, "type": "oil"},
    "South Pars Gas Field":  {"lat": 27.5000, "lon": 52.0000, "type": "gas"},
    "Kharg Island Terminal": {"lat": 29.2333, "lon": 50.3167, "type": "oil"},
    "Isfahan Nuclear Site":  {"lat": 32.6333, "lon": 51.6667, "type": "nuclear"},
}

STRIKE_ANNOTATIONS = [
    {"date": "2026-02-28", "label": "Operation Epic Fury begins"},
    {"date": "2026-03-07", "label": "Tehran oil depot strikes"},
    {"date": "2026-03-08", "label": "Tehran oil depot strikes (day 2)"},
    {"date": "2026-03-18", "label": "South Pars gas field attack"},
]

INFRA_CO2_RADIUS_KM = 5
SUMMARY_RADIUS_KM = 10
