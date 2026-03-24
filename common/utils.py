"""
Shared utility functions for all layers.
"""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta

import requests


R_EARTH_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R_EARTH_KM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def bbox_to_latlng_bounds(bbox_str: str) -> tuple[float, float, float, float]:
    """Convert FIRMS bbox string 'west,south,east,north' to (south,west,north,east)."""
    if bbox_str == "world":
        return (-90, -180, 90, 180)
    w, s, e, n = (float(x) for x in bbox_str.split(","))
    return (s, w, n, e)


def bounds_to_ee_rect(bounds: tuple[float, float, float, float]):
    """Convert (south, west, north, east) to an ee.Geometry.Rectangle [W,S,E,N]."""
    s, w, n, e = bounds
    return [w, s, e, n]


def http_get_with_retry(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    retries: int = 3,
    timeout: int = 120,
    label: str = "",
) -> requests.Response:
    """GET with exponential backoff. Returns response or raises on final failure."""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            wait = 2 ** attempt
            tag = f" [{label}]" if label else ""
            print(f"  ⚠ attempt {attempt + 1}/{retries} failed{tag}: {exc}")
            if attempt < retries - 1:
                print(f"    retrying in {wait}s …")
                time.sleep(wait)
    raise last_exc  # type: ignore[misc]


def date_range_days(start: date, end: date) -> list[date]:
    """Return inclusive list of dates from start to end."""
    n = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(n)]


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()
