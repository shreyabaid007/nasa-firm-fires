"""
Layer 6 — UNOSAT / Humanitarian Data Exchange building damage.

Searches the HDX CKAN API for satellite-assessed building damage
reports, downloads GeoJSON/CSV, and computes embodied carbon of
destroyed structures using ICE Database factors.

API: https://data.humdata.org/api/3/action/package_search
Auth: None (public data)
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.models import EmissionCategory, LayerResult
from common.regions import REGIONS
from common.utils import http_get_with_retry, parse_date

HDX_API = "https://data.humdata.org/api/3/action/package_search"

# Embodied carbon per m² of building, by damage level
# Source: ICE Database v3 (Bath Inventory of Carbon and Energy)
# and Neimark et al. (2024) conflict estimates.
# Units: tonnes CO2e per m²
EMBODIED_CO2_PER_M2 = {
    "destroyed":        0.50,  # full demolition: concrete + rebar + finishes
    "severe_damage":    0.35,
    "moderate_damage":  0.15,
    "light_damage":     0.05,
}

# Average building footprint if not specified (m²)
DEFAULT_BUILDING_AREA_M2 = 120.0

# Typical stories
DEFAULT_STORIES = 2.5

# Research-based fallback: if we cannot get granular data, use aggregate
# per-building estimates from CEOBS / post-conflict assessments.
FALLBACK_CO2_PER_DESTROYED_BUILDING = 60.0  # tonnes CO2e
FALLBACK_CO2_PER_DAMAGED_BUILDING = 20.0


def _search_hdx(country: str) -> list[dict]:
    """Search HDX for UNOSAT building damage datasets."""
    params = {
        "q": f"UNOSAT damage assessment {country}",
        "fq": f'organization:unosat res_format:"CSV" OR res_format:"GEOJSON"',
        "rows": 10,
        "sort": "metadata_modified desc",
    }
    try:
        resp = http_get_with_retry(HDX_API, params=params, label="HDX search")
        data = resp.json()
        return data.get("result", {}).get("results", [])
    except Exception as exc:
        print(f"  ⚠ HDX search failed: {exc}")
        return []


def _find_downloadable_resource(package: dict) -> str | None:
    """Find a CSV or GeoJSON resource URL in an HDX package."""
    for res in package.get("resources", []):
        fmt = res.get("format", "").upper()
        if fmt in ("CSV", "GEOJSON", "JSON", "XLSX"):
            return res.get("url")
    return None


def _download_damage_data(url: str) -> pd.DataFrame:
    """Download and parse a damage dataset (CSV or GeoJSON)."""
    try:
        if url.endswith(".geojson") or url.endswith(".json"):
            import json
            resp = http_get_with_retry(url, label="HDX download")
            geojson = resp.json()
            features = geojson.get("features", [])
            rows = []
            for f in features:
                props = f.get("properties", {})
                geom = f.get("geometry", {})
                coords = geom.get("coordinates", [0, 0])
                if geom.get("type") == "Point":
                    lon, lat = coords[0], coords[1]
                else:
                    lon, lat = 0, 0
                props["longitude"] = lon
                props["latitude"] = lat
                rows.append(props)
            return pd.DataFrame(rows)
        else:
            resp = http_get_with_retry(url, label="HDX download")
            from io import StringIO
            return pd.read_csv(StringIO(resp.text))
    except Exception as exc:
        print(f"  ⚠ Download/parse failed: {exc}")
        return pd.DataFrame()


def _detect_damage_column(df: pd.DataFrame) -> str | None:
    """Heuristic to find the damage classification column."""
    candidates = [
        "damage", "damage_level", "Damage", "damage_class",
        "dmg_level", "Main_Damage", "DAMAGE", "damage_type",
        "Damage_Lev", "dmg_cls",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if "damage" in c.lower() or "dmg" in c.lower():
            return c
    return None


def _map_damage_level(value: str) -> str:
    """Normalize damage level string to our categories."""
    v = str(value).lower().strip()
    if any(k in v for k in ["destroy", "demolish", "razed", "total"]):
        return "destroyed"
    if any(k in v for k in ["severe", "heavy", "major"]):
        return "severe_damage"
    if any(k in v for k in ["moderate", "partial"]):
        return "moderate_damage"
    if any(k in v for k in ["light", "minor", "slight"]):
        return "light_damage"
    return "moderate_damage"  # default


def _compute_building_co2(df: pd.DataFrame, damage_col: str) -> pd.DataFrame:
    """Compute embodied carbon for each building."""
    df["damage_normalized"] = df[damage_col].apply(_map_damage_level)

    area_col = None
    for c in df.columns:
        if "area" in c.lower() or "sqm" in c.lower() or "footprint" in c.lower():
            area_col = c
            break

    df["building_area_m2"] = (
        pd.to_numeric(df[area_col], errors="coerce").fillna(DEFAULT_BUILDING_AREA_M2)
        if area_col else DEFAULT_BUILDING_AREA_M2
    )

    df["total_area_m2"] = df["building_area_m2"] * DEFAULT_STORIES

    df["co2_tonnes"] = df.apply(
        lambda r: r["total_area_m2"] * EMBODIED_CO2_PER_M2.get(r["damage_normalized"], 0.15),
        axis=1,
    )
    return df


def run(
    region: str = "iran",
    start_date: str | None = None,
    end_date: str | None = None,
    output_dir: str = "output",
) -> LayerResult | None:
    print(f"\n{'='*68}")
    print(f"  LAYER 6 — UNOSAT Building Damage (Embodied Carbon)")
    print(f"{'='*68}")

    region_cfg = REGIONS[region]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    country_map = {"iran": "Iran", "middle_east": "Iran"}
    country = country_map.get(region, region_cfg["label"])

    print(f"  Searching HDX for UNOSAT damage assessments: {country} …")
    packages = _search_hdx(country)

    if not packages:
        print("  ⚠ No UNOSAT datasets found on HDX. Using research-based fallback.")
        return _fallback_result(region, start_date, end_date, out)

    all_dfs = []
    for pkg in packages[:3]:
        title = pkg.get("title", "untitled")
        print(f"  → Found: {title}")
        url = _find_downloadable_resource(pkg)
        if url:
            df = _download_damage_data(url)
            if not df.empty:
                all_dfs.append(df)

    if not all_dfs:
        print("  ⚠ Could not download any damage data. Using research-based fallback.")
        return _fallback_result(region, start_date, end_date, out)

    combined = pd.concat(all_dfs, ignore_index=True)
    damage_col = _detect_damage_column(combined)

    if damage_col is None:
        print("  ⚠ No damage classification column found. Using fallback.")
        return _fallback_result(region, start_date, end_date, out, n_buildings=len(combined))

    combined = _compute_building_co2(combined, damage_col)

    csv_path = out / "unosat_building_damage.csv"
    combined.to_csv(csv_path, index=False)
    print(f"  ✓ {len(combined)} buildings → {csv_path}")

    co2_total = combined["co2_tonnes"].sum()
    damage_counts = combined["damage_normalized"].value_counts().to_dict()

    lat_col = next((c for c in combined.columns if c.lower() in ("latitude", "lat", "y")), None)
    lon_col = next((c for c in combined.columns if c.lower() in ("longitude", "lon", "long", "x")), None)

    geo = []
    if lat_col and lon_col:
        for _, r in combined.head(3000).iterrows():
            try:
                geo.append({
                    "lat": float(r[lat_col]), "lon": float(r[lon_col]),
                    "damage": r["damage_normalized"], "source": "UNOSAT",
                })
            except (ValueError, TypeError):
                pass

    daily = pd.DataFrame({"date": [str(date.today())], "co2_mid": [co2_total],
                          "co2_low": [co2_total * 0.5], "co2_high": [co2_total * 2.0]})

    return LayerResult(
        layer_name="Layer 6: UNOSAT Buildings",
        emission_category=EmissionCategory.BUILDINGS,
        co2_tonnes_mid=round(co2_total, 1),
        co2_tonnes_low=round(co2_total * 0.5, 1),
        co2_tonnes_high=round(co2_total * 2.0, 1),
        daily_breakdown=daily,
        geo_points=geo,
        metadata={
            "source": "UNOSAT via Humanitarian Data Exchange",
            "total_buildings": len(combined),
            "damage_breakdown": damage_counts,
            "methodology": "Building area × stories × ICE embodied carbon factors",
            "csv_path": str(csv_path),
        },
    )


def _fallback_result(
    region: str, start_date: str | None, end_date: str | None,
    out: Path, n_buildings: int = 0,
) -> LayerResult | None:
    """Return OCHA-sourced estimate when granular UNOSAT geodata is unavailable.

    OCHA Humanitarian Update No.01 (17 Mar 2026) reports 54,000+ civilian
    units damaged across 20+ provinces.  Iranian Red Crescent assessed 6,668
    civilian infrastructures damaged (hospitals, schools, sports facilities).
    56 cultural heritage sites damaged including Golestan Palace.
    """
    # OCHA figures (Humanitarian Update No.01, 17 Mar 2026)
    total_units = max(n_buildings, 54_000)

    # Damage distribution estimated from OCHA narrative + CEOBS patterns:
    # ~8% destroyed, ~15% severe, ~35% moderate, ~42% light
    est_destroyed = int(total_units * 0.08)       # ~4,320
    est_severe = int(total_units * 0.15)           # ~8,100
    est_moderate = int(total_units * 0.35)         # ~18,900
    est_light = total_units - est_destroyed - est_severe - est_moderate  # ~22,680

    co2_mid = (
        est_destroyed * EMBODIED_CO2_PER_M2["destroyed"] * DEFAULT_BUILDING_AREA_M2 * DEFAULT_STORIES
        + est_severe * EMBODIED_CO2_PER_M2["severe_damage"] * DEFAULT_BUILDING_AREA_M2 * DEFAULT_STORIES
        + est_moderate * EMBODIED_CO2_PER_M2["moderate_damage"] * DEFAULT_BUILDING_AREA_M2 * DEFAULT_STORIES
        + est_light * EMBODIED_CO2_PER_M2["light_damage"] * DEFAULT_BUILDING_AREA_M2 * DEFAULT_STORIES
    )
    co2_low = co2_mid * 0.5
    co2_high = co2_mid * 2.0

    daily = pd.DataFrame({"date": [str(date.today())], "co2_mid": [co2_mid],
                          "co2_low": [co2_low], "co2_high": [co2_high]})

    print(f"  Source: OCHA Humanitarian Update No.01 (17 Mar 2026)")
    print(f"  → 54,000+ civilian units damaged across 20+ provinces")
    print(f"  → Estimated breakdown:")
    print(f"      Destroyed:  {est_destroyed:>6,d}")
    print(f"      Severe:     {est_severe:>6,d}")
    print(f"      Moderate:   {est_moderate:>6,d}")
    print(f"      Light:      {est_light:>6,d}")
    print(f"  → Estimated embodied CO₂: {co2_mid:,.0f} tonnes")

    return LayerResult(
        layer_name="Layer 6: Building Damage (OCHA est.)",
        emission_category=EmissionCategory.BUILDINGS,
        co2_tonnes_mid=round(co2_mid, 1),
        co2_tonnes_low=round(co2_low, 1),
        co2_tonnes_high=round(co2_high, 1),
        daily_breakdown=daily,
        metadata={
            "source": "OCHA Humanitarian Update No.01 (17 Mar 2026) + ICE Database",
            "ocha_total_units_damaged": total_units,
            "est_destroyed": est_destroyed,
            "est_severe": est_severe,
            "est_moderate": est_moderate,
            "est_light": est_light,
            "building_area_m2": DEFAULT_BUILDING_AREA_M2,
            "stories": DEFAULT_STORIES,
            "methodology": (
                "OCHA aggregate figure × damage distribution × "
                "ICE v3 embodied carbon factors per m²"
            ),
            "note": (
                "Granular UNOSAT georeferenced data not yet published; "
                "using OCHA's 54,000 damaged units figure with estimated damage distribution"
            ),
        },
    )
