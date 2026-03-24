"""
Layer 5 — Conflict Events (Insecurity Insight via HDX).

Downloads publicly available conflict incident data from the
Humanitarian Data Exchange (HDX) — Insecurity Insight datasets.
No authentication required.

Computes proxy CO2 from:
  - Combat fuel consumption (vehicles, aircraft)
  - Embodied carbon of destroyed military equipment
  - Munitions (missiles, drones, bombs)

Data sources (all free, public, updated weekly):
  - Explosive Weapons Incident Data (Iran)
  - Attacks on Health Care Incident Data (Iran)
  - Education in Danger Incident Data (Iran)

API: HDX CKAN + direct XLSX download (no auth)
"""

from __future__ import annotations

import io
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.models import EmissionCategory, LayerResult
from common.regions import REGIONS
from common.utils import parse_date

# HDX dataset page for Insecurity Insight Iran data
HDX_DATASET_ID = "27f98773-f05d-47e9-9fba-764aab22066b"
HDX_API = "https://data.humdata.org/api/3/action/package_show"

# Direct download URLs for key resources (updated weekly by Insecurity Insight)
HDX_RESOURCES = {
    "explosive_weapons": {
        "resource_id": "bee78262-8264-4db6-9376-417c82ee47b2",
        "label": "Explosive Weapons Incidents",
    },
    "attacks_healthcare": {
        "resource_id": "8eb8a3ec-1482-46c8-aef9-d86839d1b26a",
        "label": "Attacks on Health Care",
    },
    "education_danger": {
        "resource_id": "7b6df91e-bf03-4e7e-b711-8f6a7eb407ce",
        "label": "Education in Danger",
    },
    "aid_workers": {
        "resource_id": "644cbe8d-c3bc-488d-8e52-5a625ddbf762",
        "label": "Aid Worker KIKA",
    },
}

# CO2 proxy factors (tonnes CO2 per incident) from Neimark et al. (2024)
# and Conflict & Environment Observatory (CEOBS) estimates.
INCIDENT_CO2_FACTORS = {
    "Airstrike":          {"combat_fuel": 15.0, "equipment": 30.0, "munitions": 20.0},
    "Shelling":           {"combat_fuel": 3.0,  "equipment": 10.0, "munitions": 12.0},
    "Ground attack":      {"combat_fuel": 8.0,  "equipment": 20.0, "munitions": 5.0},
    "Explosive weapon":   {"combat_fuel": 5.0,  "equipment": 15.0, "munitions": 15.0},
    "Missile":            {"combat_fuel": 2.0,  "equipment": 5.0,  "munitions": 25.0},
    "Drone":              {"combat_fuel": 1.0,  "equipment": 3.0,  "munitions": 10.0},
    "IED":                {"combat_fuel": 0.5,  "equipment": 2.0,  "munitions": 5.0},
    "Attack":             {"combat_fuel": 5.0,  "equipment": 10.0, "munitions": 8.0},
    "Raid":               {"combat_fuel": 6.0,  "equipment": 5.0,  "munitions": 3.0},
}
DEFAULT_FACTORS = {"combat_fuel": 3.0, "equipment": 5.0, "munitions": 5.0}


def _download_hdx_resource(resource_id: str) -> pd.DataFrame:
    """Download an XLSX resource from HDX (no auth required)."""
    url = (
        f"https://data.humdata.org/dataset/{HDX_DATASET_ID}"
        f"/resource/{resource_id}/download"
    )
    try:
        resp = requests.get(url, timeout=60, allow_redirects=True)
        resp.raise_for_status()
        return pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
    except Exception as exc:
        print(f"  ⚠ Download failed for resource {resource_id}: {exc}")
        return pd.DataFrame()


def _fetch_hdx_resources_via_api() -> list[dict]:
    """Use HDX CKAN API to get fresh resource URLs."""
    import requests as req
    try:
        resp = req.get(HDX_API, params={"id": HDX_DATASET_ID}, timeout=30)
        resp.raise_for_status()
        result = resp.json().get("result", {})
        return result.get("resources", [])
    except Exception:
        return []


def _detect_date_column(df: pd.DataFrame) -> str | None:
    """Find the date column in an Insecurity Insight dataset."""
    candidates = [
        "Date", "date", "Event Date", "event_date", "Incident Date",
        "Date of Incident", "Start Date",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if "date" in c.lower():
            return c
    return None


def _detect_event_type_column(df: pd.DataFrame) -> str | None:
    """Find the event/weapon type column."""
    candidates = [
        "Weapon Type", "Event Type", "Type", "Sub-event Type",
        "Weapon", "Attack Type", "Incident Type", "Category",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if "type" in c.lower() or "weapon" in c.lower():
            return c
    return None


def _detect_lat_lon(df: pd.DataFrame) -> tuple[str | None, str | None]:
    """Find latitude/longitude columns."""
    lat_candidates = ["Latitude", "latitude", "lat", "Lat"]
    lon_candidates = ["Longitude", "longitude", "lon", "Long", "Lon"]
    lat_col = next((c for c in lat_candidates if c in df.columns), None)
    lon_col = next((c for c in lon_candidates if c in df.columns), None)
    return lat_col, lon_col


def _classify_incident(text: str) -> str:
    """Classify an incident description into a CO2-relevant type."""
    t = str(text).lower()
    if any(k in t for k in ["airstrike", "air strike", "aerial"]):
        return "Airstrike"
    if any(k in t for k in ["shell", "artillery", "mortar"]):
        return "Shelling"
    if any(k in t for k in ["missile", "ballistic"]):
        return "Missile"
    if any(k in t for k in ["drone", "uav", "unmanned"]):
        return "Drone"
    if any(k in t for k in ["ied", "improvised", "car bomb"]):
        return "IED"
    if any(k in t for k in ["raid", "incursion"]):
        return "Raid"
    if any(k in t for k in ["explos", "bomb", "blast", "detona"]):
        return "Explosive weapon"
    if any(k in t for k in ["attack", "assault", "struck"]):
        return "Attack"
    return "Attack"


def _compute_co2(df: pd.DataFrame, type_col: str | None) -> pd.DataFrame:
    """Compute CO2 proxy estimates per incident."""
    if type_col:
        df["incident_class"] = df[type_col].apply(_classify_incident)
    else:
        # Try classifying from any text columns
        text_cols = [c for c in df.columns if df[c].dtype == object]
        if text_cols:
            df["incident_class"] = df[text_cols[0]].apply(_classify_incident)
        else:
            df["incident_class"] = "Attack"

    df["co2_combat_fuel"] = df["incident_class"].apply(
        lambda t: INCIDENT_CO2_FACTORS.get(t, DEFAULT_FACTORS)["combat_fuel"]
    )
    df["co2_equipment"] = df["incident_class"].apply(
        lambda t: INCIDENT_CO2_FACTORS.get(t, DEFAULT_FACTORS)["equipment"]
    )
    df["co2_munitions"] = df["incident_class"].apply(
        lambda t: INCIDENT_CO2_FACTORS.get(t, DEFAULT_FACTORS)["munitions"]
    )
    df["co2_total"] = df["co2_combat_fuel"] + df["co2_equipment"] + df["co2_munitions"]
    return df


import requests  # noqa: E402 — needed at module level for _download_hdx_resource


def run(
    region: str = "iran",
    start_date: str | None = None,
    end_date: str | None = None,
    output_dir: str = "output",
) -> LayerResult | None:
    print(f"\n{'='*68}")
    print(f"  LAYER 5 — Conflict Events (Insecurity Insight / HDX)")
    print(f"  No authentication required — public humanitarian data")
    print(f"{'='*68}")

    region_cfg = REGIONS[region]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    today = date.today()
    sd = parse_date(start_date) if start_date else today - timedelta(days=region_cfg["default_days"])
    ed = parse_date(end_date) if end_date else today

    all_dfs = []
    for key, info in HDX_RESOURCES.items():
        print(f"  Downloading {info['label']} …")
        df = _download_hdx_resource(info["resource_id"])
        if not df.empty:
            df["_source"] = info["label"]
            all_dfs.append(df)
            print(f"    → {len(df)} rows")
        else:
            print(f"    → no data")

    if not all_dfs:
        print("  ⚠ No data retrieved from HDX.")
        return None

    combined = pd.concat(all_dfs, ignore_index=True)

    # Filter by date range
    date_col = _detect_date_column(combined)
    if date_col:
        combined[date_col] = pd.to_datetime(combined[date_col], errors="coerce")
        combined = combined.dropna(subset=[date_col])
        combined["event_date"] = combined[date_col].dt.strftime("%Y-%m-%d")
        mask = (combined["event_date"] >= str(sd)) & (combined["event_date"] <= str(ed))
        combined = combined[mask].copy()
    else:
        combined["event_date"] = str(today)

    if combined.empty:
        print(f"  ⚠ No incidents in date range {sd} → {ed}")
        return None

    print(f"  → {len(combined)} incidents in {sd} → {ed}")

    type_col = _detect_event_type_column(combined)
    combined = _compute_co2(combined, type_col)

    csv_path = out / "conflict_incidents.csv"
    safe_cols = [c for c in combined.columns if combined[c].dtype != object
                 or c in ["event_date", "incident_class", "_source"]]
    lat_col, lon_col = _detect_lat_lon(combined)
    if lat_col:
        safe_cols.append(lat_col)
    if lon_col:
        safe_cols.append(lon_col)
    export_cols = list(dict.fromkeys(safe_cols + [
        "co2_combat_fuel", "co2_equipment", "co2_munitions", "co2_total",
    ]))
    export_cols = [c for c in export_cols if c in combined.columns]
    combined[export_cols].to_csv(csv_path, index=False)
    print(f"  ✓ Incidents CSV → {csv_path}")

    co2_combat = combined["co2_combat_fuel"].sum()
    co2_equip = combined["co2_equipment"].sum()
    co2_mun = combined["co2_munitions"].sum()
    co2_total = combined["co2_total"].sum()

    incident_counts = combined["incident_class"].value_counts().to_dict()

    daily = combined.groupby("event_date").agg(
        co2_mid=("co2_total", "sum"),
    ).reset_index().rename(columns={"event_date": "date"})
    daily["co2_low"] = daily["co2_mid"] * 0.3
    daily["co2_high"] = daily["co2_mid"] * 3.0

    geo = []
    if lat_col and lon_col:
        for _, r in combined.head(3000).iterrows():
            try:
                lat = float(r[lat_col])
                lon = float(r[lon_col])
                if lat != 0 and lon != 0:
                    geo.append({
                        "lat": lat, "lon": lon,
                        "type": r.get("incident_class", ""),
                        "date": r.get("event_date", ""),
                        "source": "Insecurity Insight / HDX",
                    })
            except (ValueError, TypeError):
                pass

    print(f"\n  Incident breakdown:")
    for itype, count in incident_counts.items():
        print(f"    {itype:.<30s} {count:>5d}")
    print(f"  Total estimated CO₂: {co2_total:,.0f} t")
    print(f"    Combat fuel: {co2_combat:,.0f} t")
    print(f"    Equipment:   {co2_equip:,.0f} t")
    print(f"    Munitions:   {co2_mun:,.0f} t")

    return LayerResult(
        layer_name="Layer 5: Conflict Events",
        emission_category=EmissionCategory.COMBAT_FUEL,
        co2_tonnes_mid=round(co2_total, 1),
        co2_tonnes_low=round(co2_total * 0.3, 1),
        co2_tonnes_high=round(co2_total * 3.0, 1),
        daily_breakdown=daily,
        geo_points=geo,
        metadata={
            "source": "Insecurity Insight via HDX (public, no auth)",
            "total_incidents": len(combined),
            "incident_breakdown": incident_counts,
            "co2_combat_fuel": round(co2_combat, 1),
            "co2_equipment": round(co2_equip, 1),
            "co2_munitions": round(co2_mun, 1),
            "data_sources": [info["label"] for info in HDX_RESOURCES.values()],
            "methodology": "Per-incident proxy factors (Neimark et al. 2024, CEOBS)",
            "csv_path": str(csv_path),
        },
    )
