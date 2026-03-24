"""
Layer 7 — OpenSky Network airspace monitoring.

Uses the OpenSky Network REST API to monitor airspace density around the
conflict zone and estimate CO2 from aviation rerouting (flights avoiding
closed airspace → longer routes → extra fuel burn → extra CO2).

API: https://opensky-network.org/api/states/all (direct HTTP, no library)
Auth: Optional (anonymous has rate limits)
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.models import EmissionCategory, LayerResult
from common.regions import REGIONS
from common.utils import haversine_km, http_get_with_retry, parse_date

OPENSKY_API = "https://opensky-network.org/api"

# CO2 per extra km of flight (Eurocontrol / ICAO average)
# Based on: average fuel burn 3.5 kg/km, emission factor 3.16 kg CO2/kg jet fuel
CO2_PER_EXTRA_KM = 3.5 * 3.16 / 1000  # tonnes CO2 per km ≈ 0.01106

# Iran NOTAM closure: flights rerouted around Iranian airspace
# Average additional distance per affected flight
IRAN_CLOSURE_EXTRA_KM = 600  # km (estimate: Gulf routing via Oman/Turkmenistan)

# Pre-conflict baseline daily overflights (from historical OpenSky data)
BASELINE_DAILY_OVERFLIGHTS = {
    "iran": 450,
    "middle_east": 2000,
}


def _fetch_current_states(bounds: tuple) -> pd.DataFrame:
    """Fetch current aircraft states within bounds from OpenSky REST API."""
    s, w, n, e = bounds
    params = {
        "lamin": s, "lamax": n,
        "lomin": w, "lomax": e,
    }

    username = os.environ.get("OPENSKY_USERNAME", "")
    password = os.environ.get("OPENSKY_PASSWORD", "")
    auth = None
    if username and password:
        auth = (username, password)

    import requests
    try:
        if auth:
            resp = requests.get(f"{OPENSKY_API}/states/all", params=params,
                                auth=auth, timeout=30)
        else:
            resp = requests.get(f"{OPENSKY_API}/states/all", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"  ⚠ OpenSky API error: {exc}")
        return pd.DataFrame()

    states = data.get("states", [])
    if not states:
        return pd.DataFrame()

    cols = [
        "icao24", "callsign", "origin_country", "time_position",
        "last_contact", "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk", "spi", "position_source",
    ]
    df = pd.DataFrame(states, columns=cols[:len(states[0])])
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    return df.dropna(subset=["latitude", "longitude"])


def _estimate_rerouting_co2(
    region: str, current_count: int, n_days: int,
) -> dict:
    """Estimate CO2 from aviation rerouting due to airspace closures."""
    baseline = BASELINE_DAILY_OVERFLIGHTS.get(region, 300)

    # Number of flights affected = flights that would have crossed but are now rerouted
    daily_affected = max(0, baseline - current_count)

    total_affected = daily_affected * n_days
    extra_km = IRAN_CLOSURE_EXTRA_KM
    co2_mid = total_affected * extra_km * CO2_PER_EXTRA_KM

    return {
        "baseline_daily": baseline,
        "current_daily": current_count,
        "daily_affected": daily_affected,
        "total_affected_flights": total_affected,
        "extra_km_per_flight": extra_km,
        "co2_mid": co2_mid,
        "co2_low": co2_mid * 0.5,
        "co2_high": co2_mid * 1.5,
    }


def run(
    region: str = "iran",
    start_date: str | None = None,
    end_date: str | None = None,
    output_dir: str = "output",
) -> LayerResult | None:
    print(f"\n{'='*68}")
    print(f"  LAYER 7 — OpenSky Airspace Monitoring")
    print(f"{'='*68}")

    region_cfg = REGIONS[region]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    today = date.today()
    sd = parse_date(start_date) if start_date else today - timedelta(days=region_cfg["default_days"])
    ed = parse_date(end_date) if end_date else today
    n_days = (ed - sd).days + 1
    bounds = region_cfg["bounds"]

    print(f"  Fetching current airspace snapshot over {region_cfg['label']} …")
    states_df = _fetch_current_states(bounds)

    current_count = len(states_df)
    print(f"  → {current_count} aircraft currently in airspace")

    if not states_df.empty:
        csv_path = out / "opensky_snapshot.csv"
        states_df.to_csv(csv_path, index=False)
        print(f"  ✓ Aircraft snapshot → {csv_path}")
    else:
        csv_path = None

    reroute = _estimate_rerouting_co2(region, current_count, n_days)

    print(f"  → Baseline daily overflights: {reroute['baseline_daily']}")
    print(f"  → Current snapshot count: {current_count}")
    print(f"  → Estimated daily rerouted flights: {reroute['daily_affected']}")
    print(f"  → Total rerouting CO₂ ({n_days} days): {reroute['co2_mid']:,.0f} tonnes")

    dates = pd.date_range(str(sd), str(ed), freq="D")
    daily = pd.DataFrame({"date": dates.strftime("%Y-%m-%d")})
    daily["co2_mid"] = reroute["co2_mid"] / max(n_days, 1)
    daily["co2_low"] = reroute["co2_low"] / max(n_days, 1)
    daily["co2_high"] = reroute["co2_high"] / max(n_days, 1)

    geo = [
        {"lat": r["latitude"], "lon": r["longitude"],
         "callsign": str(r.get("callsign", "")).strip(),
         "altitude": r.get("baro_altitude", 0),
         "source": "OpenSky"}
        for _, r in states_df.head(2000).iterrows()
    ]

    return LayerResult(
        layer_name="Layer 7: Aviation Rerouting",
        emission_category=EmissionCategory.AVIATION_REROUTING,
        co2_tonnes_mid=round(reroute["co2_mid"], 1),
        co2_tonnes_low=round(reroute["co2_low"], 1),
        co2_tonnes_high=round(reroute["co2_high"], 1),
        daily_breakdown=daily,
        geo_points=geo,
        metadata={
            "source": "OpenSky Network REST API",
            "snapshot_aircraft": current_count,
            "baseline_daily_overflights": reroute["baseline_daily"],
            "daily_rerouted_flights": reroute["daily_affected"],
            "extra_km_per_flight": reroute["extra_km_per_flight"],
            "n_days": n_days,
            "methodology": (
                f"{reroute['daily_affected']} rerouted flights/day × "
                f"{IRAN_CLOSURE_EXTRA_KM} km extra × "
                f"{CO2_PER_EXTRA_KM:.4f} t CO₂/km"
            ),
            "csv_path": str(csv_path) if csv_path else None,
        },
    )
