"""
Layer 4 — VIIRS Nightfire (EOG / Payne Institute).

Downloads nighttime fire/flare detections from the EOG VIIRS Nightfire
product.  Classifies by temperature (gas flare > 1600 K vs oil fire)
and computes refined oil/gas CO2 from estimated burn rates.

API: eogdata.mines.edu (OAuth2.0 for downloading CSVs)
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.models import EmissionCategory, LayerResult
from common.regions import REGIONS
from common.utils import haversine_km, http_get_with_retry, parse_date

EOG_TOKEN_URL = "https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token"
EOG_DATA_URL = "https://eogdata.mines.edu/wwwdata/viirs_products/vnf/v31"

FLARE_TEMP_THRESHOLD_K = 1600  # above → gas flare; below → oil/other

# Burn-rate factors from Elvidge et al. (2016)
# RH = Radiant Heat output (MW). Volume burn rate estimation:
#   Q (m³/s) = RH / (ρ × ΔH_c)
# Then CO2 = Q × ρ × Δt × EF
# Simplified: for gas flares, ~2.75 kg CO₂ per MW per 6-hour window
CO2_PER_MW_6H_GAS = 2.75  # tonnes CO2 per MW sustained 6h (gas flare)
CO2_PER_MW_6H_OIL = 4.10  # tonnes CO2 per MW sustained 6h (oil fire)


def _get_eog_token() -> str | None:
    """Obtain an EOG OAuth2 bearer token from env credentials."""
    client_id = os.environ.get("EOG_CLIENT_ID", "")
    client_secret = os.environ.get("EOG_CLIENT_SECRET", "")
    username = os.environ.get("EOG_USERNAME", "")
    password = os.environ.get("EOG_PASSWORD", "")

    if not all([client_id, username, password]):
        return None

    import requests
    try:
        resp = requests.post(EOG_TOKEN_URL, data={
            "grant_type": "password",
            "client_id": client_id,
            "client_secret": client_secret,
            "username": username,
            "password": password,
        }, timeout=30)
        resp.raise_for_status()
        return resp.json()["access_token"]
    except Exception as exc:
        print(f"  ⚠ EOG token request failed: {exc}")
        return None


def _download_nightfire(token: str, year: int, month: int, bounds: tuple) -> pd.DataFrame:
    """Download a monthly VNF CSV file and filter to bounds."""
    ym = f"{year}{month:02d}"
    url = f"{EOG_DATA_URL}/{year}/VNF_{ym}.csv.gz"

    import requests
    try:
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
        resp.raise_for_status()
    except Exception as exc:
        print(f"  ⚠ Download failed for {url}: {exc}")
        return pd.DataFrame()

    df = pd.read_csv(StringIO(resp.text))

    s, w, n, e = bounds
    mask = (
        (df["Lat_GMTCO"].between(s, n)) &
        (df["Lon_GMTCO"].between(w, e))
    )
    return df[mask].copy()


def _classify(df: pd.DataFrame) -> pd.DataFrame:
    """Add fire type classification based on temperature."""
    if df.empty:
        return df
    temp_col = "Temp_BB" if "Temp_BB" in df.columns else "BB_Temp"
    if temp_col not in df.columns:
        df["fire_type"] = "unknown"
        return df

    df["fire_type"] = df[temp_col].apply(
        lambda t: "gas_flare" if t > FLARE_TEMP_THRESHOLD_K else "oil_fire"
    )
    return df


def _estimate_co2(df: pd.DataFrame) -> pd.DataFrame:
    """Compute CO2 estimates from Radiant Heat."""
    rh_col = "RHI" if "RHI" in df.columns else "RH"
    if rh_col not in df.columns:
        df["co2_tonnes"] = 0.0
        return df

    df["co2_tonnes"] = df.apply(
        lambda r: r[rh_col] * CO2_PER_MW_6H_GAS / 1e3
        if r.get("fire_type") == "gas_flare"
        else r[rh_col] * CO2_PER_MW_6H_OIL / 1e3,
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
    print(f"  LAYER 4 — VIIRS Nightfire (Gas Flares / Oil Fires)")
    print(f"{'='*68}")

    token = _get_eog_token()
    if token is None:
        print("  ⚠ Skipping Layer 4 — EOG credentials not set.")
        print("    Set EOG_CLIENT_ID, EOG_CLIENT_SECRET, EOG_USERNAME, EOG_PASSWORD in .env")
        return None

    region_cfg = REGIONS[region]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    today = date.today()
    sd = parse_date(start_date) if start_date else today - timedelta(days=region_cfg["default_days"])
    ed = parse_date(end_date) if end_date else today
    bounds = region_cfg["bounds"]

    months_needed = set()
    d = sd
    while d <= ed:
        months_needed.add((d.year, d.month))
        d += timedelta(days=28)
    months_needed.add((ed.year, ed.month))

    all_dfs = []
    for year, month in sorted(months_needed):
        print(f"  Downloading Nightfire {year}-{month:02d} …")
        mdf = _download_nightfire(token, year, month, bounds)
        if not mdf.empty:
            all_dfs.append(mdf)

    if not all_dfs:
        print("  ⚠ No Nightfire data retrieved.")
        return None

    df = pd.concat(all_dfs, ignore_index=True)
    df = _classify(df)
    df = _estimate_co2(df)

    date_col = "Date_Mscan" if "Date_Mscan" in df.columns else "Date_LTZ"
    if date_col in df.columns:
        df["acq_date"] = pd.to_datetime(df[date_col]).dt.date.astype(str)
        mask = (df["acq_date"] >= str(sd)) & (df["acq_date"] <= str(ed))
        df = df[mask].copy()

    csv_path = out / "nightfire_detections.csv"
    df.to_csv(csv_path, index=False)
    print(f"  ✓ {len(df)} Nightfire detections → {csv_path}")

    n_flares = (df["fire_type"] == "gas_flare").sum()
    n_oil = (df["fire_type"] == "oil_fire").sum()
    co2_total = df["co2_tonnes"].sum()

    daily = df.groupby("acq_date").agg(co2_mid=("co2_tonnes", "sum")).reset_index()
    daily.rename(columns={"acq_date": "date"}, inplace=True)
    daily["co2_low"] = daily["co2_mid"] * 0.5
    daily["co2_high"] = daily["co2_mid"] * 2.0

    lat_col = "Lat_GMTCO" if "Lat_GMTCO" in df.columns else "latitude"
    lon_col = "Lon_GMTCO" if "Lon_GMTCO" in df.columns else "longitude"
    geo = [
        {"lat": r[lat_col], "lon": r[lon_col],
         "type": r["fire_type"], "source": "Nightfire"}
        for _, r in df.head(3000).iterrows()
        if lat_col in r.index and lon_col in r.index
    ]

    return LayerResult(
        layer_name="Layer 4: Nightfire",
        emission_category=EmissionCategory.FUEL_INFRASTRUCTURE,
        co2_tonnes_mid=round(co2_total, 1),
        co2_tonnes_low=round(co2_total * 0.5, 1),
        co2_tonnes_high=round(co2_total * 2.0, 1),
        daily_breakdown=daily,
        geo_points=geo,
        metadata={
            "source": "EOG VIIRS Nightfire v3.1",
            "total_detections": len(df),
            "gas_flares": int(n_flares),
            "oil_fires": int(n_oil),
            "methodology": "RH (MW) × fuel-specific burn rate (Elvidge et al. 2016)",
            "csv_path": str(csv_path),
        },
    )
