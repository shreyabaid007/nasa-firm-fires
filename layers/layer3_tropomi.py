"""
Layer 3 — Sentinel-5P TROPOMI atmospheric plume analysis.

Uses the Copernicus Data Space Ecosystem OData catalogue to search for
TROPOMI L2 NO2/SO2/CO products over the conflict region, downloads
NetCDF files, computes anomaly vs. a pre-conflict baseline, and derives
a proxy CO2 cross-check via the NO2:CO2 emission ratio.

Data source: Copernicus Data Space Ecosystem (free registration)
  - Catalogue (search): https://catalogue.dataspace.copernicus.eu/odata/v1
  - Download: https://download.dataspace.copernicus.eu/odata/v1
Auth: CDSE account (free) — set CDSE_USERNAME and CDSE_PASSWORD in .env
"""

from __future__ import annotations

import math
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.models import EmissionCategory, LayerResult
from common.regions import REGIONS
from common.utils import parse_date

# Copernicus Data Space endpoints
CDSE_CATALOGUE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
CDSE_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CDSE_DOWNLOAD = "https://download.dataspace.copernicus.eu/odata/v1/Products"

# NO2:CO2 molar emission ratio for oil/gas fires
# Beirle et al. 2011; Reuter et al. 2019
NO2_CO2_RATIO_MID = 0.006
NO2_CO2_RATIO_LOW = 0.008
NO2_CO2_RATIO_HIGH = 0.004

MW_NO2 = 46.0055
MW_CO2 = 44.009

BASELINE_DAYS = 30

# S5P product types on CDSE
PRODUCTS = {
    "NO2": "L2__NO2___",
    "SO2": "L2__SO2___",
    "CO":  "L2__CO____",
}


def _get_cdse_token() -> str | None:
    """Obtain a CDSE OAuth2 bearer token."""
    username = os.environ.get("CDSE_USERNAME", "")
    password = os.environ.get("CDSE_PASSWORD", "")
    if not username or not password:
        return None
    try:
        resp = requests.post(CDSE_TOKEN_URL, data={
            "grant_type": "password",
            "username": username,
            "password": password,
            "client_id": "cdse-public",
        }, timeout=30)
        resp.raise_for_status()
        return resp.json()["access_token"]
    except Exception as exc:
        print(f"  ⚠ CDSE token request failed: {exc}")
        return None


def _build_polygon_wkt(bounds: tuple) -> str:
    """Build WKT POLYGON from (south, west, north, east) bounds."""
    s, w, n, e = bounds
    return f"POLYGON(({w} {s},{e} {s},{e} {n},{w} {n},{w} {s}))"


def _search_products(product_type: str, bounds: tuple,
                     start: str, end: str) -> list[dict]:
    """Search CDSE catalogue for S5P products (no auth needed)."""
    wkt = _build_polygon_wkt(bounds)
    filt = (
        f"Collection/Name eq 'SENTINEL-5P'"
        f" and Attributes/OData.CSC.StringAttribute/any("
        f"att:att/Name eq 'productType' and "
        f"att/OData.CSC.StringAttribute/Value eq '{product_type}')"
        f" and ContentDate/Start gt {start}T00:00:00.000Z"
        f" and ContentDate/Start lt {end}T23:59:59.999Z"
        f" and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
    )
    params = {"$filter": filt, "$top": 500, "$orderby": "ContentDate/Start asc"}

    try:
        resp = requests.get(CDSE_CATALOGUE, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json().get("value", [])
    except Exception as exc:
        print(f"  ⚠ CDSE catalogue search failed: {exc}")
        return []


def _download_product(product_id: str, token: str, out_path: str) -> str | None:
    """Download a single S5P NetCDF product."""
    url = f"{CDSE_DOWNLOAD}({product_id})/$value"
    try:
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {token}"})
        resp = session.get(url, stream=True, allow_redirects=True, timeout=120)
        resp.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return out_path
    except Exception as exc:
        print(f"  ⚠ Download failed for {product_id}: {exc}")
        return None


def _extract_mean_no2(nc_path: str, bounds: tuple) -> float | None:
    """Extract mean tropospheric NO2 column from a downloaded L2 NetCDF."""
    try:
        import xarray as xr
        ds = xr.open_dataset(nc_path, group="PRODUCT")

        no2 = ds["nitrogendioxide_tropospheric_column"]
        lat = ds["latitude"]
        lon = ds["longitude"]
        qa = ds["qa_value"]

        s, w, n, e = bounds
        mask = (lat >= s) & (lat <= n) & (lon >= w) & (lon <= e) & (qa > 0.5)
        vals = no2.where(mask).values.flatten()
        valid = vals[np.isfinite(vals)]

        if len(valid) == 0:
            return None
        return float(np.mean(valid))
    except Exception:
        return None


def _search_and_aggregate_daily(
    product_key: str, bounds: tuple, dates: list[date], token: str | None,
    tmp_dir: Path,
) -> pd.DataFrame:
    """Search and optionally download/process products for daily means."""
    product_type = PRODUCTS[product_key]
    rows = []

    for d in dates:
        ds = d.strftime("%Y-%m-%d")
        de = d.strftime("%Y-%m-%d")

        products = _search_products(product_type, bounds, ds, de)
        n_products = len(products)

        if n_products == 0:
            rows.append({"date": ds, product_key: None, "n_products": 0})
            continue

        if token:
            # Download first product and extract mean
            pid = products[0]["Id"]
            nc_path = str(tmp_dir / f"{product_key}_{ds}.nc")
            downloaded = _download_product(pid, token, nc_path)
            if downloaded:
                val = _extract_mean_no2(downloaded, bounds)
                rows.append({"date": ds, product_key: val, "n_products": n_products})
                try:
                    os.remove(nc_path)
                except OSError:
                    pass
                continue

        # Fallback: use product count as activity proxy
        rows.append({"date": ds, product_key: None, "n_products": n_products})

    return pd.DataFrame(rows)


def _proxy_co2_from_no2(no2_anomaly_mol_m2: float, area_m2: float) -> dict:
    """Estimate excess CO2 from NO2 anomaly."""
    excess_no2_mol = no2_anomaly_mol_m2 * area_m2
    co2_mid_kg = (excess_no2_mol / NO2_CO2_RATIO_MID) * MW_CO2 / 1000
    co2_low_kg = (excess_no2_mol / NO2_CO2_RATIO_LOW) * MW_CO2 / 1000
    co2_high_kg = (excess_no2_mol / NO2_CO2_RATIO_HIGH) * MW_CO2 / 1000
    return {
        "co2_mid": co2_mid_kg / 1000,
        "co2_low": co2_low_kg / 1000,
        "co2_high": co2_high_kg / 1000,
    }


def run(
    region: str = "iran",
    start_date: str | None = None,
    end_date: str | None = None,
    output_dir: str = "output",
) -> LayerResult | None:
    print(f"\n{'='*68}")
    print(f"  LAYER 3 — TROPOMI Atmospheric Plume Analysis")
    print(f"  (Copernicus Data Space Ecosystem)")
    print(f"{'='*68}")

    region_cfg = REGIONS[region]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    bounds = region_cfg["bounds"]

    from common.utils import date_range_days
    today = date.today()
    sd = parse_date(start_date) if start_date else today - timedelta(days=region_cfg["default_days"])
    ed = parse_date(end_date) if end_date else today

    baseline_start = sd - timedelta(days=BASELINE_DAYS)
    baseline_end = sd - timedelta(days=1)

    # Auth (optional — search works without, download requires it)
    token = _get_cdse_token()
    if token:
        print("  ✓ CDSE authentication successful")
    else:
        print("  ⚠ CDSE credentials not set — will search catalogue only (no download)")
        print("    Set CDSE_USERNAME and CDSE_PASSWORD in .env for full analysis")
        print("    Register free at https://dataspace.copernicus.eu")

    s, w, n, e_coord = bounds
    area_m2 = abs(n - s) * abs(e_coord - w) * (111_320 ** 2) * abs(
        np.cos(np.radians((n + s) / 2))
    )

    conflict_dates = date_range_days(sd, ed)
    baseline_dates = date_range_days(baseline_start, baseline_end)

    tmp_dir = Path(tempfile.mkdtemp(prefix="tropomi_"))

    # --- NO2 baseline ---
    print(f"  Searching NO2 baseline ({baseline_start} → {baseline_end}) …")
    bl_df = _search_and_aggregate_daily("NO2", bounds, baseline_dates, token, tmp_dir)
    baseline_mean = bl_df["NO2"].dropna().mean() if "NO2" in bl_df.columns else None

    # --- NO2 conflict period ---
    print(f"  Searching NO2 conflict period ({sd} → {ed}) …")
    conf_df = _search_and_aggregate_daily("NO2", bounds, conflict_dates, token, tmp_dir)

    # --- SO2, CO (search only for product counts — lightweight) ---
    for gas in ["SO2", "CO"]:
        print(f"  Searching {gas} conflict period …")
        gas_products = []
        for d in conflict_dates:
            ds = d.strftime("%Y-%m-%d")
            prods = _search_products(PRODUCTS[gas], bounds, ds, ds)
            gas_products.append({"date": ds, f"{gas}_products": len(prods)})
        gas_df = pd.DataFrame(gas_products)
        conf_df = conf_df.merge(gas_df, on="date", how="left")

    csv_path = out / "tropomi_daily.csv"
    conf_df.to_csv(csv_path, index=False)
    print(f"  ✓ Daily data → {csv_path}")

    # --- Compute anomaly and proxy CO2 ---
    has_no2_data = conf_df["NO2"].dropna().shape[0] > 0 if "NO2" in conf_df.columns else False

    if has_no2_data and baseline_mean and baseline_mean > 0:
        conf_df["NO2_baseline"] = baseline_mean
        conf_df["NO2_anomaly"] = conf_df["NO2"] - baseline_mean

        positive_anomaly = conf_df.loc[conf_df["NO2_anomaly"] > 0, "NO2_anomaly"]
        avg_excess = positive_anomaly.mean() if len(positive_anomaly) > 0 else 0
        n_anomaly_days = len(positive_anomaly)

        proxy = _proxy_co2_from_no2(avg_excess, area_m2)
        co2_mid = proxy["co2_mid"] * n_anomaly_days
        co2_low = proxy["co2_low"] * n_anomaly_days
        co2_high = proxy["co2_high"] * n_anomaly_days

        conflict_mean = conf_df["NO2"].dropna().mean()
        pct_increase = (conflict_mean - baseline_mean) / baseline_mean * 100

        daily_out = conf_df[["date"]].copy()
        daily_out["co2_mid"] = conf_df["NO2_anomaly"].apply(
            lambda v: _proxy_co2_from_no2(max(v, 0), area_m2)["co2_mid"]
            if pd.notna(v) else 0
        )
        daily_out["co2_low"] = daily_out["co2_mid"] * 0.5
        daily_out["co2_high"] = daily_out["co2_mid"] * 2.5

        print(f"  → NO2 baseline: {baseline_mean:.2e} mol/m²")
        print(f"  → NO2 conflict mean: {conflict_mean:.2e} mol/m²")
        print(f"  → Change: {pct_increase:+.1f}%")
        print(f"  → Proxy CO₂: {co2_mid:,.0f} t (cross-check)")
    else:
        # No download possible — report catalogue search results only
        n_no2_products = conf_df["n_products"].sum() if "n_products" in conf_df.columns else 0
        print(f"  → Found {n_no2_products} NO2 products in catalogue (download requires CDSE credentials)")

        co2_mid = 0
        co2_low = 0
        co2_high = 0
        pct_increase = 0
        baseline_mean = None

        daily_out = conf_df[["date"]].copy()
        daily_out["co2_mid"] = 0
        daily_out["co2_low"] = 0
        daily_out["co2_high"] = 0

    return LayerResult(
        layer_name="Layer 3: TROPOMI",
        emission_category=EmissionCategory.ATMOSPHERIC_VERIFICATION,
        co2_tonnes_mid=round(co2_mid, 1),
        co2_tonnes_low=round(co2_low, 1),
        co2_tonnes_high=round(co2_high, 1),
        daily_breakdown=daily_out,
        metadata={
            "source": "Sentinel-5P TROPOMI via Copernicus Data Space",
            "no2_baseline_mean_mol_m2": baseline_mean,
            "no2_pct_change": round(pct_increase, 1) if pct_increase else 0,
            "has_download_data": has_no2_data,
            "methodology": (
                "NO2 column anomaly × NO2:CO2 ratio (Beirle 2011, Reuter 2019). "
                "Independent atmospheric cross-check, not additive to total."
            ),
            "csv_path": str(csv_path),
        },
    )
