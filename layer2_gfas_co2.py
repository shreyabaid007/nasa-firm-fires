#!/usr/bin/env python3
"""
Layer 2 — CAMS GFAS CO₂ Fire Emissions (Copernicus)

Fetches peer-reviewed daily CO₂ emission estimates from the CAMS Global Fire
Assimilation System (GFAS) and compares them against the FIRMS FRP-based rough
estimates produced by Layer 1.

GFAS methodology (Copernicus/ECMWF):
  • Assimilates MODIS + VIIRS fire radiative power (FRP) observations
  • Applies land-cover-specific emission factors
  • Integrates FRP over time to compute Fire Radiative Energy (FRE)
  • Uses the Wooster et al. (2005) FRE-to-biomass conversion
  • Provides gridded daily CO₂ flux at 0.1° resolution

Setup (one-time, free):
  1. Register at  https://ads.atmosphere.copernicus.eu/
  2. Accept GFAS terms:  https://ads.atmosphere.copernicus.eu/datasets/cams-global-fire-emissions-gfas
  3. Copy your Personal Access Token from your profile page
  4. Paste it in .env as  ADS_API_KEY=<your-token>
"""

import argparse
import json
import math
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Region presets  (CDS area = [North, West, South, East])
# ---------------------------------------------------------------------------

REGIONS = {
    "world": {
        "area": None,
        "center": [20, 0], "zoom": 2,
        "default_days": 7,
        "label": "World",
    },
    "iran": {
        "area": [40, 44, 25, 63.5],
        "center": [32, 53], "zoom": 6,
        "default_days": 30,
        "label": "Iran",
    },
    "middle_east": {
        "area": [42, 25, 12, 65],
        "center": [28, 45], "zoom": 5,
        "default_days": 14,
        "label": "Middle East",
    },
    "north_america": {
        "area": [75, -170, 10, -50],
        "center": [45, -100], "zoom": 3,
        "default_days": 7,
        "label": "North America",
    },
    "south_america": {
        "area": [14, -85, -57, -32],
        "center": [-15, -60], "zoom": 3,
        "default_days": 7,
        "label": "South America",
    },
    "europe": {
        "area": [72, -15, 35, 45],
        "center": [50, 15], "zoom": 4,
        "default_days": 7,
        "label": "Europe",
    },
    "africa": {
        "area": [40, -20, -37, 55],
        "center": [5, 20], "zoom": 3,
        "default_days": 7,
        "label": "Africa",
    },
    "south_asia": {
        "area": [40, 60, 5, 100],
        "center": [22, 80], "zoom": 4,
        "default_days": 7,
        "label": "South Asia",
    },
    "southeast_asia": {
        "area": [30, 90, -15, 155],
        "center": [10, 120], "zoom": 4,
        "default_days": 7,
        "label": "Southeast Asia",
    },
    "australia": {
        "area": [-5, 110, -50, 180],
        "center": [-25, 135], "zoom": 4,
        "default_days": 7,
        "label": "Australia / Oceania",
    },
}

R_EARTH = 6_371_000.0  # metres

# ---------------------------------------------------------------------------
# Grid geometry
# ---------------------------------------------------------------------------

def cell_areas_m2(lats: np.ndarray, dlon_deg: float = 0.1, dlat_deg: float = 0.1) -> np.ndarray:
    """Return a 1-D array of grid-cell areas (m²) for each latitude band."""
    dlon_rad = math.radians(dlon_deg)
    half = dlat_deg / 2.0
    north = np.radians(lats + half)
    south = np.radians(lats - half)
    return R_EARTH ** 2 * dlon_rad * np.abs(np.sin(north) - np.sin(south))

# ---------------------------------------------------------------------------
# GFAS data fetching
# ---------------------------------------------------------------------------

def _build_client():
    """Create a cdsapi.Client, preferring ADS_API_KEY from .env."""
    import cdsapi

    key = os.getenv("ADS_API_KEY", "").strip()
    if key:
        return cdsapi.Client(
            url="https://ads.atmosphere.copernicus.eu/api",
            key=key,
        )
    # Fall back to ~/.cdsapirc
    return cdsapi.Client()


GFAS_ADS_CUTOFF = "2025-12-03"

def fetch_gfas(area: list | None, date_start: str, date_end: str,
               out_path: str) -> str | None:
    """Download GFAS CO₂ fire-emission flux from the Atmosphere Data Store.

    Returns the path to the downloaded file, or None if the data is
    unavailable (e.g. dates after the ADS discontinuation).
    """
    if date_start > GFAS_ADS_CUTOFF:
        print(f"\n  ⚠  GFAS v1.2 on the public ADS was discontinued on {GFAS_ADS_CUTOFF}.")
        print(f"     Your requested period ({date_start} → {date_end}) is after the cutoff.")
        print(f"     The new GFAS v1.4.2 (VIIRS-based) is only available via ECMWF FTP.")
        _print_ecmwf_ftp_instructions()
        return None

    effective_end = min(date_end, GFAS_ADS_CUTOFF)
    if effective_end != date_end:
        print(f"  ⚠  GFAS ADS data ends at {GFAS_ADS_CUTOFF}; clamping end date"
              f" from {date_end} to {effective_end}")

    client = _build_client()

    request: dict = {
        "variable": ["wildfire_flux_of_carbon_dioxide"],
        "date": f"{date_start}/{effective_end}",
    }
    if area is not None:
        request["area"] = area  # [N, W, S, E]

    for fmt, ext in [("netcdf_zip", ".nc"), ("netcdf", ".nc"), ("grib", ".grib")]:
        try:
            request["data_format"] = fmt
            target = out_path if out_path.endswith(ext) else out_path + ext
            print(f"  Requesting GFAS ({fmt}) for {date_start} → {effective_end} …")
            client.retrieve("cams-global-fire-emissions-gfas", request, target)
            print(f"  ✓ Downloaded → {target}")
            return target
        except Exception as exc:
            msg = str(exc).lower()
            if "format" in msg or "valid" in msg or "not available" in msg:
                print(f"  ⚠ Format '{fmt}' not accepted, trying next …")
                continue
            raise

    print("  ✗ Could not download GFAS data in any supported format.")
    return None


def _print_ecmwf_ftp_instructions():
    print("""
     To get GFAS data for 2026+, you need ECMWF Data Portal FTP access:

     1. Create an account at https://www.ecmwf.int
     2. Accept the CAMS data licence at the ADS dataset page
     3. Request FTP access via ECMWF Support Portal:
        https://jira.ecmwf.int/plugins/servlet/desk/portal/1/create/202
     4. Once approved, GFAS data is at:
        sftp://aux.ecmwf.int/DATA/CAMS_GFAS/

     This script will continue using Layer 1 (FIRMS) estimates only.
""")

# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def _open_dataset(path: str):
    """Open a GFAS file with xarray, trying NetCDF then GRIB backends."""
    import xarray as xr

    # NetCDF
    try:
        return xr.open_dataset(path, engine="netcdf4")
    except Exception:
        pass
    # GRIB
    try:
        return xr.open_dataset(path, engine="cfgrib")
    except Exception:
        pass
    # Auto
    return xr.open_dataset(path)


def _detect_var(ds) -> str:
    """Find the CO₂ fire-flux variable in the dataset."""
    candidates = [
        "co2fire", "wildfire_flux_of_carbon_dioxide",
        "co2_fire", "CO2fire",
    ]
    for name in candidates:
        if name in ds.data_vars:
            return name
    # Fall back to the first (or only) data var
    name = list(ds.data_vars)[0]
    print(f"  Using variable '{name}' (auto-detected)")
    return name


def _detect_dims(ds, var_name: str):
    """Return (time_dim, lat_dim, lon_dim) names."""
    dims = ds[var_name].dims
    time_dim = next((d for d in dims if d in ("time", "valid_time", "step")), dims[0])
    lat_dim  = next((d for d in dims if d in ("latitude", "lat")), dims[1])
    lon_dim  = next((d for d in dims if d in ("longitude", "lon")), dims[2])
    return time_dim, lat_dim, lon_dim


def process_gfas(path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read the GFAS file and compute:
      1. daily_df  — daily regional CO₂ totals (tonnes)
      2. spatial_df — lat/lon/co2_tonnes summed over all days
    """
    ds = _open_dataset(path)
    var = _detect_var(ds)
    time_dim, lat_dim, lon_dim = _detect_dims(ds, var)

    lats = ds[lat_dim].values
    lons = ds[lon_dim].values

    # Infer grid spacing
    if len(lats) > 1:
        dlat = abs(float(lats[1] - lats[0]))
    else:
        dlat = 0.1
    if len(lons) > 1:
        dlon = abs(float(lons[1] - lons[0]))
    else:
        dlon = 0.1

    areas_1d = cell_areas_m2(lats, dlon_deg=dlon, dlat_deg=dlat)  # (nlat,)
    areas_2d = areas_1d[:, np.newaxis] * np.ones(len(lons))[np.newaxis, :]  # (nlat, nlon)

    flux = ds[var]   # kg m⁻² s⁻¹
    times = ds[time_dim].values

    # --- Daily totals ---
    daily_rows = []
    spatial_accum = np.zeros((len(lats), len(lons)))

    for t_idx in range(len(times)):
        grid = flux.isel({time_dim: t_idx}).values  # (nlat, nlon)
        grid = np.nan_to_num(grid, nan=0.0)

        co2_kg_grid = grid * areas_2d * 86400  # kg per cell per day
        co2_tonnes_total = float(np.sum(co2_kg_grid)) / 1000.0
        spatial_accum += co2_kg_grid / 1000.0  # accumulate in tonnes

        date = pd.Timestamp(times[t_idx])
        daily_rows.append({
            "date": date.normalize(),
            "co2_tonnes_gfas": round(co2_tonnes_total, 2),
            "peak_flux_kg_m2_s": float(np.max(grid)),
            "active_cells": int(np.count_nonzero(grid > 0)),
        })

    daily_df = pd.DataFrame(daily_rows)

    # --- Spatial grid (summed over time) ---
    spatial_rows = []
    for i in range(len(lats)):
        for j in range(len(lons)):
            val = spatial_accum[i, j]
            if val > 0:
                spatial_rows.append({
                    "latitude": float(lats[i]),
                    "longitude": float(lons[j]),
                    "co2_tonnes": round(val, 4),
                })

    spatial_df = pd.DataFrame(spatial_rows) if spatial_rows else pd.DataFrame(
        columns=["latitude", "longitude", "co2_tonnes"]
    )

    ds.close()

    print(f"  Processed {len(daily_rows)} day(s), "
          f"{len(spatial_df):,} active grid cells, "
          f"{daily_df['co2_tonnes_gfas'].sum():,.0f} tonnes total CO₂")

    return daily_df, spatial_df

# ---------------------------------------------------------------------------
# FIRMS comparison (reads layer1 CSV output)
# ---------------------------------------------------------------------------

def load_firms_daily(csv_path: str) -> pd.DataFrame | None:
    """Aggregate the layer-1 raw CSV into daily CO₂ totals."""
    if not Path(csv_path).exists():
        return None
    df = pd.read_csv(csv_path)
    if "co2_tonnes" not in df.columns or "acq_date" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["acq_date"]).dt.normalize()
    daily = df.groupby("date").agg(
        co2_tonnes_firms=("co2_tonnes", "sum"),
        co2_tonnes_firms_low=("co2_tonnes_low", "sum") if "co2_tonnes_low" in df.columns else ("co2_tonnes", "sum"),
        co2_tonnes_firms_high=("co2_tonnes_high", "sum") if "co2_tonnes_high" in df.columns else ("co2_tonnes", "sum"),
        detections=("frp", "count"),
        co2_oil_firms=("co2_tonnes", lambda x: 0),  # placeholder, computed below
    ).reset_index()
    # Oil-infrastructure surplus: fires near infrastructure emit more CO₂ than
    # GFAS would assign (GFAS uses vegetation factors).  Estimate the surplus.
    if "near_infra" in df.columns:
        oil = df[df["near_infra"] == True]  # noqa: E712
        if not oil.empty:
            oil_daily = oil.groupby(pd.to_datetime(oil["acq_date"]).dt.normalize()).agg(
                co2_oil_firms=("co2_tonnes", "sum"),
            ).reset_index().rename(columns={"acq_date": "date"})
            daily = daily.drop(columns=["co2_oil_firms"]).merge(
                oil_daily, on="date", how="left"
            )
            daily["co2_oil_firms"] = daily["co2_oil_firms"].fillna(0)
    return daily


def compute_hybrid_co2(gfas_df: pd.DataFrame, firms_df: pd.DataFrame | None) -> pd.DataFrame:
    """
    Hybrid estimate: GFAS (validated, vegetation-optimised) + oil surplus from FIRMS.

    GFAS may undercount oil/petroleum fires because it applies vegetation
    emission factors.  The oil surplus is the extra CO₂ from FIRMS detections
    near oil/gas infrastructure that GFAS likely under-represents.

    hybrid_total = gfas_vegetation + oil_surplus_from_firms
    """
    hybrid = gfas_df[["date", "co2_tonnes_gfas"]].copy()
    hybrid["co2_oil_surplus"] = 0.0
    hybrid["co2_hybrid_total"] = hybrid["co2_tonnes_gfas"]

    if firms_df is not None and "co2_oil_firms" in firms_df.columns:
        merged = hybrid.merge(
            firms_df[["date", "co2_oil_firms"]], on="date", how="left"
        )
        merged["co2_oil_firms"] = merged["co2_oil_firms"].fillna(0)
        merged["co2_oil_surplus"] = merged["co2_oil_firms"]
        merged["co2_hybrid_total"] = merged["co2_tonnes_gfas"] + merged["co2_oil_surplus"]
        return merged[["date", "co2_tonnes_gfas", "co2_oil_surplus", "co2_hybrid_total"]]

    return hybrid

# ---------------------------------------------------------------------------
# Visualizations
# ---------------------------------------------------------------------------

def build_comparison_chart(gfas_df: pd.DataFrame,
                           firms_df: pd.DataFrame | None,
                           hybrid_df: pd.DataFrame | None,
                           region_label: str) -> go.Figure:
    """Bar chart: GFAS validated CO₂, hybrid total, and FIRMS rough estimate."""
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=gfas_df["date"], y=gfas_df["co2_tonnes_gfas"],
        name="GFAS vegetation (validated)", marker_color="#2ecc71", opacity=0.9,
    ))

    if hybrid_df is not None and "co2_oil_surplus" in hybrid_df.columns:
        surplus = hybrid_df[hybrid_df["co2_oil_surplus"] > 0]
        if not surplus.empty:
            fig.add_trace(go.Bar(
                x=surplus["date"], y=surplus["co2_oil_surplus"],
                name="Oil/infra surplus (FIRMS)", marker_color="#f39c12", opacity=0.85,
            ))

    if firms_df is not None and not firms_df.empty:
        merged = gfas_df.merge(firms_df, on="date", how="outer").sort_values("date")
        fig.add_trace(go.Bar(
            x=merged["date"], y=merged["co2_tonnes_firms"],
            name="FIRMS Layer 1 (rough est.)", marker_color="#e74c3c", opacity=0.5,
        ))

    fig.update_layout(
        template="plotly_dark",
        title=dict(
            text=f"Daily Fire CO₂ Emissions — {region_label}<br>"
                 f"<span style='font-size:13px;color:#aaa'>"
                 f"GFAS (validated) + oil surplus from FIRMS = hybrid total</span>",
            font=dict(size=18),
        ),
        xaxis=dict(title="Date", tickformat="%b %d"),
        yaxis=dict(title="CO₂ (tonnes)"),
        barmode="stack",
        bargap=0.15,
        legend=dict(x=0.01, y=0.99, bgcolor="rgba(0,0,0,0.5)"),
        hovermode="x unified",
        height=520,
    )

    return fig


def build_co2_heatmap(spatial_df: pd.DataFrame, region_cfg: dict) -> str:
    """Folium heatmap of spatial CO₂ emissions (returns HTML string)."""
    import folium
    from folium.plugins import HeatMap

    if spatial_df.empty:
        return "<p>No spatial data</p>"

    m = folium.Map(
        location=region_cfg["center"],
        zoom_start=region_cfg["zoom"],
        tiles="CartoDB dark_matter",
    )

    heat_data = spatial_df[["latitude", "longitude", "co2_tonnes"]].values.tolist()
    HeatMap(
        heat_data, name="CO₂ Emissions",
        min_opacity=0.35, radius=12, blur=10, max_zoom=8,
        gradient={0.2: "#ffffb2", 0.4: "#fecc5c", 0.6: "#fd8d3c",
                  0.8: "#f03b20", 1.0: "#bd0026"},
    ).add_to(m)

    title = (
        f'<div style="position:fixed;top:12px;left:50%;transform:translateX(-50%);'
        f'z-index:1000;background:rgba(0,0,0,.85);padding:10px 28px;border-radius:8px;'
        f'font:600 16px/1.4 Segoe UI,sans-serif;color:#fff;text-align:center;'
        f'box-shadow:0 2px 10px rgba(0,0,0,.5)">'
        f'GFAS CO₂ Fire Emissions — {region_cfg["label"]}'
        f'<br><span style="font-size:12px;color:#aaa">'
        f'{spatial_df["co2_tonnes"].sum():,.0f} tonnes total</span></div>'
    )
    m.get_root().html.add_child(folium.Element(title))
    folium.LayerControl(collapsed=False).add_to(m)

    return m._repr_html_()

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def build_report(gfas_df: pd.DataFrame, firms_df: pd.DataFrame | None,
                 hybrid_df: pd.DataFrame | None, region_cfg: dict) -> dict:
    """Compile a JSON-serialisable summary report."""
    total_gfas = round(gfas_df["co2_tonnes_gfas"].sum(), 2)
    peak_day = gfas_df.loc[gfas_df["co2_tonnes_gfas"].idxmax()]

    report: dict = {
        "source": "CAMS GFAS v1.4 (Copernicus/ECMWF)",
        "methodology": {
            "gfas": (
                "FRP assimilation (MODIS+VIIRS) → Kalman-filter FRE integration "
                "→ land-cover-specific emission factors → gridded daily CO₂ flux "
                "at 0.1° resolution. Peer-reviewed, used in IPCC reporting."
            ),
            "hybrid_approach": (
                "GFAS is optimised for vegetation fires and may undercount "
                "oil/petroleum infrastructure fires. The hybrid total adds a "
                "'oil surplus' from FIRMS Layer 1 detections near known oil/gas "
                "infrastructure, using petroleum-specific emission factors "
                "(radiative fraction χ=0.045, ΔH_c=44 MJ/kg, EF=3.12 kg CO₂/kg)."
            ),
            "references": [
                "Kaiser et al. 2012 (GFAS methodology)",
                "Wooster et al. 2005 (FRP-to-biomass conversion)",
                "Andreae 2019 (emission factors)",
                "CCI/Neimark 2026 (Scope 3+ conflict emissions framework)",
            ],
        },
        "region": region_cfg["label"],
        "date_range": {
            "start": str(gfas_df["date"].min().date()),
            "end": str(gfas_df["date"].max().date()),
            "days": len(gfas_df),
        },
        "co2_gfas_vegetation_tonnes": total_gfas,
        "co2_daily_avg_tonnes": round(total_gfas / max(len(gfas_df), 1), 2),
        "peak_day": {
            "date": str(peak_day["date"].date()),
            "co2_tonnes": round(float(peak_day["co2_tonnes_gfas"]), 2),
            "active_grid_cells": int(peak_day["active_cells"]),
        },
    }

    if hybrid_df is not None and "co2_hybrid_total" in hybrid_df.columns:
        total_hybrid = round(hybrid_df["co2_hybrid_total"].sum(), 2)
        oil_surplus = round(hybrid_df["co2_oil_surplus"].sum(), 2)
        report["co2_hybrid_total_tonnes"] = total_hybrid
        report["co2_oil_surplus_tonnes"] = oil_surplus
        report["hybrid_note"] = (
            f"Hybrid = GFAS vegetation ({total_gfas:,.0f} t) "
            f"+ oil infrastructure surplus ({oil_surplus:,.0f} t) "
            f"= {total_hybrid:,.0f} t"
        )

    if firms_df is not None and not firms_df.empty:
        total_firms = round(firms_df["co2_tonnes_firms"].sum(), 2)
        ratio = round(total_gfas / total_firms, 2) if total_firms > 0 else None
        report["firms_comparison"] = {
            "firms_total_tonnes": total_firms,
            "gfas_total_tonnes": total_gfas,
            "gfas_to_firms_ratio": ratio,
            "interpretation": (
                f"GFAS estimates are {ratio}× the FIRMS rough estimate. "
                "Differences arise from temporal integration, land-cover-aware "
                "emission factors, and combustion completeness modelling."
                if ratio else "Cannot compare (FIRMS total is zero)."
            ),
        }

    report["daily_breakdown"] = [
        {
            "date": str(row["date"].date()),
            "co2_tonnes_gfas": row["co2_tonnes_gfas"],
            "active_cells": int(row["active_cells"]),
        }
        for _, row in gfas_df.iterrows()
    ]

    return report


def print_report(r: dict) -> None:
    label = r["region"]
    w = r["date_range"]
    print("\n" + "=" * 68)
    print(f"  GFAS CO₂ FIRE EMISSION REPORT — {label.upper()}")
    print("=" * 68)
    print(f"  Source           : {r['source']}")
    print(f"  Date range       : {w['start']} → {w['end']}  ({w['days']} days)")
    print(f"  GFAS vegetation  : {r['co2_gfas_vegetation_tonnes']:,.0f} tonnes")
    if "co2_oil_surplus_tonnes" in r:
        print(f"  Oil surplus      : {r['co2_oil_surplus_tonnes']:,.0f} tonnes")
    if "co2_hybrid_total_tonnes" in r:
        print(f"  HYBRID TOTAL     : {r['co2_hybrid_total_tonnes']:,.0f} tonnes  ← best estimate")
    print(f"  Daily average    : {r['co2_daily_avg_tonnes']:,.0f} tonnes/day (GFAS only)")
    p = r["peak_day"]
    print(f"  Peak day         : {p['date']}  ({p['co2_tonnes']:,.0f} t, "
          f"{p['active_grid_cells']:,} active cells)")

    if "firms_comparison" in r:
        c = r["firms_comparison"]
        print(f"\n  --- FIRMS Layer 1 Comparison ---")
        print(f"  FIRMS rough est. : {c['firms_total_tonnes']:,.0f} tonnes")
        print(f"  GFAS validated   : {c['gfas_total_tonnes']:,.0f} tonnes")
        if c["gfas_to_firms_ratio"]:
            print(f"  Ratio (GFAS/FIRMS): {c['gfas_to_firms_ratio']}×")

    print("\n  --- Daily Breakdown (GFAS) ---")
    for d in r["daily_breakdown"]:
        print(f"    {d['date']}  {d['co2_tonnes_gfas']:>12,.0f} t  "
              f"({d['active_cells']:>6,} cells)")
    print("=" * 68 + "\n")

# ---------------------------------------------------------------------------
# FIRMS-only fallback (when GFAS data is unavailable)
# ---------------------------------------------------------------------------

def _build_firms_only_chart(firms_df: pd.DataFrame, region_label: str) -> go.Figure:
    """Bar chart from Layer 1 data when GFAS is not available."""
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=firms_df["date"], y=firms_df["co2_tonnes_firms"],
        name="FIRMS CO₂ (mid est.)", marker_color="#e74c3c", opacity=0.85,
    ))

    if "co2_tonnes_firms_low" in firms_df.columns:
        fig.add_trace(go.Scatter(
            x=pd.concat([firms_df["date"], firms_df["date"][::-1]]),
            y=pd.concat([firms_df["co2_tonnes_firms_high"],
                         firms_df["co2_tonnes_firms_low"][::-1]]),
            fill="toself", fillcolor="rgba(231,76,60,0.15)", line=dict(width=0),
            name="Uncertainty range", showlegend=True, hoverinfo="skip",
        ))

    if "co2_oil_firms" in firms_df.columns:
        oil = firms_df[firms_df["co2_oil_firms"] > 0]
        if not oil.empty:
            fig.add_trace(go.Bar(
                x=oil["date"], y=oil["co2_oil_firms"],
                name="Oil/infra fires", marker_color="#f39c12", opacity=0.8,
            ))

    fig.update_layout(
        template="plotly_dark",
        title=dict(
            text=f"Daily Fire CO₂ Emissions — {region_label}<br>"
                 f"<span style='font-size:13px;color:#f39c12'>"
                 f"FIRMS Layer 1 only (GFAS unavailable for this period)</span>",
            font=dict(size=18),
        ),
        xaxis=dict(title="Date", tickformat="%b %d"),
        yaxis=dict(title="CO₂ (tonnes)"),
        barmode="overlay",
        bargap=0.15,
        legend=dict(x=0.01, y=0.99, bgcolor="rgba(0,0,0,0.5)"),
        hovermode="x unified",
        height=520,
    )
    return fig


def _build_firms_only_report(firms_df: pd.DataFrame | None, region_cfg: dict,
                             date_start: str, date_end: str) -> dict:
    """Summary report when GFAS is unavailable."""
    report: dict = {
        "source": "FIRMS Layer 1 only (GFAS unavailable for 2026 on public ADS)",
        "gfas_status": (
            "GFAS v1.2 on the Copernicus ADS was discontinued on 2025-12-03. "
            "The new GFAS v1.4.2 (VIIRS) data is only available via ECMWF FTP "
            "(requires separate application). This report uses Layer 1 FIRMS "
            "FRP-based estimates with uncertainty bounds."
        ),
        "methodology": {
            "firms": (
                "FRP (MW) × observation_window (6h) × β → dry matter → "
                "× emission_factor → CO₂. β=0.368 (Wooster 2005) for vegetation, "
                "radiative-fraction pathway (χ=0.045) for oil fires."
            ),
            "uncertainty": "±3–5× for individual detections; aggregated uncertainty ~±50%.",
            "references": [
                "Wooster et al. 2005 (FRP-to-biomass conversion)",
                "Andreae 2019 (emission factors)",
                "Kaiser et al. 2012 (GFAS methodology reference)",
            ],
        },
        "region": region_cfg["label"],
        "date_range": {
            "start": date_start,
            "end": date_end,
        },
    }

    if firms_df is not None and not firms_df.empty:
        total_mid = round(firms_df["co2_tonnes_firms"].sum(), 2)
        total_low = round(firms_df.get("co2_tonnes_firms_low", firms_df["co2_tonnes_firms"]).sum(), 2)
        total_high = round(firms_df.get("co2_tonnes_firms_high", firms_df["co2_tonnes_firms"]).sum(), 2)
        total_oil = round(firms_df.get("co2_oil_firms", pd.Series([0])).sum(), 2)
        n_days = len(firms_df)
        peak_row = firms_df.loc[firms_df["co2_tonnes_firms"].idxmax()]

        report["date_range"]["days"] = n_days
        report["co2_firms_mid_tonnes"] = total_mid
        report["co2_firms_low_tonnes"] = total_low
        report["co2_firms_high_tonnes"] = total_high
        report["co2_oil_infra_tonnes"] = total_oil
        report["co2_daily_avg_tonnes"] = round(total_mid / max(n_days, 1), 2)
        report["total_detections"] = int(firms_df["detections"].sum())
        report["peak_day"] = {
            "date": str(peak_row["date"].date()) if hasattr(peak_row["date"], "date") else str(peak_row["date"]),
            "co2_tonnes": round(float(peak_row["co2_tonnes_firms"]), 2),
            "detections": int(peak_row["detections"]),
        }
        report["daily_breakdown"] = [
            {
                "date": str(row["date"].date()) if hasattr(row["date"], "date") else str(row["date"]),
                "co2_tonnes_mid": round(row["co2_tonnes_firms"], 1),
                "detections": int(row["detections"]),
            }
            for _, row in firms_df.iterrows()
        ]
    else:
        report["error"] = "No FIRMS data available."

    return report


def _print_firms_only_report(r: dict) -> None:
    label = r["region"]
    w = r["date_range"]
    print("\n" + "=" * 68)
    print(f"  FIRE CO₂ EMISSION REPORT — {label.upper()}")
    print(f"  (FIRMS Layer 1 only — GFAS unavailable)")
    print("=" * 68)
    print(f"  Period           : {w['start']} → {w['end']}  ({w.get('days', '?')} days)")

    if "co2_firms_mid_tonnes" in r:
        print(f"  CO₂ mid estimate : {r['co2_firms_mid_tonnes']:,.0f} tonnes")
        print(f"  CO₂ range        : {r['co2_firms_low_tonnes']:,.0f}"
              f" – {r['co2_firms_high_tonnes']:,.0f} tonnes")
        if r.get("co2_oil_infra_tonnes", 0) > 0:
            print(f"  Oil/infra fires  : {r['co2_oil_infra_tonnes']:,.0f} tonnes")
        print(f"  Daily average    : {r['co2_daily_avg_tonnes']:,.0f} tonnes/day")
        print(f"  Total detections : {r['total_detections']:,}")
        p = r["peak_day"]
        print(f"  Peak day         : {p['date']}  ({p['co2_tonnes']:,.0f} t, "
              f"{p['detections']:,} detections)")

        print(f"\n  --- Daily Breakdown ---")
        for d in r.get("daily_breakdown", []):
            print(f"    {d['date']}  {d['co2_tonnes_mid']:>12,.0f} t  "
                  f"({d['detections']:>6,} fires)")

    print(f"\n  ⚠  These are FIRMS FRP-based rough estimates (±3–5×).")
    print(f"     For validated numbers, request ECMWF FTP access to GFAS v1.4.2.")
    print("=" * 68 + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Layer 2 — Fetch CAMS GFAS validated CO₂ fire emissions.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument(
        "-r", "--region",
        choices=list(REGIONS.keys()), default="world",
        help="Region preset (default: world).\nAvailable: " + ", ".join(REGIONS.keys()),
    )
    p.add_argument(
        "-d", "--days", type=int, default=None,
        help="Days of data to fetch (default varies by region).\n"
             "Note: GFAS NRT has a ~2-day lag; the most recent 1-2 days\n"
             "may not be available yet.",
    )
    p.add_argument(
        "--start-date", default=None,
        help="Explicit start date (YYYY-MM-DD). Overrides --days.",
    )
    p.add_argument(
        "--end-date", default=None,
        help="Explicit end date (YYYY-MM-DD). Defaults to 3 days ago.",
    )
    p.add_argument(
        "--firms-csv", default="output/firms_raw_data.csv",
        help="Path to Layer 1 FIRMS CSV for comparison (default: output/firms_raw_data.csv).",
    )
    p.add_argument(
        "-o", "--output-dir", default="output",
        help="Output directory (default: output).",
    )
    return p.parse_args()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    region_cfg = REGIONS[args.region]
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # --- Date range ---
    today = datetime.now(timezone.utc).date()
    gfas_lag = timedelta(days=3)  # NRT data is ~2-3 days behind

    if args.start_date and args.end_date:
        date_start = args.start_date
        date_end = args.end_date
    elif args.start_date:
        date_start = args.start_date
        date_end = str(today - gfas_lag)
    else:
        total_days = args.days if args.days else region_cfg["default_days"]
        end = today - gfas_lag
        start = end - timedelta(days=total_days - 1)
        date_start = str(start)
        date_end = str(end)

    print(f"Region : {region_cfg['label']}")
    print(f"Period : {date_start} → {date_end}")
    if region_cfg["area"]:
        a = region_cfg["area"]
        print(f"Area   : N={a[0]}, W={a[1]}, S={a[2]}, E={a[3]}")
    else:
        print(f"Area   : Global")

    # --- Check for API key ---
    key = os.getenv("ADS_API_KEY", "").strip()
    cdsapirc = Path.home() / ".cdsapirc"
    if not key and not cdsapirc.exists():
        print("\n" + "=" * 64)
        print("  SETUP REQUIRED — Copernicus Atmosphere Data Store")
        print("=" * 64)
        print("""
  GFAS provides the gold-standard fire CO₂ estimates used in
  climate science. Access is FREE but requires a one-time setup:

  1. Register at:
     https://ads.atmosphere.copernicus.eu/

  2. Log in and accept the GFAS dataset terms at:
     https://ads.atmosphere.copernicus.eu/datasets/cams-global-fire-emissions-gfas

  3. Copy your Personal Access Token from:
     https://ads.atmosphere.copernicus.eu/profile

  4. Paste it in your .env file:
     ADS_API_KEY=<your-token>

     OR create ~/.cdsapirc with:
       url: https://ads.atmosphere.copernicus.eu/api
       key: <your-token>

  Then re-run this script.
""")
        print("=" * 64)
        sys.exit(1)

    # --- 1. Fetch GFAS data ---
    print("\n[1/4] Fetching GFAS CO₂ fire emissions …")
    gfas_file = str(out / "gfas_co2_raw")
    gfas_path = fetch_gfas(region_cfg["area"], date_start, date_end, gfas_file)

    daily_df = pd.DataFrame()
    spatial_df = pd.DataFrame()

    if gfas_path is not None:
        # --- 2. Process GFAS ---
        print("\n[2/4] Processing gridded emissions …")
        daily_df, spatial_df = process_gfas(gfas_path)
        if not daily_df.empty:
            daily_csv = out / "gfas_co2_daily.csv"
            daily_df.to_csv(daily_csv, index=False)
            print(f"  ✓ Daily CSV → {daily_csv}")

            spatial_csv = out / "gfas_co2_spatial.csv"
            spatial_df.to_csv(spatial_csv, index=False)
            print(f"  ✓ Spatial CSV ({len(spatial_df):,} cells) → {spatial_csv}")
        else:
            print("  ⚠ GFAS returned no data for this region/period.")
    else:
        print("\n[2/4] GFAS unavailable — will use FIRMS data only.")

    gfas_available = not daily_df.empty

    # --- 3. Load FIRMS comparison + compute hybrid ---
    firms_df = load_firms_daily(args.firms_csv)
    hybrid_df = None

    if firms_df is not None:
        print(f"\n  Loaded FIRMS Layer 1 data from {args.firms_csv}")
        if gfas_available:
            overlapping = daily_df.merge(firms_df, on="date", how="inner")
            print(f"  Overlapping dates: {len(overlapping)}")
            hybrid_df = compute_hybrid_co2(daily_df, firms_df)
            hybrid_csv = out / "gfas_co2_hybrid.csv"
            hybrid_df.to_csv(hybrid_csv, index=False)
            print(f"  ✓ Hybrid CO₂ CSV → {hybrid_csv}")
    else:
        print(f"\n  No FIRMS CSV found at {args.firms_csv}")
        print(f"  Run Layer 1 first:  python3 layer1_firms_fire.py --region {args.region}")
        if not gfas_available:
            sys.exit("No GFAS data and no FIRMS data — nothing to report.")

    # --- 4. Visualizations & report ---
    print("\n[3/4] Building visualizations …")

    if gfas_available:
        fig = build_comparison_chart(daily_df, firms_df, hybrid_df, region_cfg["label"])
        chart_path = out / "gfas_co2_timeseries.html"
        fig.write_html(str(chart_path), include_plotlyjs="cdn")
        print(f"  ✓ Time series → {chart_path}")

        if not spatial_df.empty:
            heatmap_html = build_co2_heatmap(spatial_df, region_cfg)
            hm_path = out / "gfas_co2_heatmap.html"
            hm_path.write_text(heatmap_html, encoding="utf-8")
            print(f"  ✓ CO₂ heatmap → {hm_path}")
    elif firms_df is not None:
        fig = _build_firms_only_chart(firms_df, region_cfg["label"])
        chart_path = out / "layer2_co2_timeseries.html"
        fig.write_html(str(chart_path), include_plotlyjs="cdn")
        print(f"  ✓ FIRMS-only time series → {chart_path}")

    print("\n[4/4] Generating report …")
    if gfas_available:
        report = build_report(daily_df, firms_df, hybrid_df, region_cfg)
    else:
        report = _build_firms_only_report(firms_df, region_cfg, date_start, date_end)
    report_path = out / "layer2_co2_report.json"
    report_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"  ✓ Report JSON → {report_path}")

    if gfas_available:
        print_report(report)
    else:
        _print_firms_only_report(report)


if __name__ == "__main__":
    main()
