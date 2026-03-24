#!/usr/bin/env python3
"""
Layer 1 — NASA FIRMS VIIRS Global Fire Detection Visualizer
Fetches near-real-time fire data for any region (or the entire world),
estimates CO₂ emissions, and produces an interactive 2D map, 3D map
(Google Photorealistic Tiles), time-series chart, CSV export, and JSON summary.
"""

import argparse
import functools
import http.server
import io
import json
import math
import os
import signal
import socket
import sys
import threading
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path

import folium
import pandas as pd
import plotly.graph_objects as go
import requests
from dotenv import load_dotenv
from folium.plugins import HeatMap, MarkerCluster

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

MAP_KEY = os.getenv("MAP_KEY")
if not MAP_KEY:
    sys.exit("ERROR: MAP_KEY not found. Create a .env file with MAP_KEY=<your key>")

GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")  # optional, for photorealistic 3D tiles

FIRMS_BASE = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
MAX_DAYS_PER_REQUEST = 5  # FIRMS area API maximum is 5

SOURCES_ALL = {
    "viirs_noaa20": "VIIRS_NOAA20_NRT",
    "viirs_noaa21": "VIIRS_NOAA21_NRT",
    "viirs_snpp":   "VIIRS_SNPP_NRT",
    "modis":        "MODIS_NRT",
}

NRT_TO_SP = {
    "VIIRS_NOAA20_NRT": "VIIRS_NOAA20_SP",
    "VIIRS_NOAA21_NRT": "VIIRS_NOAA21_SP",
    "VIIRS_SNPP_NRT":   "VIIRS_SNPP_SP",
    "MODIS_NRT":        "MODIS_SP",
}

# ---------------------------------------------------------------------------
# Region presets
# ---------------------------------------------------------------------------

REGIONS = {
    "world": {
        "bbox": "world",
        "center": [20, 0], "zoom": 2,
        "cam_lon": 25, "cam_lat": 10, "cam_alt": 6_000_000,
        "default_days": 2,
        "label": "World",
    },
    "iran": {
        "bbox": "44,25,63.5,40",
        "center": [32, 53], "zoom": 6,
        "cam_lon": 53, "cam_lat": 28, "cam_alt": 1_800_000,
        "default_days": 10,
        "label": "Iran",
    },
    "middle_east": {
        "bbox": "25,12,65,42",
        "center": [28, 45], "zoom": 5,
        "cam_lon": 45, "cam_lat": 24, "cam_alt": 3_000_000,
        "default_days": 5,
        "label": "Middle East",
    },
    "north_america": {
        "bbox": "-170,10,-50,75",
        "center": [45, -100], "zoom": 3,
        "cam_lon": -100, "cam_lat": 38, "cam_alt": 8_000_000,
        "default_days": 3,
        "label": "North America",
    },
    "south_america": {
        "bbox": "-85,-57,-32,14",
        "center": [-15, -60], "zoom": 3,
        "cam_lon": -60, "cam_lat": -20, "cam_alt": 8_000_000,
        "default_days": 3,
        "label": "South America",
    },
    "europe": {
        "bbox": "-15,35,45,72",
        "center": [50, 15], "zoom": 4,
        "cam_lon": 15, "cam_lat": 45, "cam_alt": 4_000_000,
        "default_days": 3,
        "label": "Europe",
    },
    "africa": {
        "bbox": "-20,-37,55,40",
        "center": [5, 20], "zoom": 3,
        "cam_lon": 20, "cam_lat": 0, "cam_alt": 8_000_000,
        "default_days": 3,
        "label": "Africa",
    },
    "south_asia": {
        "bbox": "60,5,100,40",
        "center": [22, 80], "zoom": 4,
        "cam_lon": 80, "cam_lat": 18, "cam_alt": 4_000_000,
        "default_days": 5,
        "label": "South Asia",
    },
    "southeast_asia": {
        "bbox": "90,-15,155,30",
        "center": [10, 120], "zoom": 4,
        "cam_lon": 120, "cam_lat": 5, "cam_alt": 5_000_000,
        "default_days": 3,
        "label": "Southeast Asia",
    },
    "australia": {
        "bbox": "110,-50,180,-5",
        "center": [-25, 135], "zoom": 4,
        "cam_lon": 135, "cam_lat": -30, "cam_alt": 5_000_000,
        "default_days": 3,
        "label": "Australia / Oceania",
    },
    "usa_west": {
        "bbox": "-130,30,-100,50",
        "center": [40, -115], "zoom": 5,
        "cam_lon": -115, "cam_lat": 36, "cam_alt": 2_500_000,
        "default_days": 5,
        "label": "USA West Coast",
    },
    "amazon": {
        "bbox": "-75,-20,-45,5",
        "center": [-8, -60], "zoom": 5,
        "cam_lon": -60, "cam_lat": -10, "cam_alt": 3_000_000,
        "default_days": 5,
        "label": "Amazon Basin",
    },
}

# ---------------------------------------------------------------------------
# Iran-specific overlays (only shown when region is iran / middle_east)
# ---------------------------------------------------------------------------

INFRASTRUCTURE = {
    "Tehran Oil Refineries": {"lat": 35.6892, "lon": 51.3890, "type": "oil"},
    "South Pars Gas Field":  {"lat": 27.5000, "lon": 52.0000, "type": "gas"},
    "Kharg Island Terminal": {"lat": 29.2333, "lon": 50.3167, "type": "oil"},
    "Isfahan Nuclear Site":  {"lat": 32.6333, "lon": 51.6667, "type": "nuclear"},
}

STRIKE_ANNOTATIONS = [
    {"date": "2025-02-28", "label": "Operation Epic Fury begins"},
    {"date": "2025-03-07", "label": "Tehran oil depot strikes"},
    {"date": "2025-03-08", "label": "Tehran oil depot strikes (day 2)"},
    {"date": "2025-03-18", "label": "South Pars gas field attack"},
]

INFRA_CO2_RADIUS_KM = 5
SUMMARY_RADIUS_KM = 10

# ---------------------------------------------------------------------------
# CO₂ estimation methodology (aligned with Wooster et al. 2005, Kaiser et al.
# 2012, and CCI/Neimark Scope 3+ framework).
#
#   Pathway:  FRP (MW) × Δt (s) → FRE (MJ) → × β (kg DM/MJ) → DM (kg)
#             → × EF (kg CO₂/kg DM) → CO₂ (kg) → ÷ 1000 → CO₂ (tonnes)
#
# VEGETATION fires:
#   β  = 0.368 kg DM per MJ  (Wooster et al. 2005)
#   EF = 1.64  kg CO₂ per kg DM (Andreae 2019, tropical/savanna average)
#   Δt = 21600 s (6-hour observation window per satellite pass)
#
# OIL / PETROLEUM fires (infrastructure strikes):
#   Uses radiative-fraction pathway instead of β:
#     fuel_rate = FRP / (χ × ΔH_c)
#     χ  ≈ 0.045  radiative fraction for large pool fires (Koseki 1989,
#                  Mudan 1984; large smoke-producing oil fires radiate less)
#     ΔH_c = 44 MJ/kg  heat of combustion of crude oil
#     EF   = 3.12 kg CO₂/kg oil  (carbon fraction 0.85 × 44/12)
#   β_oil_equiv = 1/(χ × ΔH_c) × EF = 1/(0.045×44) × 3.12 ≈ 1.576
#
# Uncertainty: FIRMS-based estimates are inherently rough (±factor of 3–5)
# because each detection is an instantaneous snapshot extrapolated over a
# 6-hour window.  GFAS (Layer 2) provides validated daily totals with
# Kalman-filter temporal integration and land-cover-aware factors.
#
# β ranges for uncertainty (Wooster 2005 / Kaiser 2012):
#   LOW  = 0.230  (lower bound, Wooster)
#   MID  = 0.368  (central, Wooster)
#   HIGH = 0.550  (empirical upper, accounts for partial observation gaps)
# ---------------------------------------------------------------------------

OBSERVATION_WINDOW_S = 21600  # 6 hours per satellite overpass window

BETA_VEG_LOW  = 0.230
BETA_VEG_MID  = 0.368   # Wooster et al. 2005
BETA_VEG_HIGH = 0.550

EF_VEG = 1.64  # kg CO₂ per kg dry matter (Andreae 2019, average)

CHI_OIL = 0.045        # radiative fraction, large oil pool fires
DH_C_OIL = 44.0        # MJ/kg, heat of combustion of crude oil
EF_OIL = 3.12          # kg CO₂ per kg crude oil (C frac 0.85 × 44/12)
BETA_OIL_EQUIV = EF_OIL / (CHI_OIL * DH_C_OIL)  # ≈ 1.576

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def near_infrastructure(lat: float, lon: float, radius_km: float) -> bool:
    oil_gas = [v for v in INFRASTRUCTURE.values() if v["type"] in ("oil", "gas")]
    return any(
        haversine_km(lat, lon, site["lat"], site["lon"]) <= radius_km
        for site in oil_gas
    )


def region_contains_iran(region_key: str) -> bool:
    return region_key in ("iran", "middle_east", "world")


def frp_color(frp: float) -> str:
    if frp > 100:
        return "#e74c3c"
    if frp > 10:
        return "#f39c12"
    return "#f1c40f"


def frp_radius(frp: float) -> float:
    return max(3, min(15, 3 + math.sqrt(frp) * 0.8))

# ---------------------------------------------------------------------------
# Data fetching with exponential backoff
# ---------------------------------------------------------------------------

def _fetch_one(source: str, bbox: str, day_range: int,
               date_str: str | None = None, retries: int = 4) -> pd.DataFrame:
    url = f"{FIRMS_BASE}/{MAP_KEY}/{source}/{bbox}/{day_range}"
    if date_str:
        url += f"/{date_str}"

    last_status = None
    for attempt in range(retries):
        try:
            print(f"  GET {url}")
            resp = requests.get(url, timeout=120)
            last_status = resp.status_code
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            print(f"      ↳ {len(df):,} rows")
            return df
        except requests.RequestException as exc:
            wait = 2 ** attempt
            print(f"      ⚠ attempt {attempt + 1}/{retries} failed: {exc}")
            if last_status == 400 and attempt == 0:
                break
            if attempt < retries - 1:
                print(f"        retrying in {wait}s …")
                time.sleep(wait)

    sp_source = NRT_TO_SP.get(source)
    if last_status == 400 and sp_source:
        print(f"      → NRT unavailable for this date range, trying SP archive ({sp_source}) …")
        return _fetch_one(sp_source, bbox, day_range, date_str, retries)

    print(f"      ✗ all attempts failed for {source}")
    return pd.DataFrame()


def fetch_fire_data(bbox: str, total_days: int,
                    sources: list[str],
                    start_date: datetime | None = None) -> pd.DataFrame:
    today = datetime.now(timezone.utc).date()
    frames: list[pd.DataFrame] = []

    if start_date is None:
        window_start = today - timedelta(days=total_days - 1)
    else:
        window_start = start_date

    for source in sources:
        print(f"\n[{source}]")
        remaining = total_days
        ws = window_start

        while remaining > 0:
            chunk = min(remaining, MAX_DAYS_PER_REQUEST)
            date_str = ws.strftime("%Y-%m-%d")
            df = _fetch_one(source, bbox, chunk, date_str)
            if not df.empty:
                df["source"] = source
                frames.append(df)
            ws += timedelta(days=chunk)
            remaining -= chunk

    if not frames:
        sys.exit("No data retrieved from any source. Check MAP_KEY or network.")

    combined = pd.concat(frames, ignore_index=True)
    combined.drop_duplicates(
        subset=["latitude", "longitude", "acq_date", "acq_time"], keep="first", inplace=True
    )
    print(f"\nCombined: {len(combined):,} unique detections across {len(sources)} sources")
    return combined

# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def process(df: pd.DataFrame, check_infra: bool) -> pd.DataFrame:
    df = df.copy()
    df = df.dropna(subset=["frp"])
    df = df[df["frp"] > 0]
    df = df[df["confidence"].isin(["h", "n"])]

    if check_infra:
        df["near_infra"] = df.apply(
            lambda r: near_infrastructure(r["latitude"], r["longitude"], INFRA_CO2_RADIUS_KM),
            axis=1,
        )
    else:
        df["near_infra"] = False

    t = OBSERVATION_WINDOW_S

    # Mid estimate (central β)
    df["co2_tonnes"] = df.apply(
        lambda r: r["frp"] * t * BETA_OIL_EQUIV / 1e3
        if r["near_infra"]
        else r["frp"] * t * BETA_VEG_MID * EF_VEG / 1e3,
        axis=1,
    )

    # Low / high bounds (vegetation uncertainty; oil held constant)
    df["co2_tonnes_low"] = df.apply(
        lambda r: r["frp"] * t * BETA_OIL_EQUIV / 1e3
        if r["near_infra"]
        else r["frp"] * t * BETA_VEG_LOW * EF_VEG / 1e3,
        axis=1,
    )
    df["co2_tonnes_high"] = df.apply(
        lambda r: r["frp"] * t * BETA_OIL_EQUIV / 1e3
        if r["near_infra"]
        else r["frp"] * t * BETA_VEG_HIGH * EF_VEG / 1e3,
        axis=1,
    )

    df["acq_date"] = pd.to_datetime(df["acq_date"])
    df["acq_time_str"] = df["acq_time"].astype(str).str.zfill(4)
    df["acq_hour"] = df["acq_time_str"].str[:2] + ":" + df["acq_time_str"].str[2:]

    print(f"After filtering (confidence h/n, frp>0): {len(df):,} detections")
    return df

# ---------------------------------------------------------------------------
# 2D Map (Folium)
# ---------------------------------------------------------------------------

def build_map(df: pd.DataFrame, region_cfg: dict,
              show_infra: bool) -> folium.Map:
    m = folium.Map(
        location=region_cfg["center"],
        zoom_start=region_cfg["zoom"],
        tiles="CartoDB dark_matter",
        control_scale=True,
    )
    folium.TileLayer("CartoDB positron", name="Light basemap").add_to(m)

    use_cluster = len(df) > 2000

    if use_cluster:
        fire_layer = MarkerCluster(name="Fire Detections")
    else:
        fire_layer = folium.FeatureGroup(name="Fire Detections", show=True)

    for _, r in df.iterrows():
        popup = (
            f"<div style='font-family:monospace;font-size:12px;min-width:200px'>"
            f"<b>VIIRS Fire Detection</b><br>"
            f"<b>Date:</b> {r['acq_date'].strftime('%Y-%m-%d')} {r['acq_hour']} UTC<br>"
            f"<b>FRP:</b> {r['frp']:.1f} MW<br>"
            f"<b>Confidence:</b> {r['confidence']}<br>"
            f"<b>CO₂ est:</b> {r['co2_tonnes']:,.1f} t "
            f"<span style='color:#888'>({r['co2_tonnes_low']:,.1f}–{r['co2_tonnes_high']:,.1f})</span>"
            f"</div>"
        )
        folium.CircleMarker(
            location=[r["latitude"], r["longitude"]],
            radius=frp_radius(r["frp"]),
            color=frp_color(r["frp"]),
            fill=True,
            fill_color=frp_color(r["frp"]),
            fill_opacity=0.75,
            weight=1,
            popup=folium.Popup(popup, max_width=300),
        ).add_to(fire_layer)
    fire_layer.add_to(m)

    heat_data = df[["latitude", "longitude", "frp"]].values.tolist()
    HeatMap(
        heat_data,
        name="FRP Heatmap",
        min_opacity=0.3,
        radius=14 if len(df) < 5000 else 8,
        blur=12 if len(df) < 5000 else 6,
        gradient={0.2: "#ffffb2", 0.4: "#fecc5c", 0.6: "#fd8d3c", 0.8: "#f03b20", 1.0: "#bd0026"},
        show=False,
    ).add_to(m)

    if show_infra:
        infra_group = folium.FeatureGroup(name="Infrastructure Targets", show=True)
        for name, info in INFRASTRUCTURE.items():
            icon_html = (
                f"<div style='font-size:22px; text-shadow:0 0 6px rgba(0,0,0,0.8);'>"
                f"{'⭐' if info['type'] == 'nuclear' else '🔴'}</div>"
            )
            folium.Marker(
                location=[info["lat"], info["lon"]],
                popup=folium.Popup(
                    f"<b>{name}</b><br>Type: {info['type']}<br>"
                    f"Coords: {info['lat']:.4f}, {info['lon']:.4f}",
                    max_width=250,
                ),
                icon=folium.DivIcon(html=icon_html, icon_size=(28, 28), icon_anchor=(14, 14)),
            ).add_to(infra_group)
        infra_group.add_to(m)

    legend_html = """
    <div style="
        position:fixed; bottom:30px; left:30px; z-index:1000;
        background:rgba(0,0,0,0.82); padding:14px 18px; border-radius:8px;
        font-family:'Segoe UI',sans-serif; font-size:13px; color:#fff;
        box-shadow:0 2px 10px rgba(0,0,0,0.5); line-height:1.9;">
        <b style="font-size:14px;">FRP Intensity</b><br>
        <span style="color:#f1c40f;">●</span> &lt; 10 MW &nbsp;
        <span style="color:#f39c12;">●</span> 10–100 MW &nbsp;
        <span style="color:#e74c3c;">●</span> &gt; 100 MW
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    label = region_cfg["label"]
    title_html = f"""
    <div style="
        position:fixed; top:12px; left:50%; transform:translateX(-50%); z-index:1000;
        background:rgba(0,0,0,0.85); padding:10px 28px; border-radius:8px;
        font-family:'Segoe UI',sans-serif; font-size:16px; color:#fff;
        text-align:center; box-shadow:0 2px 10px rgba(0,0,0,0.5);">
        FIRMS VIIRS — <b>{len(df):,}</b> fire detections — {label}
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    folium.LayerControl(collapsed=False).add_to(m)
    return m

# ---------------------------------------------------------------------------
# 3D Map — CesiumJS + OpenStreetMap (free, no API key)
# ---------------------------------------------------------------------------

_CESIUM_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FIRMS VIIRS 3D Fire Map — %%REGION_LABEL%%</title>
<script src="https://cesium.com/downloads/cesiumjs/releases/1.124/Build/Cesium/Cesium.js"></script>
<link href="https://cesium.com/downloads/cesiumjs/releases/1.124/Build/Cesium/Widgets/widgets.css" rel="stylesheet">
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  html,body,#cesiumContainer{width:100%;height:100%;overflow:hidden;background:#0a0a0a}
  #hud{position:fixed;top:14px;left:50%;transform:translateX(-50%);z-index:10;
       background:rgba(0,0,0,.82);padding:10px 28px;border-radius:8px;
       font:600 15px/1.4 'Segoe UI',system-ui,sans-serif;color:#fff;
       text-align:center;box-shadow:0 4px 20px rgba(0,0,0,.6);
       backdrop-filter:blur(6px);pointer-events:none}
  #hud b{color:#e74c3c}
  #legend{position:fixed;bottom:30px;left:20px;z-index:10;
          background:rgba(0,0,0,.78);padding:14px 18px;border-radius:8px;
          font:13px/1.9 'Segoe UI',system-ui,sans-serif;color:#fff;
          box-shadow:0 2px 12px rgba(0,0,0,.5);backdrop-filter:blur(6px)}
  #legend b{font-size:14px}
  #controls{position:fixed;top:14px;right:14px;z-index:10;display:flex;flex-direction:column;gap:6px}
  #controls button{background:rgba(0,0,0,.75);color:#fff;border:1px solid rgba(255,255,255,.15);
                    padding:8px 14px;border-radius:6px;font:13px 'Segoe UI',sans-serif;cursor:pointer;
                    backdrop-filter:blur(6px);transition:background .2s}
  #controls button:hover{background:rgba(255,255,255,.15)}
  #controls button.active{background:rgba(231,76,60,.5);border-color:#e74c3c}
  #info{position:fixed;bottom:30px;right:20px;z-index:10;max-width:280px;
        background:rgba(0,0,0,.85);padding:14px 16px;border-radius:8px;
        font:12px/1.7 'Segoe UI Mono','SF Mono',monospace;color:#ccc;
        box-shadow:0 2px 12px rgba(0,0,0,.5);backdrop-filter:blur(6px);
        display:none;border:1px solid rgba(255,255,255,.08)}
  #info .label{color:#888;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
  #info .value{color:#fff;font-weight:600}
  #info .hi{color:#e74c3c}
  .cesium-credit-logoContainer{display:none !important}
</style>
</head>
<body>
<div id="cesiumContainer"></div>
<div id="hud">FIRMS VIIRS — <b>%%FIRE_COUNT%%</b> fire detections — %%REGION_LABEL%% (3D)</div>
<div id="legend">
  <b>FRP Intensity</b><br>
  <span style="color:#f1c40f">●</span> &lt; 10 MW &nbsp;
  <span style="color:#f39c12">●</span> 10–100 MW &nbsp;
  <span style="color:#e74c3c">●</span> &gt; 100 MW
</div>
<div id="controls">
  <button id="btnFires" class="active" onclick="toggleFires()">Fires</button>
  <button id="btnInfra" class="%%INFRA_BTN_CLASS%%" onclick="toggleInfra()" style="%%INFRA_BTN_STYLE%%">Infrastructure</button>
  <button id="btnBasemap" onclick="cycleBasemap()">Basemap</button>
  <button id="btnOverview" onclick="flyToOverview()">Overview</button>
</div>
<div id="info"></div>

<script>
var FIRE_DATA = %%FIRE_JSON%%;
var INFRA_DATA = %%INFRA_JSON%%;
var CAM = %%CAM_JSON%%;
var IS_WORLD = %%IS_WORLD%%;
var GOOGLE_KEY = "%%GOOGLE_API_KEY%%";

// --- Basemap tile providers (all free) ---
var darkTiles = new Cesium.UrlTemplateImageryProvider({
  url: "https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
  credit: "CartoDB Dark Matter",
  maximumLevel: 18,
});
var osmTiles = new Cesium.OpenStreetMapImageryProvider({
  url: "https://tile.openstreetmap.org/",
});
var lightTiles = new Cesium.UrlTemplateImageryProvider({
  url: "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png",
  credit: "CartoDB Positron",
  maximumLevel: 18,
});
var basemaps = [
  { provider: darkTiles,  name: "Dark" },
  { provider: osmTiles,   name: "OSM" },
  { provider: lightTiles, name: "Light" },
];
var currentBasemap = 0;

var viewer = new Cesium.Viewer("cesiumContainer", {
  imageryProvider: darkTiles,
  baseLayerPicker: false,
  geocoder: false,
  homeButton: false,
  sceneModePicker: false,
  navigationHelpButton: false,
  animation: false,
  timeline: false,
  fullscreenButton: false,
  vrButton: false,
  msaaSamples: IS_WORLD ? 1 : 4,
});

viewer.scene.globe.show = true;
viewer.scene.globe.baseColor = Cesium.Color.fromCssColorString("#0d1b2a");
viewer.scene.globe.showGroundAtmosphere = true;
viewer.scene.skyAtmosphere.show = true;
viewer.scene.fog.enabled = true;
viewer.scene.globe.enableLighting = false;
viewer.scene.backgroundColor = Cesium.Color.fromCssColorString("#020810");

// If user provided a Google API key, load 3D tiles on top as a bonus
if (GOOGLE_KEY && GOOGLE_KEY !== "YOUR_GOOGLE_API_KEY_HERE" && GOOGLE_KEY !== "") {
  try {
    Cesium.RequestScheduler.requestsByServer["tile.googleapis.com:443"] = 18;
    viewer.scene.primitives.add(
      new Cesium.Cesium3DTileset({
        url: "https://tile.googleapis.com/v1/3dtiles/root.json?key=" + GOOGLE_KEY,
        showCreditsOnScreen: true,
        maximumScreenSpaceError: IS_WORLD ? 16 : 8,
      })
    );
    viewer.scene.globe.show = false;
  } catch (e) { console.warn("Google 3D Tiles not loaded:", e); }
}

function cycleBasemap() {
  if (!viewer.scene.globe.show) return; // 3D tiles active, no globe to swap
  currentBasemap = (currentBasemap + 1) % basemaps.length;
  viewer.imageryLayers.removeAll();
  viewer.imageryLayers.addImageryProvider(basemaps[currentBasemap].provider);
  document.getElementById("btnBasemap").textContent = basemaps[currentBasemap].name;
  viewer.scene.requestRender();
}

// --- Fire rendering ---
var N = FIRE_DATA.length;
var USE_POINTS = N > 3000;

function frpToColor(frp) {
  if (frp > 100) return Cesium.Color.fromCssColorString("#e74c3c").withAlpha(0.9);
  if (frp > 10)  return Cesium.Color.fromCssColorString("#f39c12").withAlpha(0.85);
  return Cesium.Color.fromCssColorString("#f1c40f").withAlpha(0.8);
}

// Store fire data for click lookup when using points
var fireIndex = [];
var firePoints = null;
var fireDataSource = null;

if (USE_POINTS) {
  // GPU-instanced point primitives — handles 100k+ easily
  firePoints = viewer.scene.primitives.add(new Cesium.PointPrimitiveCollection());
  FIRE_DATA.forEach(function(f, i) {
    var col = frpToColor(f.frp);
    var sz = Math.max(4, Math.min(18, 3 + Math.sqrt(f.frp) * 0.8));
    firePoints.add({
      position: Cesium.Cartesian3.fromDegrees(f.lon, f.lat, 100),
      color: col,
      pixelSize: sz,
      disableDepthTestDistance: Number.POSITIVE_INFINITY,
      id: i,
    });
    fireIndex.push(f);
  });
} else {
  // Small dataset — use full cylinder entities
  fireDataSource = new Cesium.CustomDataSource("fires");
  viewer.dataSources.add(fireDataSource);

  FIRE_DATA.forEach(function(f) {
    var col = frpToColor(f.frp);
    var h = Math.max(800, Math.min(80000, Math.sqrt(f.frp) * 3000));

    fireDataSource.entities.add({
      position: Cesium.Cartesian3.fromDegrees(f.lon, f.lat),
      cylinder: {
        length: h, topRadius: 200, bottomRadius: 600,
        material: new Cesium.ColorMaterialProperty(col),
        outline: false,
        heightReference: Cesium.HeightReference.CLAMP_TO_GROUND,
      },
      properties: { frp: f.frp, date: f.date, time: f.time, confidence: f.conf, co2: f.co2, co2lo: f.co2lo, co2hi: f.co2hi },
      description:
        '<table style="font:13px monospace;color:#ccc;width:100%">' +
        '<tr><td style="color:#888">Date</td><td>' + f.date + ' ' + f.time + ' UTC</td></tr>' +
        '<tr><td style="color:#888">FRP</td><td style="color:#e74c3c;font-weight:700">' + f.frp.toFixed(1) + ' MW</td></tr>' +
        '<tr><td style="color:#888">Confidence</td><td>' + f.conf + '</td></tr>' +
        '<tr><td style="color:#888">CO\u2082 est.</td><td>' + f.co2.toFixed(1) + ' t (' + f.co2lo.toFixed(1) + '\u2013' + f.co2hi.toFixed(1) + ')</td></tr>' +
        '<tr><td style="color:#888">Coords</td><td>' + f.lat.toFixed(4) + ', ' + f.lon.toFixed(4) + '</td></tr>' +
        '</table>',
    });

    fireDataSource.entities.add({
      position: Cesium.Cartesian3.fromDegrees(f.lon, f.lat),
      ellipse: {
        semiMajorAxis: 1200 + Math.sqrt(f.frp) * 80,
        semiMinorAxis: 1200 + Math.sqrt(f.frp) * 80,
        material: new Cesium.ColorMaterialProperty(col.withAlpha(0.25)),
        heightReference: Cesium.HeightReference.CLAMP_TO_GROUND,
      },
    });
  });
}

// --- Infrastructure markers ---
var infraDataSource = new Cesium.CustomDataSource("infrastructure");
viewer.dataSources.add(infraDataSource);

INFRA_DATA.forEach(function(site) {
  var pinColor = site.type === "nuclear"
    ? Cesium.Color.fromCssColorString("#f1c40f")
    : Cesium.Color.fromCssColorString("#e74c3c");

  infraDataSource.entities.add({
    position: Cesium.Cartesian3.fromDegrees(site.lon, site.lat, 500),
    point: {
      pixelSize: 14, color: pinColor,
      outlineColor: Cesium.Color.WHITE, outlineWidth: 2,
      heightReference: Cesium.HeightReference.RELATIVE_TO_GROUND,
      disableDepthTestDistance: Number.POSITIVE_INFINITY,
      scaleByDistance: new Cesium.NearFarScalar(1e3, 1.5, 8e6, 0.5),
    },
    label: {
      text: site.name,
      font: "13px 'Segoe UI', sans-serif",
      fillColor: Cesium.Color.WHITE,
      outlineColor: Cesium.Color.BLACK, outlineWidth: 3,
      style: Cesium.LabelStyle.FILL_AND_OUTLINE,
      verticalOrigin: Cesium.VerticalOrigin.BOTTOM,
      pixelOffset: new Cesium.Cartesian2(0, -18),
      heightReference: Cesium.HeightReference.RELATIVE_TO_GROUND,
      disableDepthTestDistance: Number.POSITIVE_INFINITY,
      scaleByDistance: new Cesium.NearFarScalar(1e3, 1.0, 5e6, 0.4),
    },
    description:
      '<div style="font:13px monospace;color:#ccc"><b>' + site.name +
      '</b><br>Type: ' + site.type +
      '<br>Coords: ' + site.lat.toFixed(4) + ', ' + site.lon.toFixed(4) + '</div>',
  });

  infraDataSource.entities.add({
    position: Cesium.Cartesian3.fromDegrees(site.lon, site.lat),
    ellipse: {
      semiMajorAxis: 5000, semiMinorAxis: 5000,
      material: Cesium.Color.RED.withAlpha(0.08),
      outline: true, outlineColor: pinColor.withAlpha(0.4), outlineWidth: 1,
      heightReference: Cesium.HeightReference.CLAMP_TO_GROUND,
    },
  });
});

// --- Toggle controls ---
var firesVisible = true, infraVisible = INFRA_DATA.length > 0;
function toggleFires() {
  firesVisible = !firesVisible;
  if (USE_POINTS) { firePoints.show = firesVisible; }
  else { fireDataSource.show = firesVisible; }
  document.getElementById("btnFires").classList.toggle("active", firesVisible);
}
function toggleInfra() {
  infraVisible = !infraVisible;
  infraDataSource.show = infraVisible;
  document.getElementById("btnInfra").classList.toggle("active", infraVisible);
}
function flyToOverview() {
  viewer.camera.flyTo({
    destination: Cesium.Cartesian3.fromDegrees(CAM.lon, CAM.lat, CAM.alt),
    orientation: { heading: 0, pitch: Cesium.Math.toRadians(-65), roll: 0 },
    duration: 2.0,
  });
}

// --- Click/hover info ---
var infoDiv = document.getElementById("info");
var handler = new Cesium.ScreenSpaceEventHandler(viewer.scene.canvas);

function showFireInfo(f) {
  infoDiv.style.display = "block";
  infoDiv.innerHTML =
    '<div class="label">FRP</div><div class="value hi">' + f.frp.toFixed(1) + ' MW</div>' +
    '<div class="label">Date</div><div class="value">' + f.date + ' ' + f.time + ' UTC</div>' +
    '<div class="label">CO\u2082 est.</div><div class="value">' + f.co2.toFixed(1) + ' t' +
    (f.co2lo ? ' <span style="color:#666;font-size:11px">(' + f.co2lo.toFixed(1) + '\u2013' + f.co2hi.toFixed(1) + ')</span>' : '') + '</div>' +
    '<div class="label">Confidence</div><div class="value">' + f.conf + '</div>' +
    '<div class="label">Coords</div><div class="value">' + f.lat.toFixed(4) + ', ' + f.lon.toFixed(4) + '</div>';
}

handler.setInputAction(function(movement) {
  var picked = viewer.scene.pick(movement.endPosition);
  if (!Cesium.defined(picked)) { infoDiv.style.display = "none"; return; }

  // Point primitives — id is the index into fireIndex
  if (USE_POINTS && Cesium.defined(picked.primitive) && picked.primitive === firePoints && Cesium.defined(picked.id)) {
    showFireInfo(fireIndex[picked.id]);
    return;
  }
  // Entity cylinders — properties bag
  if (Cesium.defined(picked.id) && picked.id.properties && picked.id.properties.frp) {
    var p = picked.id.properties;
    showFireInfo({ frp: p.frp.getValue(), date: p.date.getValue(), time: p.time.getValue(),
                   co2: p.co2.getValue(), conf: p.confidence.getValue(), lat: 0, lon: 0 });
    return;
  }
  infoDiv.style.display = "none";
}, Cesium.ScreenSpaceEventType.MOUSE_MOVE);

// --- Initial camera ---
viewer.camera.flyTo({
  destination: Cesium.Cartesian3.fromDegrees(CAM.lon, CAM.lat, CAM.alt),
  orientation: { heading: 0, pitch: Cesium.Math.toRadians(-65), roll: 0 },
  duration: 0,
});
</script>
</body>
</html>"""


def build_3d_map(df: pd.DataFrame, region_cfg: dict,
                 show_infra: bool) -> str:
    """Generate a self-contained CesiumJS HTML with OSM tiles (free) on a 3D globe."""
    fire_records = []
    for _, r in df.iterrows():
        fire_records.append({
            "lat": round(float(r["latitude"]), 5),
            "lon": round(float(r["longitude"]), 5),
            "frp": round(float(r["frp"]), 2),
            "date": r["acq_date"].strftime("%Y-%m-%d"),
            "time": r["acq_hour"],
            "conf": r["confidence"],
            "co2": round(float(r["co2_tonnes"]), 2),
            "co2lo": round(float(r["co2_tonnes_low"]), 2),
            "co2hi": round(float(r["co2_tonnes_high"]), 2),
        })

    infra_records = []
    if show_infra:
        infra_records = [
            {"name": name, "lat": info["lat"], "lon": info["lon"], "type": info["type"]}
            for name, info in INFRASTRUCTURE.items()
        ]

    cam = {
        "lon": region_cfg["cam_lon"],
        "lat": region_cfg["cam_lat"],
        "alt": region_cfg["cam_alt"],
    }

    html = _CESIUM_HTML_TEMPLATE
    html = html.replace("%%FIRE_COUNT%%", f"{len(df):,}")
    html = html.replace("%%REGION_LABEL%%", region_cfg["label"])
    html = html.replace("%%FIRE_JSON%%", json.dumps(fire_records))
    html = html.replace("%%INFRA_JSON%%", json.dumps(infra_records))
    html = html.replace("%%CAM_JSON%%", json.dumps(cam))
    html = html.replace("%%IS_WORLD%%", "true" if region_cfg.get("bbox") == "world" else "false")
    html = html.replace("%%INFRA_BTN_CLASS%%", "active" if show_infra else "")
    html = html.replace("%%INFRA_BTN_STYLE%%", "" if show_infra else "display:none")
    html = html.replace("%%GOOGLE_API_KEY%%", GOOGLE_API_KEY or "")

    return html

# ---------------------------------------------------------------------------
# Time-series chart
# ---------------------------------------------------------------------------

def build_timeseries(df: pd.DataFrame, region_cfg: dict,
                     show_annotations: bool) -> go.Figure:
    daily = df.groupby("acq_date").agg(
        detections=("frp", "count"),
        co2_total=("co2_tonnes", "sum"),
        co2_low=("co2_tonnes_low", "sum"),
        co2_high=("co2_tonnes_high", "sum"),
    ).reset_index()

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=daily["acq_date"],
        y=daily["detections"],
        name="Fire Detections",
        marker_color="#e74c3c",
        opacity=0.85,
        yaxis="y",
    ))

    fig.add_trace(go.Scatter(
        x=pd.concat([daily["acq_date"], daily["acq_date"][::-1]]),
        y=pd.concat([daily["co2_high"], daily["co2_low"][::-1]]),
        fill="toself",
        fillcolor="rgba(52,152,219,0.15)",
        line=dict(width=0),
        name="CO₂ uncertainty range",
        yaxis="y2",
        showlegend=True,
        hoverinfo="skip",
    ))

    fig.add_trace(go.Scatter(
        x=daily["acq_date"],
        y=daily["co2_total"],
        name="Estimated CO₂ (tonnes)",
        mode="lines+markers",
        line=dict(color="#3498db", width=3),
        marker=dict(size=7),
        yaxis="y2",
    ))

    if show_annotations:
        for ann in STRIKE_ANNOTATIONS:
            d = pd.Timestamp(ann["date"])
            if daily["acq_date"].min() <= d <= daily["acq_date"].max():
                fig.add_shape(
                    type="line",
                    x0=ann["date"], x1=ann["date"],
                    y0=0, y1=1, yref="paper",
                    line=dict(color="#2ecc71", width=1.5, dash="dash"),
                )
                fig.add_annotation(
                    x=ann["date"], y=1, yref="paper",
                    text=ann["label"],
                    showarrow=False, xanchor="left", yanchor="bottom",
                    font=dict(size=10, color="#2ecc71"),
                    textangle=-35,
                )

    label = region_cfg["label"]
    fig.update_layout(
        template="plotly_dark",
        title=dict(
            text=f"Daily VIIRS Fire Detections & Estimated CO₂ — {label}",
            font=dict(size=18),
        ),
        xaxis=dict(title="Date", tickformat="%b %d"),
        yaxis=dict(
            title=dict(text="Fire Detections", font=dict(color="#e74c3c")),
            tickfont=dict(color="#e74c3c"), side="left",
        ),
        yaxis2=dict(
            title=dict(text="Estimated CO₂ (tonnes)", font=dict(color="#3498db")),
            tickfont=dict(color="#3498db"), overlaying="y", side="right",
        ),
        legend=dict(x=0.01, y=0.99, bgcolor="rgba(0,0,0,0.5)"),
        bargap=0.15,
        hovermode="x unified",
        height=520,
    )

    return fig

# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def compute_summary(df: pd.DataFrame, region_cfg: dict,
                    check_infra: bool) -> dict:
    peak_row = df.loc[df["frp"].idxmax()]
    date_range = df["acq_date"]
    n_days = max((date_range.max() - date_range.min()).days + 1, 1)

    co2_mid = round(df["co2_tonnes"].sum(), 2)
    co2_low = round(df["co2_tonnes_low"].sum(), 2)
    co2_high = round(df["co2_tonnes_high"].sum(), 2)

    summary: dict = {
        "methodology": {
            "source": "NASA FIRMS VIIRS (Layer 1 — rough estimate)",
            "pathway": "FRP × Δt × β × EF  (Wooster et al. 2005)",
            "observation_window_s": OBSERVATION_WINDOW_S,
            "vegetation_factors": {
                "beta_mid": BETA_VEG_MID,
                "beta_low": BETA_VEG_LOW,
                "beta_high": BETA_VEG_HIGH,
                "ef_co2_kg_per_kg_dm": EF_VEG,
            },
            "oil_factors": {
                "radiative_fraction": CHI_OIL,
                "heat_combustion_mj_per_kg": DH_C_OIL,
                "ef_co2_kg_per_kg_oil": EF_OIL,
                "beta_equivalent": round(BETA_OIL_EQUIV, 3),
            },
            "caveat": (
                "FIRMS estimates are rough (±factor of 3–5). Each detection is "
                "an instantaneous FRP snapshot extrapolated over a 6-hour window. "
                "Use GFAS (Layer 2) for validated daily totals with temporal "
                "integration and land-cover-aware emission factors."
            ),
        },
        "region": region_cfg["label"],
        "data_window": {
            "start": str(date_range.min().date()),
            "end": str(date_range.max().date()),
            "days_covered": n_days,
        },
        "total_detections": len(df),
        "total_co2_tonnes": {"low": co2_low, "mid": co2_mid, "high": co2_high},
        "peak_frp": {
            "value_mw": round(float(peak_row["frp"]), 2),
            "latitude": round(float(peak_row["latitude"]), 4),
            "longitude": round(float(peak_row["longitude"]), 4),
            "date": str(peak_row["acq_date"].date()),
            "time_utc": peak_row["acq_hour"],
        },
        "daily_average_detections": round(len(df) / n_days, 1),
        "confidence_breakdown": df["confidence"].value_counts().to_dict(),
    }

    if check_infra:
        conflict_start = pd.Timestamp("2025-02-28")
        since_conflict = df[df["acq_date"] >= conflict_start]
        near_infra_mask = df.apply(
            lambda r: near_infrastructure(r["latitude"], r["longitude"], SUMMARY_RADIUS_KM),
            axis=1,
        )
        summary["detections_since_feb28"] = len(since_conflict)
        summary["detections_near_infrastructure_10km"] = int(near_infra_mask.sum())
        summary["co2_near_infrastructure_tonnes"] = round(
            df.loc[near_infra_mask, "co2_tonnes"].sum(), 2
        )

    return summary


def print_summary(s: dict) -> None:
    label = s["region"]
    co2 = s["total_co2_tonnes"]
    print("\n" + "=" * 68)
    print(f"  FIRMS VIIRS FIRE DETECTION SUMMARY — {label.upper()}")
    print("=" * 68)
    w = s["data_window"]
    print(f"  Data window        : {w['start']} → {w['end']}  ({w['days_covered']} days)")
    print(f"  Total detections   : {s['total_detections']:,}")
    print(f"  CO₂ estimate (mid) : {co2['mid']:,.0f} tonnes")
    print(f"  CO₂ range          : {co2['low']:,.0f} – {co2['high']:,.0f} tonnes")
    if "detections_since_feb28" in s:
        print(f"  Since Feb 28       : {s['detections_since_feb28']:,}")
    if "detections_near_infrastructure_10km" in s:
        print(f"  Near infra (10 km) : {s['detections_near_infrastructure_10km']:,}")
    if "co2_near_infrastructure_tonnes" in s:
        print(f"  CO₂ near infra     : {s['co2_near_infrastructure_tonnes']:,.0f} tonnes")
    p = s["peak_frp"]
    print(f"  Peak FRP           : {p['value_mw']} MW  @ "
          f"{p['latitude']}, {p['longitude']}  "
          f"on {p['date']} {p['time_utc']} UTC")
    print(f"  Daily avg detect.  : {s['daily_average_detections']}")
    print(f"  Confidence split   : {s['confidence_breakdown']}")
    print(f"\n  ⚠  Layer 1 estimates are rough (±3–5×). Use Layer 2 (GFAS)")
    print(f"     for peer-reviewed daily CO₂ totals.")
    print("=" * 68 + "\n")

# ---------------------------------------------------------------------------
# CLI & Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="NASA FIRMS VIIRS fire detection — fetch, analyse, visualize.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument(
        "-r", "--region",
        choices=list(REGIONS.keys()),
        default="world",
        help="Preset region (default: world).\nAvailable: " + ", ".join(REGIONS.keys()),
    )
    p.add_argument(
        "-d", "--days", type=int, default=None,
        help="Days of data to fetch. Default varies by region.\n"
             "FIRMS NRT covers ~60 days; use --start-date for older data.",
    )
    p.add_argument(
        "--start-date", default=None,
        help="Explicit start date (YYYY-MM-DD). Overrides --days.",
    )
    p.add_argument(
        "--end-date", default=None,
        help="Explicit end date (YYYY-MM-DD). Defaults to today.",
    )
    p.add_argument(
        "-s", "--sources", nargs="+",
        choices=list(SOURCES_ALL.keys()),
        default=["viirs_noaa20", "viirs_noaa21"],
        help="Satellite sources (default: viirs_noaa20 viirs_noaa21).",
    )
    p.add_argument(
        "-o", "--output-dir", default="output",
        help="Output directory (default: output).",
    )
    p.add_argument(
        "--no-3d", action="store_true",
        help="Skip 3D map generation.",
    )
    p.add_argument(
        "--serve", action="store_true",
        help="Start a local HTTP server and open results in browser.\n"
             "Required for 3D map (CesiumJS tiles need HTTP, not file://).",
    )
    p.add_argument(
        "--port", type=int, default=8070,
        help="Port for the local server (default: 8070).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    region_cfg = REGIONS[args.region]
    source_codes = [SOURCES_ALL[s] for s in args.sources]
    show_infra = region_contains_iran(args.region)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).date()
    start_date = None

    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = (
            datetime.strptime(args.end_date, "%Y-%m-%d").date()
            if args.end_date else today
        )
        total_days = (end_date - start_date).days + 1
    else:
        total_days = args.days if args.days else region_cfg["default_days"]
        total_days = max(1, total_days)

    print(f"Region: {region_cfg['label']}  |  Days: {total_days}  |  "
          f"Sources: {', '.join(source_codes)}")
    if start_date:
        print(f"Date range: {start_date} → {start_date + timedelta(days=total_days - 1)}")

    # 1 — Fetch
    raw_df = fetch_fire_data(region_cfg["bbox"], total_days, source_codes,
                             start_date=start_date)

    # 2 — Process
    df = process(raw_df, check_infra=show_infra)
    if df.empty:
        sys.exit("No detections remain after filtering. Exiting.")

    # 3a — 2D Map
    print("Building 2D interactive map …")
    fire_map = build_map(df, region_cfg, show_infra)
    map_path = out / "firms_fire_map.html"
    fire_map.save(str(map_path))
    print(f"  ✓ 2D map saved → {map_path.resolve()}")

    # 3b — 3D Globe Map (CesiumJS + OSM, free — no API key needed)
    if not args.no_3d:
        print("Building 3D globe map …")
        html_3d = build_3d_map(df, region_cfg, show_infra)
        map3d_path = out / "firms_fire_3d.html"
        map3d_path.write_text(html_3d, encoding="utf-8")
        print(f"  ✓ 3D map saved → {map3d_path.resolve()}")

    # 4 — Time series
    print("Building time-series chart …")
    fig = build_timeseries(df, region_cfg, show_annotations=show_infra)
    ts_path = out / "firms_timeseries.html"
    fig.write_html(str(ts_path), include_plotlyjs="cdn")
    print(f"  ✓ Time series saved → {ts_path.resolve()}")

    # 5 — Raw data
    csv_path = out / "firms_raw_data.csv"
    df.to_csv(csv_path, index=False)
    print(f"  ✓ Raw CSV saved → {csv_path.resolve()}")

    # 6 — Summary
    summary = compute_summary(df, region_cfg, check_infra=show_infra)
    json_path = out / "firms_summary.json"
    json_path.write_text(json.dumps(summary, indent=2, default=str))
    print(f"  ✓ Summary JSON saved → {json_path.resolve()}")

    print_summary(summary)

    # 7 — Optional local server
    if args.serve:
        serve_output(out, args.port)


def serve_output(directory: Path, port: int) -> None:
    """Start a local HTTP server so CesiumJS can load tiles (file:// blocks CORS)."""
    handler = functools.partial(http.server.SimpleHTTPRequestHandler,
                                directory=str(directory.resolve()))

    server = None
    for try_port in range(port, port + 10):
        try:
            srv = http.server.HTTPServer(("127.0.0.1", try_port), handler)
            srv.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server = srv
            port = try_port
            break
        except OSError:
            print(f"  ⚠ port {try_port} in use, trying {try_port + 1} …")

    if server is None:
        print(f"  ✗ Could not find a free port in range {port}–{port + 9}. "
              "Open the HTML files directly or kill the old server process.")
        return

    url_3d = f"http://localhost:{port}/firms_fire_3d.html"
    url_2d = f"http://localhost:{port}/firms_fire_map.html"

    print(f"\n  Local server running at http://localhost:{port}/")
    print(f"  → 3D map : {url_3d}")
    print(f"  → 2D map : {url_2d}")
    print(f"  Press Ctrl+C to stop.\n")

    webbrowser.open(url_3d)

    signal.signal(signal.SIGINT, lambda *_: (server.shutdown(), sys.exit(0)))
    server.serve_forever()


if __name__ == "__main__":
    main()
