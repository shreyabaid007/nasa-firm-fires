"""
Unified conflict CO₂ emissions dashboard.

Generates a single `output/dashboard.html` that combines:
  - Summary header with total CO₂ and per-category breakdown
  - Stacked bar chart by emission category over time
  - Multi-layer map with all geo-located detections
  - Per-layer detail panels
  - Methodology & data sources section
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from common.models import EmissionCategory, LayerResult


def build_dashboard(
    results: list[LayerResult],
    consolidated: dict[str, Any],
    output_dir: str = "output",
) -> str:
    """Build the unified HTML dashboard and return its path."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    html = _render_html(results, consolidated)
    path = out / "dashboard.html"
    path.write_text(html, encoding="utf-8")
    return str(path)


def _render_html(results: list[LayerResult], data: dict) -> str:
    total_mid = data.get("total_co2_mid", 0)
    total_low = data.get("total_co2_low", 0)
    total_high = data.get("total_co2_high", 0)
    region_label = data.get("region_label", "")
    start = data.get("start_date", "")
    end = data.get("end_date", "")

    category_totals = {}
    for r in results:
        cat = r.emission_category.value
        if cat not in category_totals:
            category_totals[cat] = {"mid": 0, "low": 0, "high": 0}
        category_totals[cat]["mid"] += r.co2_tonnes_mid
        category_totals[cat]["low"] += r.co2_tonnes_low
        category_totals[cat]["high"] += r.co2_tonnes_high

    category_cards_html = ""
    cat_colors = {
        EmissionCategory.FUEL_INFRASTRUCTURE.value: "#e74c3c",
        EmissionCategory.BUILDINGS.value: "#e67e22",
        EmissionCategory.COMBAT_FUEL.value: "#f39c12",
        EmissionCategory.EQUIPMENT.value: "#9b59b6",
        EmissionCategory.MUNITIONS.value: "#3498db",
        EmissionCategory.AVIATION_REROUTING.value: "#1abc9c",
        EmissionCategory.ATMOSPHERIC_VERIFICATION.value: "#95a5a6",
    }

    for cat, vals in category_totals.items():
        color = cat_colors.get(cat, "#666")
        is_crosscheck = "cross-check" in cat.lower()
        badge = ' <span class="badge">cross-check</span>' if is_crosscheck else ""
        category_cards_html += f"""
        <div class="cat-card" style="border-left: 4px solid {color}">
          <div class="cat-label">{cat}{badge}</div>
          <div class="cat-value">{vals['mid']:,.0f} t</div>
          <div class="cat-range">{vals['low']:,.0f} – {vals['high']:,.0f} t</div>
        </div>"""

    # Build daily time series data for Plotly stacked chart
    daily_traces = []
    for r in results:
        if r.daily_breakdown.empty:
            continue
        df = r.daily_breakdown.copy()
        if "date" not in df.columns:
            continue
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        records = df[["date", "co2_mid"]].to_dict(orient="records")
        color = cat_colors.get(r.emission_category.value, "#666")
        daily_traces.append({
            "name": r.layer_name,
            "color": color,
            "data": records,
        })

    traces_json = json.dumps(daily_traces, default=str)

    # Build geo points for Leaflet map
    all_geo = []
    for r in results:
        for pt in r.geo_points[:2000]:
            pt["layer"] = r.layer_name
            pt["color"] = cat_colors.get(r.emission_category.value, "#666")
            all_geo.append(pt)
    geo_json = json.dumps(all_geo[:8000], default=str)

    layer_panels_html = ""
    for r in results:
        meta_items = ""
        for k, v in r.metadata.items():
            if k == "sub_results":
                continue
            if isinstance(v, dict):
                v = json.dumps(v)
            meta_items += f"<tr><td>{k}</td><td>{v}</td></tr>"

        layer_panels_html += f"""
        <div class="layer-panel">
          <h3>{r.layer_name}</h3>
          <div class="layer-co2">
            <span class="mid">{r.co2_tonnes_mid:,.0f} t CO₂</span>
            <span class="range">({r.co2_tonnes_low:,.0f} – {r.co2_tonnes_high:,.0f})</span>
          </div>
          <div class="layer-cat">{r.emission_category.value}</div>
          <table class="meta-table">{meta_items}</table>
        </div>"""

    center_lat = 32
    center_lon = 53
    zoom = 6
    from common.regions import REGIONS
    if data.get("region") and data["region"] in REGIONS:
        rc = REGIONS[data["region"]]
        center_lat, center_lon = rc["center"]
        zoom = rc["zoom"]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Conflict CO₂ Emissions Dashboard — {region_label}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  :root {{
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #e6edf3; --text2: #8b949e; --accent: #58a6ff;
    --red: #e74c3c; --orange: #e67e22; --yellow: #f39c12;
    --green: #1abc9c; --blue: #3498db; --purple: #9b59b6;
    --gray: #95a5a6;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
         background: var(--bg); color: var(--text); line-height: 1.6; }}

  .container {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}

  header {{ text-align: center; padding: 40px 0 24px; }}
  header h1 {{ font-size: 28px; font-weight: 600; margin-bottom: 8px; }}
  header .subtitle {{ color: var(--text2); font-size: 16px; }}

  .total-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 32px; text-align: center;
    margin: 24px auto; max-width: 600px;
  }}
  .total-card .value {{ font-size: 48px; font-weight: 700; color: var(--red); }}
  .total-card .label {{ font-size: 14px; color: var(--text2); text-transform: uppercase;
                        letter-spacing: 1px; margin-bottom: 8px; }}
  .total-card .range {{ color: var(--text2); font-size: 16px; }}

  .cat-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px; margin: 24px 0;
  }}
  .cat-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 16px;
  }}
  .cat-label {{ font-size: 13px; color: var(--text2); margin-bottom: 4px; }}
  .cat-value {{ font-size: 24px; font-weight: 600; }}
  .cat-range {{ font-size: 12px; color: var(--text2); }}
  .badge {{
    background: var(--border); color: var(--text2); font-size: 10px;
    padding: 2px 6px; border-radius: 4px; vertical-align: middle;
  }}

  .section {{ margin: 40px 0; }}
  .section h2 {{
    font-size: 20px; font-weight: 600; margin-bottom: 16px;
    padding-bottom: 8px; border-bottom: 1px solid var(--border);
  }}

  #chart {{ background: var(--surface); border-radius: 8px; padding: 8px; }}
  #map {{ height: 500px; border-radius: 8px; border: 1px solid var(--border); }}

  .layer-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
    gap: 16px;
  }}
  .layer-panel {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 20px;
  }}
  .layer-panel h3 {{ font-size: 16px; margin-bottom: 8px; }}
  .layer-co2 .mid {{ font-size: 22px; font-weight: 600; }}
  .layer-co2 .range {{ color: var(--text2); font-size: 13px; margin-left: 8px; }}
  .layer-cat {{ color: var(--text2); font-size: 13px; margin: 4px 0 12px; }}
  .meta-table {{ width: 100%; font-size: 12px; border-collapse: collapse; }}
  .meta-table td {{ padding: 4px 8px; border-top: 1px solid var(--border);
                    vertical-align: top; word-break: break-word; }}
  .meta-table td:first-child {{ color: var(--text2); width: 40%; white-space: nowrap; }}

  .methodology {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 24px; font-size: 14px; line-height: 1.8;
  }}
  .methodology h3 {{ margin: 16px 0 8px; }}
  .methodology ul {{ padding-left: 20px; }}

  footer {{ text-align: center; padding: 40px 0; color: var(--text2); font-size: 12px; }}
</style>
</head>
<body>
<div class="container">

<header>
  <h1>Conflict CO₂ Emissions Dashboard</h1>
  <div class="subtitle">{region_label} &nbsp;|&nbsp; {start} → {end}</div>
</header>

<div class="total-card">
  <div class="label">Total Estimated CO₂ Emissions (excl. cross-checks)</div>
  <div class="value">{total_mid:,.0f} t</div>
  <div class="range">{total_low:,.0f} – {total_high:,.0f} tonnes</div>
</div>

<div class="cat-grid">
  {category_cards_html}
</div>

<div class="section">
  <h2>Daily CO₂ Emissions by Layer</h2>
  <div id="chart"></div>
</div>

<div class="section">
  <h2>Multi-Layer Map</h2>
  <div id="map"></div>
</div>

<div class="section">
  <h2>Layer Details</h2>
  <div class="layer-grid">
    {layer_panels_html}
  </div>
</div>

<div class="section">
  <h2>Methodology & Data Sources</h2>
  <div class="methodology">
    <p>This dashboard integrates multiple independent data sources to estimate
    conflict-related CO₂ emissions across five emission categories.</p>

    <h3>Layer 1: FIRMS Fire Detection (NASA)</h3>
    <p>VIIRS NOAA-20/21 active fire detections. CO₂ estimated via FRP × observation
    window × conversion factors (Wooster et al. 2005, Andreae 2019). Oil/gas
    infrastructure fires use radiative-fraction pathway.</p>

    <h3>Layer 2: GFAS Validated Emissions (Copernicus)</h3>
    <p>Global Fire Assimilation System daily gridded CO₂ flux. Assimilates satellite
    FRP with Kalman filter, land-cover-aware emission factors. Note: GFAS v1.2 discontinued
    Dec 2025 on public ADS; 2026 data requires ECMWF FTP access.</p>

    <h3>Layer 3: TROPOMI Atmospheric Plumes (ESA)</h3>
    <p>Sentinel-5P NO₂/SO₂/CO column densities via Google Earth Engine. Anomaly vs
    pre-conflict baseline provides independent atmospheric cross-check using NO₂:CO₂
    emission ratios (Beirle 2011, Reuter 2019).</p>

    <h3>Layer 4: VIIRS Nightfire (Payne Institute)</h3>
    <p>Nighttime fire/flare detections with blackbody temperature. Classifies gas flares
    (&gt;1600K) vs oil fires. CO₂ from radiant heat × fuel-specific burn rates
    (Elvidge et al. 2016).</p>

    <h3>Layer 5: ACLED Conflict Events</h3>
    <p>Geolocated battles, explosions, and violence events. Proxy CO₂ from combat fuel
    consumption, embodied carbon of destroyed equipment, and munitions
    (Neimark et al. 2024, CEOBS).</p>

    <h3>Layer 6: UNOSAT Building Damage</h3>
    <p>Satellite-assessed building destruction from Humanitarian Data Exchange. Embodied
    carbon computed using ICE Database v3 factors × building area × stories.</p>

    <h3>Layer 7: Aviation Rerouting (OpenSky)</h3>
    <p>Airspace monitoring via OpenSky Network. Rerouting CO₂ = affected flights ×
    extra distance × fuel burn rate (Eurocontrol/ICAO).</p>

    <h3>Uncertainty</h3>
    <p>All estimates carry significant uncertainty. FIRMS-based values are ±3-5×.
    ACLED proxy factors are order-of-magnitude. Building damage depends on available
    assessment data. Ranges shown reflect low/high bounds per methodology.</p>
  </div>
</div>

<footer>
  Generated {date.today().isoformat()} &nbsp;|&nbsp;
  Multi-layer conflict emissions pipeline v1.0
</footer>

</div>

<script>
// --- Stacked bar chart ---
const traces = {traces_json};
const plotTraces = traces.map(t => {{
  const dates = t.data.map(d => d.date);
  const vals = t.data.map(d => d.co2_mid || 0);
  return {{
    x: dates, y: vals, name: t.name, type: 'bar',
    marker: {{ color: t.color }},
  }};
}});
Plotly.newPlot('chart', plotTraces, {{
  barmode: 'stack',
  paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
  font: {{ color: '#e6edf3', size: 12 }},
  xaxis: {{ gridcolor: '#30363d', title: 'Date' }},
  yaxis: {{ gridcolor: '#30363d', title: 'CO₂ (tonnes)' }},
  legend: {{ orientation: 'h', y: -0.15 }},
  margin: {{ t: 20, r: 20, b: 80, l: 60 }},
}}, {{ responsive: true }});

// --- Leaflet map ---
const map = L.map('map', {{
  center: [{center_lat}, {center_lon}],
  zoom: {zoom},
}});
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
  attribution: '&copy; CartoDB &copy; OSM',
  maxZoom: 18,
}}).addTo(map);

const geoPoints = {geo_json};
const layerGroups = {{}};

geoPoints.forEach(pt => {{
  const lyr = pt.layer || 'Other';
  if (!layerGroups[lyr]) layerGroups[lyr] = L.layerGroup();

  const r = Math.max(3, Math.min(12, Math.sqrt(pt.frp || 10) * 1.5));
  const popup = Object.entries(pt)
    .filter(([k]) => !['color','layer'].includes(k))
    .map(([k,v]) => `<b>${{k}}</b>: ${{v}}`)
    .join('<br>');

  L.circleMarker([pt.lat, pt.lon], {{
    radius: r, color: pt.color, fillColor: pt.color,
    fillOpacity: 0.7, weight: 1,
  }}).bindPopup(popup).addTo(layerGroups[lyr]);
}});

Object.entries(layerGroups).forEach(([name, grp]) => {{
  grp.addTo(map);
}});
L.control.layers(null, layerGroups, {{ collapsed: false }}).addTo(map);
</script>
</body>
</html>"""
