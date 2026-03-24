#!/usr/bin/env python3
"""
NASA FIRMS VIIRS Fire Detection Visualizer
Fetches active fire data from the FIRMS API and renders an interactive map.
"""

import argparse
import io
import sys
import webbrowser
from pathlib import Path

import folium
import pandas as pd
import requests
from folium.plugins import HeatMap, MarkerCluster

MAP_KEY = "b8b8a015f0c1b6033fc15080a1a01223"

FIRMS_AREA_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

SOURCES = {
    "viirs_noaa20":  "VIIRS_NOAA20_NRT",
    "viirs_noaa21":  "VIIRS_NOAA21_NRT",
    "viirs_snpp":    "VIIRS_SNPP_NRT",
    "modis":         "MODIS_NRT",
}

REGIONS = {
    "world":          {"coords": "world",                      "center": [20, 0],    "zoom": 2},
    "north_america":  {"coords": "-170,10,-50,75",             "center": [45, -100], "zoom": 3},
    "south_america":  {"coords": "-85,-57,-32,14",             "center": [-15, -60], "zoom": 3},
    "europe":         {"coords": "-15,35,45,72",               "center": [50, 15],   "zoom": 4},
    "africa":         {"coords": "-20,-37,55,40",              "center": [5, 20],    "zoom": 3},
    "south_asia":     {"coords": "60,5,100,40",                "center": [22, 80],   "zoom": 4},
    "southeast_asia": {"coords": "90,-15,155,30",              "center": [10, 120],  "zoom": 4},
    "australia":      {"coords": "110,-50,180,-5",             "center": [-25, 135], "zoom": 4},
    "middle_east":    {"coords": "25,12,65,42",                "center": [28, 45],   "zoom": 5},
    "usa_west":       {"coords": "-130,30,-100,50",            "center": [40, -115], "zoom": 5},
    "amazon":         {"coords": "-75,-20,-45,5",              "center": [-8, -60],  "zoom": 5},
}


def fetch_fire_data(source: str, region: str, day_range: int) -> pd.DataFrame:
    region_info = REGIONS[region]
    source_code = SOURCES[source]
    url = f"{FIRMS_AREA_URL}/{MAP_KEY}/{source_code}/{region_info['coords']}/{day_range}"

    print(f"Fetching fires  →  source={source_code}  region={region}  days={day_range}")
    print(f"  URL: {url}")

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))
    print(f"  Retrieved {len(df):,} fire detections\n")
    return df


def confidence_color(val) -> str:
    """Map confidence (string or numeric) to a hex colour."""
    if isinstance(val, str):
        return {"l": "#f0ad4e", "n": "#e67e22", "h": "#e74c3c"}.get(val.lower(), "#e67e22")
    try:
        v = float(val)
    except (TypeError, ValueError):
        return "#e67e22"
    if v >= 80:
        return "#e74c3c"
    if v >= 50:
        return "#e67e22"
    return "#f0ad4e"


def build_map(df: pd.DataFrame, region: str, mode: str) -> folium.Map:
    region_info = REGIONS[region]
    m = folium.Map(
        location=region_info["center"],
        zoom_start=region_info["zoom"],
        tiles="CartoDB dark_matter",
        control_scale=True,
    )

    folium.TileLayer("CartoDB positron", name="Light").add_to(m)
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap").add_to(m)

    if df.empty:
        folium.Marker(
            region_info["center"],
            popup="No fire detections for this query.",
            icon=folium.Icon(icon="info-sign"),
        ).add_to(m)
        folium.LayerControl().add_to(m)
        return m

    if "bright_ti4" in df.columns:
        brt_col = "bright_ti4"
    elif "brightness" in df.columns:
        brt_col = "brightness"
    else:
        brt_col = None

    if mode in ("markers", "both"):
        cluster = MarkerCluster(name="Fire Detections (markers)").add_to(m)
        for _, row in df.iterrows():
            lat, lon = row["latitude"], row["longitude"]
            conf = row.get("confidence", "n")
            bright = row.get(brt_col, "N/A") if brt_col else "N/A"
            acq_date = row.get("acq_date", "?")
            acq_time = row.get("acq_time", "?")
            frp = row.get("frp", "N/A")

            popup_html = (
                f"<b>Fire Detection</b><br>"
                f"<b>Date:</b> {acq_date} {str(acq_time).zfill(4)[:2]}:{str(acq_time).zfill(4)[2:]} UTC<br>"
                f"<b>Lat/Lon:</b> {lat:.4f}, {lon:.4f}<br>"
                f"<b>Brightness:</b> {bright} K<br>"
                f"<b>FRP:</b> {frp} MW<br>"
                f"<b>Confidence:</b> {conf}"
            )

            folium.CircleMarker(
                location=[lat, lon],
                radius=4,
                color=confidence_color(conf),
                fill=True,
                fill_color=confidence_color(conf),
                fill_opacity=0.8,
                popup=folium.Popup(popup_html, max_width=260),
            ).add_to(cluster)

    if mode in ("heatmap", "both"):
        heat_data = df[["latitude", "longitude"]].values.tolist()
        if brt_col and brt_col in df.columns:
            weights = df[brt_col].fillna(300).tolist()
            mn, mx = min(weights), max(weights)
            if mx > mn:
                weights = [(w - mn) / (mx - mn) for w in weights]
            else:
                weights = [1.0] * len(weights)
            heat_data = [[r[0], r[1], w] for r, w in zip(heat_data, weights)]

        HeatMap(
            heat_data,
            name="Fire Heatmap",
            min_opacity=0.35,
            radius=12,
            blur=10,
            gradient={0.2: "#ffffb2", 0.4: "#fecc5c", 0.6: "#fd8d3c", 0.8: "#f03b20", 1.0: "#bd0026"},
        ).add_to(m)

    legend_html = """
    <div style="
        position: fixed; bottom: 30px; left: 30px; z-index: 1000;
        background: rgba(0,0,0,0.75); padding: 12px 16px; border-radius: 8px;
        font-family: 'Segoe UI', sans-serif; font-size: 13px; color: #fff;
        box-shadow: 0 2px 8px rgba(0,0,0,0.4); line-height: 1.8;">
        <b style="font-size:14px;">🔥 Fire Confidence</b><br>
        <span style="color:#e74c3c;">●</span> High &nbsp;
        <span style="color:#e67e22;">●</span> Nominal &nbsp;
        <span style="color:#f0ad4e;">●</span> Low
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    title_html = f"""
    <div style="
        position: fixed; top: 12px; left: 50%; transform: translateX(-50%);
        z-index: 1000; background: rgba(0,0,0,0.8); padding: 10px 24px;
        border-radius: 8px; font-family: 'Segoe UI', sans-serif;
        font-size: 16px; color: #fff; text-align: center;
        box-shadow: 0 2px 10px rgba(0,0,0,0.5);">
        NASA FIRMS VIIRS — <b>{len(df):,}</b> fire detections — <i>{region.replace('_',' ').title()}</i>
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    folium.LayerControl(collapsed=False).add_to(m)
    return m


def main():
    parser = argparse.ArgumentParser(
        description="Visualize NASA FIRMS VIIRS fire detections on an interactive map.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-r", "--region",
        choices=list(REGIONS.keys()),
        default="world",
        help="Preset region to query (default: world).\nAvailable: " + ", ".join(REGIONS.keys()),
    )
    parser.add_argument(
        "-s", "--source",
        choices=list(SOURCES.keys()),
        default="viirs_noaa20",
        help="Satellite source (default: viirs_noaa20).",
    )
    parser.add_argument(
        "-d", "--days",
        type=int,
        choices=range(1, 6),
        default=2,
        help="Number of days of data (1-5, default: 2).",
    )
    parser.add_argument(
        "-m", "--mode",
        choices=["markers", "heatmap", "both"],
        default="both",
        help="Visualisation mode (default: both).",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output HTML file path (default: firms_fire_<region>.html).",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Don't auto-open the map in a browser.",
    )
    args = parser.parse_args()

    try:
        df = fetch_fire_data(args.source, args.region, args.days)
    except requests.HTTPError as exc:
        print(f"API request failed: {exc}", file=sys.stderr)
        sys.exit(1)

    fire_map = build_map(df, args.region, args.mode)

    out_path = args.output or f"firms_fire_{args.region}.html"
    fire_map.save(out_path)
    full_path = str(Path(out_path).resolve())
    print(f"Map saved → {full_path}")

    if not args.no_open:
        webbrowser.open(f"file://{full_path}")


if __name__ == "__main__":
    main()
