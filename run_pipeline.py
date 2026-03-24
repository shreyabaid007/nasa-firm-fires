#!/usr/bin/env python3
"""
Unified conflict CO₂ emissions pipeline.

Runs all 7 data layers, collects LayerResults, and feeds them into the
dashboard generator. Layers with missing API keys are gracefully skipped.

Usage:
    python run_pipeline.py --region iran --start-date 2026-02-28 --end-date 2026-03-23
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.models import LayerResult
from common.regions import REGIONS


def _run_layer(module_path: str, fn_name: str, kwargs: dict, label: str) -> LayerResult | None:
    """Import and call a layer's run(), catching all exceptions."""
    try:
        import importlib
        mod = importlib.import_module(module_path)
        fn = getattr(mod, fn_name)
        return fn(**kwargs)
    except Exception:
        print(f"\n  ✗ {label} FAILED:")
        traceback.print_exc()
        return None


def parse_args():
    p = argparse.ArgumentParser(description="Conflict CO₂ emissions pipeline")
    p.add_argument("--region", default="iran", choices=list(REGIONS.keys()))
    p.add_argument("--start-date", default="2026-02-28")
    p.add_argument("--end-date", default=None)
    p.add_argument("--output-dir", default="output")
    p.add_argument("--skip-layers", nargs="*", default=[],
                   help="Layer numbers to skip, e.g. --skip-layers 3 4")
    p.add_argument("--serve", action="store_true",
                   help="Start local HTTP server after pipeline completes")
    p.add_argument("--port", type=int, default=8080)
    return p.parse_args()


def main():
    args = parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).date()
    end_date = args.end_date or str(today)
    skip = set(args.skip_layers)

    common_kwargs = {
        "region": args.region,
        "start_date": args.start_date,
        "end_date": end_date,
        "output_dir": args.output_dir,
    }

    region_cfg = REGIONS[args.region]
    print("=" * 68)
    print(f"  CONFLICT CO₂ EMISSIONS PIPELINE")
    print(f"  Region: {region_cfg['label']}  |  {args.start_date} → {end_date}")
    print("=" * 68)

    results: list[LayerResult] = []

    # --- Layer 1: FIRMS Fire ---
    if "1" not in skip:
        r = _run_layer("layers.layer1_firms", "run", common_kwargs, "Layer 1: FIRMS")
        if r:
            results.append(r)

    firms_csv = str(out / "firms_raw_data.csv")

    # --- Layer 2: GFAS CO₂ ---
    if "2" not in skip:
        l2_kwargs = {**common_kwargs, "firms_csv": firms_csv}
        r = _run_layer("layers.layer2_gfas", "run", l2_kwargs, "Layer 2: GFAS")
        if r:
            results.append(r)

    # --- Layer 3: TROPOMI ---
    if "3" not in skip:
        r = _run_layer("layers.layer3_tropomi", "run", common_kwargs, "Layer 3: TROPOMI")
        if r:
            results.append(r)

    # --- Layer 4: Nightfire ---
    if "4" not in skip:
        r = _run_layer("layers.layer4_nightfire", "run", common_kwargs, "Layer 4: Nightfire")
        if r:
            results.append(r)

    # --- Layer 5: ACLED ---
    if "5" not in skip:
        r = _run_layer("layers.layer5_acled", "run", common_kwargs, "Layer 5: ACLED")
        if r:
            results.append(r)

    # --- Layer 6: UNOSAT ---
    if "6" not in skip:
        r = _run_layer("layers.layer6_unosat", "run", common_kwargs, "Layer 6: UNOSAT")
        if r:
            results.append(r)

    # --- Layer 7: OpenSky ---
    if "7" not in skip:
        r = _run_layer("layers.layer7_opensky", "run", common_kwargs, "Layer 7: OpenSky")
        if r:
            results.append(r)

    # --- Summary ---
    print(f"\n{'='*68}")
    print(f"  PIPELINE COMPLETE — {len(results)} layers returned data")
    print(f"{'='*68}")

    if not results:
        print("  No data collected. Check API keys and network access.")
        return

    for r in results:
        print(r.summary_line())

    total_mid = sum(r.co2_tonnes_mid for r in results
                    if r.emission_category.value != "Atmospheric verification (cross-check)")
    total_low = sum(r.co2_tonnes_low for r in results
                    if r.emission_category.value != "Atmospheric verification (cross-check)")
    total_high = sum(r.co2_tonnes_high for r in results
                     if r.emission_category.value != "Atmospheric verification (cross-check)")
    print(f"\n  {'TOTAL (excl. cross-checks)':<73s} │ {total_mid:>12,.0f} t  "
          f"({total_low:,.0f}–{total_high:,.0f})")

    # --- Save consolidated JSON ---
    consolidated = {
        "region": args.region,
        "region_label": region_cfg["label"],
        "start_date": args.start_date,
        "end_date": end_date,
        "layers": [],
        "total_co2_mid": round(total_mid, 1),
        "total_co2_low": round(total_low, 1),
        "total_co2_high": round(total_high, 1),
    }

    for r in results:
        daily_records = []
        if not r.daily_breakdown.empty:
            daily_records = r.daily_breakdown.to_dict(orient="records")
        consolidated["layers"].append({
            "layer_name": r.layer_name,
            "category": r.emission_category.value,
            "co2_mid": r.co2_tonnes_mid,
            "co2_low": r.co2_tonnes_low,
            "co2_high": r.co2_tonnes_high,
            "daily": daily_records,
            "n_geo_points": len(r.geo_points),
            "metadata": {k: v for k, v in r.metadata.items()
                         if k != "sub_results"},
        })

    json_path = out / "pipeline_results.json"
    json_path.write_text(json.dumps(consolidated, indent=2, default=str))
    print(f"\n  ✓ Consolidated JSON → {json_path}")

    # --- Dashboard ---
    print("\n  Building unified dashboard …")
    try:
        from dashboard import build_dashboard
        dash_path = build_dashboard(results, consolidated, args.output_dir)
        print(f"  ✓ Dashboard → {dash_path}")
    except Exception:
        print("  ⚠ Dashboard generation failed:")
        traceback.print_exc()

    # --- Serve ---
    if args.serve:
        _serve(out, args.port)


def _serve(directory: Path, port: int):
    import http.server
    import socket
    import webbrowser

    handler = lambda *a: http.server.SimpleHTTPRequestHandler(*a, directory=str(directory))

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
        print("  ✗ Could not find an open port.")
        return

    url = f"http://127.0.0.1:{port}/dashboard.html"
    print(f"\n  Serving at {url}")
    print("  Press Ctrl+C to stop.\n")
    webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")


if __name__ == "__main__":
    main()
