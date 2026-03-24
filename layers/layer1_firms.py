"""
Layer 1 wrapper — NASA FIRMS VIIRS fire detection.

Invokes the existing layer1_firms_fire.py logic and packages the
result as a LayerResult for the unified pipeline.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.models import EmissionCategory, LayerResult
from common.regions import REGIONS


def run(
    region: str = "iran",
    start_date: str | None = None,
    end_date: str | None = None,
    output_dir: str = "output",
) -> LayerResult | None:
    """Run Layer 1 and return a LayerResult.

    Delegates to the existing layer1_firms_fire module functions.
    """
    import layer1_firms_fire as L1

    region_cfg = REGIONS[region]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    from datetime import datetime, timedelta, timezone

    today = datetime.now(timezone.utc).date()
    sd = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    ed = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else today

    if sd:
        total_days = (ed - sd).days + 1
    else:
        total_days = region_cfg["default_days"]
        sd = today - timedelta(days=total_days - 1)

    source_codes = [L1.SOURCES_ALL["viirs_noaa20"], L1.SOURCES_ALL["viirs_noaa21"]]
    show_infra = L1.region_contains_iran(region)

    print(f"\n{'='*68}")
    print(f"  LAYER 1 — FIRMS VIIRS Fire Detection")
    print(f"{'='*68}")

    raw_df = L1.fetch_fire_data(region_cfg["bbox"], total_days, source_codes, start_date=sd)
    df = L1.process(raw_df, check_infra=show_infra)

    if df.empty:
        print("  ⚠ No detections after filtering.")
        return None

    csv_path = out / "firms_raw_data.csv"
    df.to_csv(csv_path, index=False)

    summary = L1.compute_summary(df, region_cfg, check_infra=show_infra)
    json_path = out / "firms_summary.json"
    json_path.write_text(json.dumps(summary, indent=2, default=str))

    co2_dict = summary.get("total_co2_tonnes", {})
    co2_mid = co2_dict.get("mid", 0)
    co2_low = co2_dict.get("low", 0)
    co2_high = co2_dict.get("high", 0)

    daily = df.groupby("acq_date").agg(
        co2_mid=("co2_tonnes", "sum"),
        co2_low=("co2_tonnes_low", "sum"),
        co2_high=("co2_tonnes_high", "sum"),
    ).reset_index().rename(columns={"acq_date": "date"})

    geo = [
        {"lat": r["latitude"], "lon": r["longitude"], "frp": r["frp"],
         "date": str(r["acq_date"]), "source": "FIRMS"}
        for _, r in df.head(5000).iterrows()
    ]

    return LayerResult(
        layer_name="Layer 1: FIRMS Fire",
        emission_category=EmissionCategory.FUEL_INFRASTRUCTURE,
        co2_tonnes_mid=co2_mid,
        co2_tonnes_low=co2_low,
        co2_tonnes_high=co2_high,
        daily_breakdown=daily,
        geo_points=geo,
        metadata={
            "source": "NASA FIRMS VIIRS (NOAA-20 + NOAA-21)",
            "total_detections": len(df),
            "near_infrastructure": int(df["near_infra"].sum()) if "near_infra" in df.columns else 0,
            "methodology": "FRP × 6h observation window × β → CO₂ (Wooster et al. 2005)",
            "csv_path": str(csv_path),
        },
    )
