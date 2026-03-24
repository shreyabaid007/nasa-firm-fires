"""
Layer 2 wrapper — CAMS GFAS CO₂ fire emissions.

Invokes the existing layer2_gfas_co2.py logic and packages the
result as a LayerResult for the unified pipeline.
"""

from __future__ import annotations

import json
import sys
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
    firms_csv: str = "output/firms_raw_data.csv",
) -> LayerResult | None:
    """Run Layer 2 and return a LayerResult."""
    import layer2_gfas_co2 as L2

    region_cfg = REGIONS[region]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    from datetime import datetime, timedelta, timezone

    today = datetime.now(timezone.utc).date()
    gfas_lag = timedelta(days=3)

    ds = start_date or str(today - timedelta(days=region_cfg["default_days"]))
    de = end_date or str(today - gfas_lag)

    print(f"\n{'='*68}")
    print(f"  LAYER 2 — GFAS / FIRMS CO₂ Emissions")
    print(f"{'='*68}")

    gfas_file = str(out / "gfas_co2_raw")
    gfas_path = L2.fetch_gfas(region_cfg.get("area"), ds, de, gfas_file)

    firms_df = L2.load_firms_daily(firms_csv)

    if gfas_path is not None:
        daily_df, _ = L2.process_gfas(gfas_path)
        if not daily_df.empty and firms_df is not None:
            hybrid_df = L2.compute_hybrid_co2(daily_df, firms_df)
            co2_mid = hybrid_df["co2_hybrid_total"].sum()
            co2_low = co2_mid * 0.7
            co2_high = co2_mid * 1.3
            daily = hybrid_df.rename(columns={
                "co2_hybrid_total": "co2_mid",
            })[["date", "co2_mid"]].copy()
            daily["co2_low"] = daily["co2_mid"] * 0.7
            daily["co2_high"] = daily["co2_mid"] * 1.3
            source = "CAMS GFAS + FIRMS hybrid"
        elif not daily_df.empty:
            co2_mid = daily_df["co2_tonnes_gfas"].sum()
            co2_low = co2_mid * 0.7
            co2_high = co2_mid * 1.3
            daily = daily_df.rename(columns={"co2_tonnes_gfas": "co2_mid"})[["date", "co2_mid"]].copy()
            daily["co2_low"] = daily["co2_mid"] * 0.7
            daily["co2_high"] = daily["co2_mid"] * 1.3
            source = "CAMS GFAS (validated)"
        else:
            return _firms_only_result(firms_df, out)
    else:
        return _firms_only_result(firms_df, out)

    return LayerResult(
        layer_name="Layer 2: GFAS CO₂",
        emission_category=EmissionCategory.FUEL_INFRASTRUCTURE,
        co2_tonnes_mid=round(co2_mid, 1),
        co2_tonnes_low=round(co2_low, 1),
        co2_tonnes_high=round(co2_high, 1),
        daily_breakdown=daily,
        metadata={"source": source, "methodology": "GFAS FRP assimilation + Kalman filter"},
    )


def _firms_only_result(firms_df: pd.DataFrame | None, out: Path) -> LayerResult | None:
    if firms_df is None or firms_df.empty:
        print("  ⚠ No GFAS or FIRMS data available for Layer 2.")
        return None

    co2_mid = firms_df["co2_tonnes_firms"].sum()
    co2_low = firms_df.get("co2_tonnes_firms_low", firms_df["co2_tonnes_firms"]).sum()
    co2_high = firms_df.get("co2_tonnes_firms_high", firms_df["co2_tonnes_firms"]).sum()

    daily = firms_df[["date"]].copy()
    daily["co2_mid"] = firms_df["co2_tonnes_firms"]
    daily["co2_low"] = co2_low / max(len(firms_df), 1)
    daily["co2_high"] = co2_high / max(len(firms_df), 1)

    return LayerResult(
        layer_name="Layer 2: FIRMS-only CO₂",
        emission_category=EmissionCategory.FUEL_INFRASTRUCTURE,
        co2_tonnes_mid=round(co2_mid, 1),
        co2_tonnes_low=round(co2_low, 1),
        co2_tonnes_high=round(co2_high, 1),
        daily_breakdown=daily,
        metadata={
            "source": "FIRMS Layer 1 (GFAS unavailable for 2026)",
            "methodology": "FRP-based rough estimate (±3-5×)",
        },
    )
