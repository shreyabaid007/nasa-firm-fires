"""
Shared data models for the multi-layer CO2 pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd


class EmissionCategory(str, Enum):
    FUEL_INFRASTRUCTURE = "Destroyed fuel infrastructure"
    BUILDINGS = "Destroyed buildings (embodied carbon)"
    COMBAT_FUEL = "Combat fuel consumption"
    EQUIPMENT = "Equipment embodied carbon"
    MUNITIONS = "Missiles and drones"
    AVIATION_REROUTING = "Aviation rerouting"
    ATMOSPHERIC_VERIFICATION = "Atmospheric verification (cross-check)"


@dataclass
class LayerResult:
    """Standard output from every layer module."""

    layer_name: str
    emission_category: EmissionCategory
    co2_tonnes_mid: float = 0.0
    co2_tonnes_low: float = 0.0
    co2_tonnes_high: float = 0.0
    daily_breakdown: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(
        columns=["date", "co2_mid", "co2_low", "co2_high"]
    ))
    geo_points: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def summary_line(self) -> str:
        cat = self.emission_category.value
        if self.co2_tonnes_mid > 0:
            return (
                f"  {self.layer_name:<28s} │ {cat:<42s} │ "
                f"{self.co2_tonnes_mid:>12,.0f} t  "
                f"({self.co2_tonnes_low:,.0f}–{self.co2_tonnes_high:,.0f})"
            )
        return f"  {self.layer_name:<28s} │ {cat:<42s} │ cross-check only"
