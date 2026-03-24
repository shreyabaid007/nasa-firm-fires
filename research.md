# Building a real-time CO2 dashboard for Operation Epic Fury

**Five million tonnes of CO2 equivalent were released in the first 14 days of the US-Israel-Iran conflict** — exceeding Iceland's entire annual emissions — yet no real-time emissions tracker exists for this war. The Climate and Community Institute's preliminary assessment confirms that satellite data, open APIs, and established emission factor databases make such a dashboard technically feasible today. The methodology has been battle-tested on Ukraine (236.8 MtCO2e over three years) and Gaza (33.2 MtCO2e total), and the same research teams are already applying it to Iran. This report provides the complete blueprint: which data sources work for each emission category, the exact APIs and code to access them, the calculation frameworks to convert raw observations into CO2 estimates, and the architecture to tie it all together.

---

## The five emission categories and what data actually exists for each

The core challenge is that no single satellite or API covers all conflict emission categories. Each requires a different combination of remote sensing, proxy modeling, and ground-truth data. Here is the honest assessment of what's achievable for Iran right now.

**Destroyed buildings (embodied carbon) — 2,415,000 tCO2e in 14 days.** This is the largest emission category, driven by Iran's **~20,000 damaged civilian structures**. Building damage can be estimated through UNOSAT satellite damage assessments (typically available 2–7 days after events, published on HDX), Sentinel-1 SAR radar imagery (works through clouds, 10m resolution, using the open-source Pixel-Wise T-Test algorithm), and Copernicus Emergency Management Service rapid mapping. The conversion uses floor area × material-specific embodied carbon factors from the ICE Database: residential concrete buildings at **400–700 kgCO2e/m²**, commercial at 500–900 kgCO2e/m², industrial at 600–1,200 kgCO2e/m². Accuracy is moderate — damage extent is observable but building composition requires assumptions. This category is inherently delayed by days to weeks.

**Destroyed fuel infrastructure — 1,883,000 tCO2e in 14 days.** The Tehran oil depot strikes (March 7–8) and South Pars gas field attack (March 18) are the defining events. NASA FIRMS VIIRS provides **near-real-time fire detection within 3 hours** at 375m resolution with Fire Radiative Power (FRP) values — critical for quantifying combustion. CAMS/GFAS converts this FRP into daily gridded CO2 flux estimates at 0.1° resolution. VIIRS Nightfire from the Payne Institute provides nighttime temperature characterization of burning infrastructure, and has already been used to analyze Israeli strikes on Iran from June 2025. Sentinel-5P TROPOMI detects SO2 and CO plumes from refinery fires — the Tehran fires produced a toxic plume stretching **1,000+ km** detectable in satellite aerosol data. The emission factor for crude oil combustion is approximately **423 kgCO2 per barrel** (IPCC). This is the most satellite-observable category with good real-time coverage.

**Combat fuel consumption — 529,000 tCO2e in 14 days.** Military fuel burn is estimated through proxy modeling, not direct observation. The key emission factors: F-35 fighters burn **~5,600 kg of jet fuel per hour** (~17.7 tCO2/hr), F-16s approximately 3,500 kg/hr cruise, M1 Abrams tanks consume fuel at 0.6 miles per gallon, and naval vessels burn marine diesel at **3.206 kgCO2/kg fuel**. The activity data comes from open-source conflict reporting — ACLED provides geolocated event data (1–2 week delay), CSIS and think tank strike counts, and media reporting on sortie numbers. The US DOD's fact sheet cited 7,000+ targets struck in three weeks, with 50+ Iranian naval vessels destroyed. OpenSky Network can track civilian aviation rerouting (which UNCTAD estimates adds **70% more emissions** on Singapore–Northern Europe routes via Cape of Good Hope), but cannot track military aircraft with transponders off. This category relies heavily on modeling and has the widest uncertainty ranges.

**Equipment embodied carbon — 172,000 tCO2e in 14 days.** Estimating the embodied carbon of destroyed military hardware (28 Iranian aircraft, 21+ naval vessels, ~300 missile launchers in the first 14 days) requires lifecycle assessment data that militaries rarely publish. Lockheed Martin is among the few defense firms disclosing product-use emissions. CEOBS estimates military supply chain emissions run **5–6× higher than direct operational emissions**. The approach is fundamentally proxy-based: count destroyed platforms from open-source reporting, apply estimated embodied carbon per platform type. No satellite data directly addresses this category.

**Missiles and drones — 55,000 tCO2e in 14 days.** Iran alone launched 500+ ballistic missiles and 2,000+ drones; the US-Israel coalition fired thousands of precision munitions. Each is calculated as: (fuel burn per unit + embodied manufacturing carbon) × number expended. The Gaza study established per-munition factors for specific weapon types. Data comes from strike reporting and military announcements, not satellites. This is the smallest but most straightforward category to estimate once activity data is compiled.

---

## Three converging methodologies now define the field

The conflict emissions accounting field has rapidly professionalized around three interlocking methodological frameworks, all sharing researchers and cross-referencing each other.

**The Initiative on GHG Accounting of War (IGGAW)**, led by Dutch carbon accountant Lennard de Klerk with IPCC member Svitlana Krakovska, published the **first formal methodological guidance for conflict emissions** at COP29 in November 2024. It covers 11 subcategories across pre-conflict, conflict, and post-conflict phases, aligned with IPCC national inventory guidelines. Their Ukraine accounting reached **236.8 MtCO2e** over three years with Monte Carlo uncertainty of 22% at 95% confidence. The methodology is explicitly designed to produce legally admissible evidence — "verifiable, comparable data that can potentially be used in the legal field." IGGAW uses EFFIS satellite fire data and TROPOMI atmospheric monitoring for near-real-time components.

**The CCI/Neimark "Scope 3+" methodology**, published in the peer-reviewed journal *One Earth* (March 2026), extends the GHG Protocol's scope framework to conflict. It adds a novel "Scope 3+" category covering infrastructure destruction, displacement, reconstruction, and other conflict-specific emissions that don't fit corporate accounting frameworks. The Gaza study applied this to produce the **33.2 MtCO2e total lifecycle estimate**, and the same team (Fred Otu-Larbi, Patrick Bigger, Benjamin Neimark) published the Iran 14-day estimate of 5.055 MtCO2e within three weeks of the conflict's start — demonstrating that near-real-time accounting is viable.

**CEOBS's military emissions framework** provides the institutional scaffolding, proposing Scope 1 (direct fuel use), Scope 2 (purchased energy), Scope 3 (supply chain), and Scope 3+ (conflict-specific) categories. Their finding that military emissions represent an estimated **5.5% of global GHG** with an 82% reporting gap under UNFCCC makes the case for independent monitoring. For the Iran conflict specifically, CEOBS identified **300+ environmentally relevant incidents** across 12 countries within 10 days, using their WISEN wartime incident database. Their approach combines social media monitoring with satellite verification — a model any dashboard should replicate.

The core calculation engine across all three is identical: **Activity Data × Emission Factors**, with Monte Carlo simulation for uncertainty propagation. The FRP-to-CO2 pathway follows Wooster et al. (2005): FRP (MW) is temporally integrated to Fire Radiative Energy (FRE in MJ), multiplied by a biome-specific conversion factor (β ≈ 0.368 kg dry matter per MJ), then multiplied by CO2 emission factors (~1,580–1,640 g CO2 per kg dry matter depending on fuel type). GFAS applies this globally at 0.1° resolution daily.

---

## Seven data layers to activate today, ranked by immediacy

Every data source below is free and accessible with minimal setup. They should be activated in this order for maximum immediate impact.

**Layer 1 — NASA FIRMS VIIRS (activate first, minutes to set up).** Register for a free MAP_KEY at `firms.modaps.eosdis.nasa.gov/api/map_key`. Query the Iran bounding box `[44, 25, 63.5, 40]` using:

```python
import pandas as pd
MAP_KEY = 'your_key'
url = f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/44,25,63.5,40/10'
fires = pd.read_csv(url)
# Key columns: latitude, longitude, frp (MW), confidence, acq_date
```

VIIRS provides **375m resolution** fire detections with FRP values within 3 hours of satellite overpass. This captures the Tehran oil depot fires, South Pars gas field burns, and any ongoing infrastructure fires. The GOES geostationary source adds 15-minute temporal resolution for the Middle East region. Transaction limit: 5,000 per 10-minute window.

**Layer 2 — CAMS/GFAS fire emissions (daily CO2 estimates).** Register at `ads.atmosphere.copernicus.eu`, install `cdsapi`, and retrieve the `cams-global-fire-emissions-gfas` dataset. GFAS directly provides `co2fire` flux in kg/m²/s at 0.1° resolution — no manual FRP conversion needed. It runs daily using MODIS and VIIRS FRP inputs with Kalman filter gap-filling. One critical caveat: GFAS is designed for vegetation fires and may partially filter industrial/oil fires. Cross-reference with raw FIRMS FRP data to catch what GFAS misses.

**Layer 3 — Sentinel-5P TROPOMI via Google Earth Engine (atmospheric verification).** Register at `earthengine.google.com`, authenticate via `ee.Authenticate()`, then query `COPERNICUS/S5P/NRTI/L3_NO2` for near-real-time NO2 column data over Iran. SO2 plumes from the Tehran refinery fires and CO columns from large combustion events are directly observable. TROPOMI's **daily global coverage at ~5.5 km resolution** makes it ideal for tracking pollution dispersion. Pre-conflict baseline comparison (January–February 2026) against March 2026 reveals anomalous emission signatures. TROPOMI cannot directly measure CO2, but NO2:CO2 ratios from combustion chemistry provide proxy estimates.

**Layer 4 — VIIRS Nightfire from Payne Institute/EOG.** Register at `eogdata.mines.edu` for nightly combustion source data with temperature characterization. The Payne Institute has already demonstrated this for Israeli strikes on Iran from June 2025, making it a proven tool for this specific conflict. Temperature profiles distinguish gas flares (1,500–2,000K) from oil depot fires, enabling source attribution.

**Layer 5 — ACLED conflict events (1–2 week delay).** Register at `developer.acleddata.com` for geolocated conflict event data. Query for Iran with `event_type=Battles|Explosions/Remote violence`. ACLED provides the ground-truth event catalog (date, location, actors, fatalities, narrative notes) needed to attribute satellite fire detections to specific military actions. The 1–2 week coding delay means it serves as verification rather than real-time alerting.

**Layer 6 — UNOSAT/HDX building damage assessments.** Access via `data.humdata.org/organization/unosat` or the HDX CKAN API (`data.humdata.org/api/3/action/package_search?q=iran`). UNOSAT has activated for every major recent conflict within days. Building-level damage grading (destroyed, severely damaged, moderately damaged) directly feeds the embodied carbon calculation: damaged area × ICE Database emission factors per building type.

**Layer 7 — OpenSky Network for aviation rerouting.** The REST API at `opensky-network.org/api/states/all` with Iran bounding box parameters tracks civilian flight diversions. With **95% of Strait of Hormuz shipping halted** and Gulf airspace restricted, the emissions impact from rerouted global commerce is substantial — but this is the hardest category to attribute specifically to the conflict.

---

## Dashboard architecture: from raw feeds to CO2 estimates

The recommended architecture uses a PostgreSQL + PostGIS + TimescaleDB stack for geospatial time-series storage, FastAPI for the async backend, and Deck.gl with Mapbox GL JS for GPU-accelerated visualization. The critical design pattern is a three-stage pipeline.

**Stage 1: Ingestion.** Apache Airflow DAGs schedule data fetches at source-appropriate intervals — FIRMS every 3 hours, GFAS daily at 07:00 UTC, ACLED weekly, Sentinel-5P daily. Each source feeds into a raw data table with standardized fields: `time (TIMESTAMPTZ)`, `geom (GEOGRAPHY)`, `source_type`, `data_source`, `raw_data (JSONB)`. TimescaleDB hypertables with automatic time partitioning handle the time-series dimension; PostGIS GIST indexes handle spatial queries.

**Stage 2: Emission calculation engine.** For each ingested observation, apply the appropriate conversion pathway. FIRMS fire detections with FRP → integrate FRP over observation window → multiply by β (biome/fuel-specific, **0.37–1.60 kg/MJ**) → multiply by EF_CO2 (1,500–1,640 g/kg dry matter). UNOSAT damage polygons → intersect with building footprint data → estimate floor area → multiply by ICE embodied carbon factors. ACLED events → classify by type → apply proxy emission rates (sortie counts × aircraft fuel burn, ground operations × vehicle fuel consumption). Each calculation produces a point estimate with Monte Carlo confidence intervals (minimum 10,000 simulations per estimate).

**Stage 3: Aggregation and display.** TimescaleDB continuous aggregates pre-compute hourly, daily, and weekly emission totals by category and geography. The frontend renders a Deck.gl HexagonLayer for spatial emission density, ScatterplotLayer for individual events, and D3.js time-series charts for cumulative totals. WebSocket connections via FastAPI push updates when new data arrives.

A minimum viable dashboard can be built in **2–4 weeks** covering FIRMS fire detection and GFAS CO2 estimates with basic Folium mapping. The full multi-source platform with uncertainty quantification and all seven data layers requires 8–12 weeks. The StrikeMap open-source project (`github.com/strikemaplive/StrikeMap`) provides a proven starting template with NASA FIRMS integration, multi-source intelligence aggregation, and Mapbox visualization already built.

---

## The data availability matrix holds up — with important nuances

The user's matrix showing most Iran categories as RED (actively accumulating) is accurate for a 22-day-old conflict. Here is the verified status as of March 23, 2026:

| Category | Iran Status | Notes |
|---|---|---|
| Landscape fires (FIRMS/EFFIS) | 🔴 Active, data accumulating | VIIRS detections confirmed for Tehran oil fires, South Pars |
| Atmospheric plumes (TROPOMI) | 🔴 Active, data accumulating | SO2/CO plumes tracked 1,000+ km from Tehran fires |
| Fire-based CO2 (GFAS) | 🔴 Active, daily data | May undercount oil/fuel fires vs. vegetation fires |
| Building damage (UNOSAT) | 🟠 Partial — assessments likely in progress | ~20,000 structures damaged per Iranian Red Crescent |
| Conflict events (ACLED) | 🔴 Active, 1–2 week lag | 7,000+ targets struck; events coded with delay |
| Displacement | 🔴 Active | Millions affected across 12 countries |
| Energy infrastructure attacks | 🔴 Active, well-documented | Tehran depots, South Pars, Kharg Island |
| Aviation rerouting | 🟢 Live via OpenSky | Gulf airspace closed; Hormuz shipping down 95% |
| Military fuel/warfare | 🟠 Proxy only | Requires open-source compilation of sortie/vehicle data |
| Oil/gas flaring | 🔴 Active via VIIRS Nightfire | Payne Institute precedent for Iran |

The comparison with Ukraine (3+ years of IGGAW data), Gaza (peer-reviewed One Earth methodology), and Sudan (CEOBS monitoring with limited satellite coverage) is structurally sound. Iran has the advantage of being a conflict where **energy infrastructure is a primary target**, making satellite-observable fire and pollution signatures unusually prominent compared to infantry-heavy conflicts.

---

## A credible dashboard demands methodological rigor, not just data feeds

Tyler McBrien's *Baffler* analysis ("Situational Unawareness," March 12, 2026) provides an essential warning: the Iran conflict has spawned an explosion of "vibe-coded situation monitor slop" — dashboards that create an illusion of intelligence through data layering without methodological substance. McBrien distinguishes between rigorous OSINT (Bellingcat's geolocated investigations, the Berkeley Protocol on Digital Open Source Investigations) and amateur "BROSINT" that fosters apophenia — pattern-seeking in random data.

A legitimate conflict emissions dashboard must follow the CEOBS/CCI model: transparent methodology published alongside the data, explicit uncertainty ranges on every estimate, clear attribution of data sources, and honest acknowledgment of what cannot be measured. The CCI's Monte Carlo uncertainty analysis and the IGGAW's 22% relative uncertainty at 95% CI set the standard. Every CO2 number displayed should carry confidence bounds. Bilawal Sidhu's WorldView platform (planned for open-source release April 2026) demonstrates the power of multi-source data fusion on a 3D globe — but its value came from layering data thoughtfully, not from the technology alone.

The most impactful immediate contribution is not another real-time map but rather **systematizing the FRP-to-CO2 pipeline for oil infrastructure fires** — a gap in the existing GFAS methodology, which is optimized for vegetation biomass burning. Oil and fuel fires have fundamentally different emission profiles than forest fires: higher CO2 per unit energy, different particulate composition, and no biome-specific β factor in the standard literature. Calibrating this conversion for the Tehran oil depot fires using the combined FIRMS FRP, TROPOMI SO2/CO plume data, and known fuel volumes at struck facilities would be a genuine methodological contribution to the field.

---

## Conclusion

The infrastructure for real-time conflict emissions tracking exists and is already producing results — CCI published credible 14-day estimates within three weeks of the conflict's start. The path from here is not inventing new methodology but **operationalizing existing ones into a continuous pipeline**. The IGGAW's IPCC-aligned framework provides the accounting structure, the CCI's Scope 3+ approach covers the full lifecycle, and CEOBS's incident monitoring provides the ground-truth event catalog. The satellite stack — FIRMS for fire detection, GFAS for CO2 flux, TROPOMI for atmospheric verification, Nightfire for infrastructure characterization — covers the observable categories with free, open data updated daily or faster. What remains proxy-dependent (military fuel consumption, equipment embodied carbon, munitions) will always require open-source intelligence compilation rather than remote sensing. The honest dashboard acknowledges this boundary explicitly, showing satellite-derived estimates in one panel and modeled estimates in another, with uncertainty ranges throughout. At **5+ million tCO2e in 14 days and climbing**, the emissions from Operation Epic Fury are already globally significant — making accurate, transparent tracking both technically achievable and morally urgent.