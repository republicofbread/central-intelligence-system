# FCL BLOOMTECH LIMITED — Central Intelligence System (CIS)
**Baseline Production Foundation & Multi-Agent Intelligence Substrate (`DMR-FCL-2026-001`)**

[![Live Web Application](https://img.shields.io/badge/Live%20Web%20App-Open%20Access-0284c7?style=for-the-badge&logo=google-chrome&logoColor=white)](https://republicofbread.github.io/central-intelligence-system/)
[![GitHub Pages Status](https://img.shields.io/badge/GitHub%20Pages-Active%20%E2%9C%93-10b981?style=for-the-badge&logo=github&logoColor=white)](https://republicofbread.github.io/central-intelligence-system/)
[![FCL Security Clearance](https://img.shields.io/badge/RBAC-Level%204%20Governance-6366f1?style=for-the-badge)](https://republicofbread.github.io/central-intelligence-system/?recipient=keren_obara&key=KO-2026-FCL)

---

## 🌐 Live Application Links (Hosted on GitHub)

| Access Type | Description | Live Direct URL |
| :--- | :--- | :--- |
| **Universal Shareable Link** *(Open Access)* | **Open to anyone with the link.** Instant access to all 6 intelligence domains, interactive satellite Leaflet map, NASA/Copernicus weather telemetry, 5 live free dataset cards, 12-agent orchestration trace, and 19-page institutional reporting on any device. | **[Launch Web Application →](https://republicofbread.github.io/central-intelligence-system/)** |
| **Dedicated Lakehouse Backend Portal** | **Organized administrative CMS & Lakehouse dashboard.** Live CRUD tables (`silver_districts`, `silver_loans`, `silver_documents`), **20 Free Web API Connectors**, **12 Specialist AI Agents Management**, interactive SQL runner, and REST API reference. | **[Launch Backend Portal →](https://republicofbread.github.io/central-intelligence-system/backend.html)** |
| **Pre-Authenticated Recipient Link** | Direct access pre-authenticated for **Keren Obara (KO)** with Senior FCL Analyst governance sign-off privileges (`KO-2026-FCL`). | **[Launch as Keren Obara →](https://republicofbread.github.io/central-intelligence-system/?recipient=keren_obara&key=KO-2026-FCL)** |
| **GitHub Repository** | Source code, SQLite Medallion Lakehouse schema, and Docker deployment assets under `republicofbread`. | **[github.com/republicofbread/central-intelligence-system](https://github.com/republicofbread/central-intelligence-system)** |

---

## 1. Architecture Overview (5 Planes)
Per the **FCL BLOOMTECH Central Intelligence System Developer Response (Sections 1–34)** and **Demani Technical Proposal (`DMR-FCL-2026-001`)**, this platform is engineered as a modular, provider-neutral data-to-intelligence operating system underneath the three-panel **FCL Intelligence Canvas**:

1. **Plane 1 — Data Plane (`fcl_cis_lakehouse.db` Medallion Lakehouse + 20 Free Web API Connectors)**:
   - **Bronze Layer**: Immutable ingestion of source documents, STAC 1.1.0 scene payloads, NASA POWER agroclimatology series, ISRIC SoilGrids REST profiles, and SHA-256 checksums.
   - **Silver Layer**: Validated, normalized tables (`silver_districts`, `silver_loans`, `silver_documents`, `connectors_registry`, `silver_soil_profiles`, `silver_chirps_climatology`, `silver_firms_hotspots`, `silver_wapor_metrics`, `silver_worldpop_demographics`) with strict database-level tenant isolation (`tenant_id`).
   - **Gold Layer**: Curated vegetation change indicators ($\Delta\text{NDVI}$), standardized precipitation anomalies (SPI-3), actual evapotranspiration (AET), year-on-year crop productivity rankings, and calibrated credit risk scores.
2. **Plane 2 — Intelligence Plane**:
   - **Hybrid RAG**: Vector + keyword + metadata/tenant filtering with automatic **cross-document conflict detection** (surfacing policy thresholds vs field verification surveys).
   - **Deterministic SQL & Quantitative Engine**: Executes real SQL aggregations and returns exact numbers + SQL query provenance.
   - **Deterministic GeoAI Engine**: Performs STAC catalog search, cloud filtering, Sentinel-1 SAR fallback, and 90-day zonal $\Delta\text{NDVI}$ calculation with explicit **Satellite Acquisition Timestamp vs Platform Processing Timestamp** disclosure.
   - **Institutional 19-Page Dossier Generator**: Generates official institutional reports compliant with Kenya Meteorological Service Authority (KMD) seasonal outlook standards (`Ref: KMD/FCST/05-2026/SO/03`).
3. **Plane 3 — Agent Plane (Router + 12 Specialist AI Agents)**:
   - `1. Router (Orchestrator-Prime)`: Intent parsing, RBAC validation, tenant isolation, dynamic agent DAG synthesis.
   - `2. GeoSpatial-Sentinel-v4`: Sentinel-2 L2A optical and Sentinel-1 C-Band SAR processing, cloud masking, zonal NDVI.
   - `3. AgroClim-ERA5`: 90-day precipitation series, NOAA 10-year station baselines, ground synoptic calibration.
   - `4. ISRIC Soil & Agronomy Agent`: Soil physical-chemical profiling (pH, SOC, clay/sand/silt, CEC) at 250m resolution.
   - `5. Lakehouse-SQL-Synthesizer`: Deterministic SQL translation, PostGIS spatial joins, tenant-isolated queries.
   - `6. DocuParse-Vision-Pro`: Enterprise document RAG, table extraction, multi-document policy vs field discrepancy surfacing.
   - `7. AgriScore-XGBoost-v3`: Gradient-boosted credit default risk scoring, loan portfolio stress testing, collateral discounts.
   - `8. Audit-Governance-v2`: 5-level source citation verification, mathematical checks, immutable SHA-256 audit logging.
   - `9. AgroMind-Pheno-v1`: Crop phenological stage progression (BBCH scale), thermal accumulation (GDD), wilting onset alerts.
   - `10. HydroSentinel-v2`: GloFAS 30-day river discharge modeling, SAR flood extent mapping, basin inundation alerts.
   - `11. AgriClaim-SmartAudit`: Parametric crop insurance triggers, claim payout quantification, structured debt restructuring.
   - `12. EAC-CorridorLogistics`: Northern & Central trade corridors, border dwelling times, and regional commodity price arbitrage.
   - `13. GEDSI-SovereignInclusion`: Gender equality, disability and smallholder vulnerability indexing, SDG-2/8 tracking.
4. **Plane 4 — Experience Plane (The 3-Panel Intelligence Canvas)**:
   - **Left Panel — Domain Navigator**: `+ New Intelligence`, `🏠 Canvas Home`, 6 Intelligence Domains (`Satellite Intelligence`, `Climate Intelligence`, `Agriculture`, `Finance`, `Documents`, `Analytics`), Workspaces, Recent Sessions, and `⌘K` Command Palette.
   - **Centre Panel — Working Canvas**: Seamlessly navigates between the reference Welcome state and the full interactive Results Canvas featuring an **Interactive Satellite/GIS Map with a Bottom Temporal Comparison Slider (`Jul 2026` ⟷ `Oct 2026`)**, **5-Card Free Telemetry Strip (Soil pH/SOC, CHIRPS SPI, NASA FIRMS, FAO WaPOR, WorldPop)**, 12-Agent trace inspector, quantitative charts/tables, and secure file ingestion.
   - **Right Panel — Evidence Panel**: Displays clickable Evidence Cards for Sources 1 through 8 with a full **Bronze → Silver → Gold Data Lineage Inspector**.
5. **Plane 5 — Governance Plane**:
   - Database-level Multi-Tenant RBAC isolation (`fcl_internal`, `client_centenary`, `client_min_water`).
   - Pre-retrieval **Prompt Injection & Privilege Escalation Firewall** (OWASP LLM01 / NIST AI RMF).
   - **Governed Self-Learning Loop**: Captures human feedback and telemetry into a sandbox candidate registry with offline benchmark evaluation and one-click Approval or Rollback.
   - **Tamper-Evident SHA-256 Hash-Chained Audit Log**.

---

## 2. Connected Free Web APIs & Catalogs (100% Free & Open Access)
1. **Element84 Earth Search STAC API v1** (`https://earth-search.aws.element84.com/v1/search`) — Sentinel-2 L2A COG & Sentinel-1 GRD SAR
2. **Microsoft Planetary Computer STAC API v1** (`https://planetarycomputer.microsoft.com/api/stac/v1/search`) — USGS Landsat-9 & ESA WorldCover
3. **NASA POWER Agroclimatology API** (`https://power.larc.nasa.gov/api/temporal/daily/point`) — Daily Precipitation, Temperature, Root-Zone Soil Wetness
4. **NASA EONET v3 Natural Events API** (`https://eonet.gsfc.nasa.gov/api/v3/events`) — Active Wildfires, Floods & Severe Storms
5. **NASA GIBS WMTS & CMR-STAC** (`https://gibs.earthdata.nasa.gov/wmts/epsg3857/best`) — Global Imagery Browse Services (MODIS / VIIRS)
6. **Open-Meteo Weather, Evapotranspiration & GloFAS Flood API** (`https://api.open-meteo.com/v1/forecast`)
7. **NOAA NCEI Global Historical Climate Network** (`https://www.ncei.noaa.gov/access/services/data/v1`) — 10-Year Station Baselines
8. **Iowa Environmental Mesonet (IEM) Archive** (`https://mesonet.agron.iastate.edu/geojson/network/KE__ASOS.geojson`) — East Africa Synoptic Stations (`HKJK`, `HUGU`, `HUEN`, `HKKS`)
9. **World Bank Open Data API** (`https://api.worldbank.org/v2/country/UGA;KEN;TZA/indicator/AG.YLD.CREL.KG`)
10. **Atlas GIS Cloud Project (`FAyqMJBcNHRsuEHV8Iit`)** (`https://app.atlas.co/projects/FAyqMJBcNHRsuEHV8Iit?loc=32.2732%2C1.5064%2C5.1213z`)
11. **OpenStreetMap Nominatim & Overpass API** (`https://nominatim.openstreetmap.org/search`)
12. **FCL BLOOMTECH Enterprise Knowledge & Gold Lakehouse**
13. **ISRIC SoilGrids 250m REST API** (`https://rest.isric.org/soilgrids/v2.0/properties/query`) — Soil pH, SOC, Clay/Sand/Silt fractions
14. **CHIRPS 40-Yr African Rainfall & Anomaly Catalog** (`https://data.chc.ucsb.edu/products/CHIRPS-2.0/`) — UCSB / FEWS NET Climatology
15. **NASA FIRMS Near Real-Time Active Fire API** (`https://firms.modaps.eosdis.nasa.gov/api/area/`) — Thermal Anomalies (VIIRS/MODIS)
16. **FAO WaPOR 3.0 Water Productivity API** (`https://io.apps.fao.org/gismgr/api/v1/swagger-ui.html`) — Actual Evapotranspiration (AET) & Water Deficit
17. **ESA WorldCover 10m Land Use Classification** (`https://esa-worldcover.org/en/data-access`) — Parcel-level Crop vs Forest Cover
18. **GloFAS Copernicus Global Flood Early Warning** (`https://globalfloods.eu/`) — 30-day river discharge hydrographs
19. **Planet Labs NICFI 4.7m Tropical Basemaps** (`https://api.planet.com/basemaps/v1/mosaics`) — High-resolution optical basemaps
20. **WorldPop 100m Demographics & Settlements** (`https://hub.worldpop.org/geodata/summary?id=49786`) — Smallholder population & gender metrics

---

## 3. Running Locally & Docker
```bash
# Option 1: Native Python
python3 server.py

# Option 2: Docker Compose
docker-compose up --build
```
Once started, open `http://localhost:8095` in your browser.
