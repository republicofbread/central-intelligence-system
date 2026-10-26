# FCL BLOOMTECH LIMITED — Central Intelligence System (CIS)
**Baseline Production Foundation & Multi-Agent Intelligence Substrate (`DMR-FCL-2026-001`)**

## 1. Architecture Overview (5 Planes)
Per the **FCL BLOOMTECH Central Intelligence System Developer Response (Sections 1–34)** and **Demani Technical Proposal (`DMR-FCL-2026-001`)**, this platform is engineered as a modular, provider-neutral data-to-intelligence operating system underneath the three-panel **FCL Intelligence Canvas**:

1. **Plane 1 — Data Plane (`fcl_cis_lakehouse.db` Medallion Lakehouse + 12 Web API Connectors)**:
   - **Bronze Layer**: Immutable ingestion of source documents, STAC 1.1.0 scene payloads, NASA POWER agroclimatology series, and SHA-256 checksums.
   - **Silver Layer**: Validated, normalized tables (`silver_districts`, `silver_loans`, `silver_documents`, `connectors_registry`) with strict database-level tenant isolation (`tenant_id`).
   - **Gold Layer**: Curated vegetation change indicators ($\Delta\text{NDVI}$), standardized precipitation anomalies, year-on-year crop productivity rankings, and calibrated credit risk scores.
2. **Plane 2 — Intelligence Plane**:
   - **Hybrid RAG**: Vector + keyword + metadata/tenant filtering with automatic **cross-document conflict detection** (e.g., surfacing the threshold conflict between *Policy v4.0 Section 4.2* $\text{NDVI} \le 0.35$ and the *October 2026 Field Survey Report* $\text{NDVI} \le 0.45$).
   - **Deterministic SQL & Quantitative Engine**: Executes real SQL aggregations and returns exact numbers + SQL query provenance.
   - **Deterministic GeoAI Engine**: Performs STAC catalog search, cloud filtering, Sentinel-1 SAR fallback, and 90-day zonal $\Delta\text{NDVI}$ calculation with explicit **Satellite Acquisition Timestamp vs Platform Processing Timestamp** disclosure.
3. **Plane 3 — Agent Plane (Router + 7 Specialist Agents)**:
   - `Router / Task Classifier Agent`
   - `GeoAI & Satellite Intelligence Agent`
   - `Climate & Environmental Agent`
   - `Data Analyst Agent`
   - `Document Analyst Agent`
   - `ML / Risk Model Agent` (`AgriRisk-XGB-v2.4` & `TerraMind-EO-v1.2`)
   - `Validation & Reporting Agent`
4. **Plane 4 — Experience Plane (The 3-Panel Intelligence Canvas)**:
   - **Left Panel — Domain Navigator**: `+ New Intelligence`, 6 Intelligence Domains (`Satellite Intelligence`, `Climate Intelligence`, `Agriculture`, `Finance`, `Documents`, `Analytics`), Workspaces, Recent Sessions, and `⌘K` Command Palette.
   - **Centre Panel — Working Canvas**: Matches the reference UI design in its Welcome state and transitions to the full interactive Results Canvas featuring an **Interactive Satellite/GIS Map with a Bottom Temporal Comparison Slider (`Jul 2026` ⟷ `Oct 2026`)**, Analytical Mode trace inspector, quantitative charts/tables, and secure file ingestion.
   - **Right Panel — Evidence Panel**: Displays clickable Evidence Cards for every source with a full **Bronze → Silver → Gold Data Lineage Inspector**.
5. **Plane 5 — Governance Plane**:
   - Database-level Multi-Tenant RBAC isolation (`fcl_internal`, `client_centenary`, `client_min_water`).
   - Pre-retrieval **Prompt Injection & Privilege Escalation Firewall** (OWASP LLM01 / NIST AI RMF).
   - **Governed Self-Learning Loop**: Captures human feedback and telemetry into a sandbox candidate registry with offline benchmark evaluation and one-click Approval or Rollback.
   - **Tamper-Evident SHA-256 Hash-Chained Audit Log**.

## 2. Connected Free Web APIs & Datasets
1. **Element84 Earth Search STAC API v1** (`https://earth-search.aws.element84.com/v1/search`) — Sentinel-2 L2A COG & Sentinel-1 GRD SAR
2. **Microsoft Planetary Computer STAC API v1** (`https://planetarycomputer.microsoft.com/api/stac/v1/search`) — USGS Landsat-9 & ESA WorldCover
3. **NASA POWER Agroclimatology API** (`https://power.larc.nasa.gov/api/temporal/daily/point`) — Precipitation, Temperature, Root-Zone Soil Wetness
4. **NASA EONET v3 Natural Events API** (`https://eonet.gsfc.nasa.gov/api/v3/events`) — Active Wildfires, Floods & Severe Storms
5. **NASA GIBS WMTS & CMR-STAC** (`https://gibs.earthdata.nasa.gov/wmts/epsg3857/best`) — Global Imagery Browse Services
6. **Open-Meteo Weather, Evapotranspiration & GloFAS Flood API** (`https://api.open-meteo.com/v1/forecast`)
7. **NOAA NCEI Global Historical Climate Network** (`https://www.ncei.noaa.gov/access/services/data/v1`) — 10-Year Station Baselines
8. **Iowa Environmental Mesonet (IEM) Archive** (`https://mesonet.agron.iastate.edu/geojson/network/KE__ASOS.geojson`) — East Africa Synoptic Stations (`HKJK`, `HUGU`, `HUEN`, `HKKS`)
9. **World Bank Open Data API** (`https://api.worldbank.org/v2/country/UGA;KEN;TZA/indicator/AG.YLD.CREL.KG`)
10. **Atlas GIS Cloud Project (`FAyqMJBcNHRsuEHV8Iit`)** (`https://app.atlas.co/projects/FAyqMJBcNHRsuEHV8Iit?loc=32.2732%2C1.5064%2C5.1213z`)
11. **OpenStreetMap Nominatim & Overpass API** (`https://nominatim.openstreetmap.org/search`)
12. **FCL BLOOMTECH Enterprise Knowledge & Gold Lakehouse**

## 3. Running Locally
```bash
python3 server.py
```
Then open `http://127.0.0.1:8095` in your browser.
