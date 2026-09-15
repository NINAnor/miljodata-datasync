# EU Project/Funding Databases — API & Data Access Report

Research into programmatic access options for: CORDIS, KEEP.eu, ERC grants, LIFE public database, and Biodiversa+.

## Summary Table

| Source | API? | Access Method | Auth | Formats |
| --- | --- | --- | --- | --- |
| **CORDIS** | ✅ SPARQL + bulk | SPARQL endpoint or ZIP downloads | None | CSV/JSON/XML/XLSX/RDF |
| **KEEP.eu** | ✅ REST (launched Nov 2024) | `keep.eu/api/open-data` | API key (email request) | JSON |
| **ERC grants** | ✅ via CORDIS | Same as CORDIS + dedicated ERC file | None | CSV/JSON/XML/XLSX |
| **LIFE** | ⚠️ Unofficial REST | `webgate.ec.europa.eu/life/publicWebsite/api/rest` | None | JSON/XLSX |
| **Biodiversa+** | ⚠️ Undocumented | `databases.biodiversa.eu/databases/{section}/download` | None | CSV/XLSX/ODS |

---

## 1. CORDIS (cordis.europa.eu/projects)

EU research projects database (FP1 → Horizon Europe). No traditional REST API for filtered project queries, but strong alternatives:

- **SPARQL endpoint** (best for flexible querying):
  `https://cordis.europa.eu/datalab/sparql-endpoint`
  Returns RDF / SPARQL result sets (JSON, XML, CSV depending on query).

- **Bulk ZIP downloads** (best for full datasets), per framework programme:
  - `https://cordis.europa.eu/data/cordis-h2020projects-{csv,json,xml,xlsx}.zip`
  - `https://cordis.europa.eu/data/cordis-HORIZONprojects-{csv,json,xml,xlsx}.zip`
  - Similar files exist for FP7, FP6, etc.
  - Reference/lookup data: `cordisref-data` dataset (funding schemes, country codes, org types).

- **EU Open Data Portal listing**:
  `https://data.europa.eu/data/datasets?query=CORDIS&locale=en&publisher=Publications+Office+of+the+European+Union`
  Dataset IDs include `cordish2020projects`, `cordisfp7projects`, `cordis-EU-research-projects-under-horizon-europe-2021-2027`.

- **Search UI with export** (no login needed for basic use):
  `https://cordis.europa.eu/search` — supports Boolean AND/OR/NOT, advanced "edit query" mode, and export to XML/CSV/JSON. EU Login only required for saved searches, alerts, or full extractions.

**Auth:** None for querying/downloading.
**Update frequency:** Bulk datasets regenerated monthly (can lag the live site).
**Licence:** CC BY 4.0 (attribution required); reuse also covered by EC Decision 2011/833/EU.

---

## 2. ERC (European Research Council) Grants

ERC-funded projects are embedded within the CORDIS H2020 and Horizon Europe datasets — there's no separate ERC-only system.

- Access via the same CORDIS SPARQL endpoint or bulk ZIPs (filter by funding scheme, e.g. `ERC-STG`, `ERC-ADG`, `ERC-COG`, `ERC-SyG`).
- **Dedicated file**: ERC Principal Investigators list —
  `https://cordis.europa.eu/data/cordis-h2020-erc-pi.xlsx`
- Facet filtering also available via the CORDIS search UI (`cordis.europa.eu/search`).

**Auth:** None. **Licence:** Same as CORDIS (open reuse, attribution).

---

## 3. KEEP.eu (keep.eu/projects/) — Interreg / territorial cooperation

KEEP.eu (managed by the Interact Programme) launched an **official Open Data REST API in November 2024**.

- **Endpoint:** `https://keep.eu/api/open-data?key=YOUR_API_KEY`
- **Getting a key:** Register at `https://keep.eu/register/` (or use Interact collaboration-platform credentials), then email **keep.support@interact.eu** to request access. Granted case-by-case.
- **Data model:** Hierarchical — Programmes → Projects → Partnerships. Structure reference PDF:
  `https://keep.eu/wp-content/uploads/2025/06/opendata_structure_20250624.pdf`

**Query parameters (combinable):**

- `ids=` — comma-separated programme IDs (found in each programme's URL under `keep.eu/programmes/`)
- `period=` — `1` (`2000-2006`), `2` (`2007-2013`), `3` (`2014-2020`), or `4` (`2021-2027`); full period values are also accepted
- `onlyprogramme=true` — return only programme-level data (excludes projects/partnerships)
- `callsstatus=` — `ongoing`, `future`, or `both`

Example: `https://keep.eu/api/open-data?key=KEY&ids=ID_1,ID2&period=2021-2027`

**Scope:** ~32,000+ Interreg projects since 2000, ~390 programmes, ~153,000 partnerships. Calling with just the key returns all data (effectively a bulk export).

**Auth:** API key required. **Format:** JSON only. **Licence:** Attribution required (credit keep.eu, link back, note changes) + Terms of Use; access revocable for misuse.

> Note: `thekeep.info` is an unrelated US archival aggregator — not to be confused with `keep.eu`.

---

## 4. LIFE Public Database

The EU LIFE programme (environment/climate funding) database lives at:
`https://webgate.ec.europa.eu/life/publicWebsite/`

No dataset on data.europa.eu. The web app is backed by an **unofficial, undocumented REST API**.

**Base URL:** `https://webgate.ec.europa.eu/life/publicWebsite/api/rest`

| Endpoint | Method | Returns |
| --- | --- | --- |
| `/priorityArea/list` | GET | Priority areas (BIO, Environment, Climate…) |
| `/country/list` | GET | Countries |
| `/theme/list` | GET | Themes |
| `/keyword/list` | GET | Keywords |
| `/legislative/list` | GET | Legislation references |
| `/beneficiaryType/list` | GET | Beneficiary types |
| `/habitat/list/type/{type}/{search}` | GET | Habitat types |
| `/species/list/{search}` | GET | Species lookup |
| `/nat2kSite/...`, `/nutsCode/...` | GET | Natura 2000 sites, NUTS regions |
| `/dissemination/search` | POST (JSON body) | Project/document search results |
| `/dissemination/search/excel` | POST | Export search results as XLSX |

- **Auth:** None (public).
- **Formats:** JSON (reference/search), XLSX (export).
- **Filtering:** Priority area, country, theme, keyword, legislation, habitat, species, Natura 2000 site, NUTS region, beneficiary type. Advanced search UI at `.../search/advanced`.
- **Caveat:** `/dissemination/search` requires the full Kendo-grid filter/paging JSON payload used internally by the web app — minimal payloads return HTTP 500. The reference-list endpoints and Excel export are the most reliable programmatic entry points.
- **Licence:** Not explicitly stated; standard EC reuse (Decision 2011/833/EU) likely applies. Contact: `CINEA-LIFE-ENQUIRIES@ec.europa.eu`.

---

## 5. Biodiversa+ (biodiversa.eu)

- Marketing/info site: `biodiversa.eu` (funded-projects pages, database description at `biodiversa.eu/databases/`)
- Actual CRIS database (CERIF standard): `https://databases.biodiversa.eu/`
  - Sections (all under `/databases/`): `research-projects` (~13,919 projects), `research-organisations`, `funding-agencies`, `funding-programmes`, `research-infrastructures` (55 entries)

**Findings:**

- The SPA has no documented REST/SPARQL API, but its "Download" button on each section calls an **unauthenticated, undocumented export endpoint** backed by Elasticsearch:
  `GET https://databases.biodiversa.eu/databases/{section}/download?query={elasticsearch query}&format={csv|xlsx|ods}`
- Passing `query={"match_all":{}}` returns the full section in one request — no pagination, auth, or registration needed. Verified working for all five sections above.
- The SPA also queries `/es/projects/_msearch` directly (a proxied Elasticsearch endpoint) for search-as-you-type filtering, but the `/download` export is the simpler bulk-access path.
- Implemented in [`biodiversa.py`](biodiversa.py).

---

## Recommendation for Building an Aggregator

There is **no single unified API** across these five sources. A practical approach is to build one adapter per source and normalize into a common schema (e.g. project ID, title, funding programme, budget, partners/organisations, country, start/end dates):

1. **CORDIS** (+ ERC) → query via SPARQL endpoint, or download/parse the bulk JSON/CSV ZIPs. No auth needed. Best-supported source.
2. **KEEP.eu** → single authenticated JSON call via `keep.eu/api/open-data`, filtered by `period`/`ids` as needed. Requires requesting an API key by email first.
3. **LIFE** → POST requests to the search/excel endpoints, or start with the simpler reference-list GET endpoints; expect to reverse-engineer the Kendo grid payload for full search.
4. **Biodiversa+** → GET requests to the undocumented `/databases/{section}/download` export endpoint with `query={"match_all":{}}`. No auth needed; fully automatable.

Priority order for automation effort vs. payoff: **CORDIS/ERC and Biodiversa+ (easiest, no auth) → KEEP.eu (easy once key granted) → LIFE (moderate, undocumented)**.
