# EU Project/Funding Databases — API & Data Access Report

## Running the synchronizers

All commands write parquet files to the configured S3-compatible filesystem.
Set the `TOKENS_AWS_*` variables in `.env` first, then run a source with:

```bash
uv run datasync tokens biodiversa run
uv run datasync tokens cordis run
uv run datasync tokens erc run
uv run datasync tokens keep run
uv run datasync tokens life run
uv run datasync tokens eurlex run
uv run datasync tokens eupublications run
```

Source-specific credentials and endpoint overrides are listed in
`.env.example`. The KEEP.eu API requires an API key. CORDIS, ERC, and
Biodiversa+ are public. LIFE uses an undocumented public API and may change
without notice.

## TODO

### EUR-Lex

- [x] Include EUR-Lex documents through the EUR-Lex API.
- [x] Store complete document text because the records do not contain an
  abstract.
- [x] Limit records to directory 15: Environment, consumers and health
  protection.
- [x] Subset records by relevant EuroVoc keywords.

### EU Open Publications

- [x] Include EU Open Publications through the
  [CELLAR SPARQL endpoint](https://publications.europa.eu/webapi/rdf/sparql).
- [x] Download report metadata, identifiers, dates, access rights, and record
  URIs.
- [ ] Review the downloaded report schema and determine which fields should be
  exposed to the TOKENS app.

### ERC

- [x] Compare ERC with CORDIS: ERC-funded projects are CORDIS project rows
  with an `ERC-` funding scheme.
- [x] Treat ERC as a source of principal investigator information only, rather
  than as a separate project dataset.

### CORDIS

- [x] Include `euro_sci_voc_title` values from `euro_sci_voc.parquet` so
  projects can be subset by EuroSciVoc keywords.
- [x] Derive coordinator, partners, and countries from `organization.parquet`.

### Biodiversa+

- [x] Support filtering projects to `source_acronym == "BiodivERsA"`.
- [x] Include the project URL and keywords when provided by the source.
- [ ] Scrape the funded-project pages to fill missing descriptions, budgets,
  dates, statuses, coordinators, partners, countries, and missing URLs.
- [x] Document that the remaining Biodiversa+ parquet tables are independent
  CERIF reference datasets and are not linked to research projects by ID.

### KEEP.eu

- [x] Use `open_data__projects__themes.parquet` to filter relevant projects
  and classify research projects from the `Scientific cooperation` theme.
- [x] Use `open_data__projects__partnerships.parquet` for coordinator,
  partners, and countries.

### LIFE

- [x] Confirm that the public API does not provide a free-text
  abstract/description; use keywords and themes as alternatives.
- [x] Include coordinator, partners, and normalized English country names from
  the richer export endpoint.

Research into programmatic access options for: CORDIS, KEEP.eu, ERC grants, LIFE public database, and Biodiversa+.

## Consuming this data (notes for the TOKENS app)

Guidance from building the TOKENS Streamlit app against this data, in
response to a data-quality review from the app's original requester:

- **ERC is redundant as a standalone "projects" source.** `erc.py` only
  fetches the CORDIS ERC Principal Investigators list (name, org, funding
  scheme, per `project_id`) -- it has no project-level fields of its own.
  ERC-funded projects are simply CORDIS `project` rows where
  `funding_scheme LIKE 'ERC-%'`. Join `erc.principal_investigators` onto
  `cordis.project` by `project_id` only if you need PI names; don't treat
  `erc` as a separate project list.
- **CORDIS EuroSciVoc keywords**: `cordis.euro_sci_voc` has one row per
  `(project_id, euro_sci_voc_code)` with an `euro_sci_voc_title` column.
  Aggregate with `GROUP BY project_id` + `string_agg`/`GROUP_CONCAT` to get
  a semicolon-joined "EuroSciVoc" column per project.
- **CORDIS coordinator/partners/countries**: `cordis.organization` has one
  row per `(project_id, organisation)` with a `role` column (`coordinator`,
  `participant`, `thirdParty`, `partner`, `associatedPartner`,
  `internationalPartner`). Pivot on `role == 'coordinator'` for the
  coordinator name, and aggregate the rest for partners/countries.
- **Biodiversa+ `research_projects`** is fetched from the live
  `/es/projects/_msearch` index (see §5 below), not the CSV export, because
  the CSV export flattens each project's partner institutions
  (`orgunits`) and funding sources (`fundings`) into a fixed 5-slot window,
  silently dropping anything beyond that. The ES-backed sync instead
  produces normal dlt child tables `research_projects__orgunits` and
  `research_projects__fundings` with the full, unbounded lists, plus a
  `url` field (populated for some projects, not all -- there is no
  complete project-URL field in the underlying CRIS for the rest) and a
  `keywords` field not present in the CSV export at all. Filter to
  `source_acronym == 'BiodivERsA'` to isolate the BiodivERsA-funded subset
  (216 of 13,919 projects as of this writing) -- the other funders/sources
  present are each EU member state's own national research council (not
  linked to BiodivERsA). Roughly 40% of BiodivERsA rows are still missing
  `abstract`/`startdate`/`enddate`/`amount`/orgunits in the source data
  itself (not a sync limitation) -- filling those gaps would require
  scraping `biodiversa.eu/research-funding/funded-projects/`, which is not
  implemented here.
- **Biodiversa+ reference tables** (`research_organisations`,
  `funding_agencies`, `funding_programmes`, `research_infrastructures`) are
  *not* linked to `research_projects` by ID -- they're independent
  CERIF-standard reference lists spanning all of Biodiversa's national
  funders, not just BiodivERsA, and `research_infrastructures` is unrelated
  to projects entirely (a directory of shared research infrastructure).
- **KEEP.eu themes/type split**: `keep.open_data__projects__themes` (one
  row per `(project, theme value)`) is exactly the field to use for a
  research-vs-conservation split -- `"Scientific cooperation"` is one of
  the 42 theme values. `keep.open_data__projects__partnerships` has
  `partner_name`, `type` (`lead`/`partner`/`associate`), `country__name`,
  `country__iso_code` per project -- `type == 'lead'` is the coordinator.
- **LIFE** has no free-text abstract/description anywhere in the public
  API (confirmed directly against `/dissemination/search/excel`, the
  richest available endpoint) -- `themes`/`keywords` are the closest
  substitute. As of this change, `life.py` uses that `/excel` endpoint
  instead of the plainer `/dissemination/search`, which adds `themes`,
  `keywords`, `legislatives`, `habitats`, `species`, `country`,
  `startDate`/`endDate`, `totalBudget`/`ecContribution`, and a derived
  `coordinator`/`partners` split from `participantNames`+
  `participantTypes`. `country` is also normalized to an English name +
  ISO code (the raw API value is the EC's own multi-lingual country label,
  e.g. `"Ellas"`, `"Österreich"`) -- see `_LIFE_COUNTRY_NAMES` and the
  `country_original` column.
- **EUR-Lex scope**: despite the name, the legacy `EURLex_docs.csv`
  bundled with the R Shiny app has only ~1,659 real rows, not the
  ~408,000 line count `wc -l` reports on it (each row's `content` field
  contains embedded newlines, since it's the full document text). The
  current SPARQL scope (in-force, directory 15 minus 15070000, restricted
  to the agricultural/environmental EuroVoc branches) matches ~2,500-2,900
  documents and is a superset of the legacy file's apparent scope, not an
  incomplete subset of it as row counts might otherwise suggest.
  `eurlex.py` now fetches documents concurrently (`EURLEX_MAX_WORKERS`
  threads) with retry/backoff, since each document needs 2-3 sequential
  CELLAR requests and a full sequential run would take hours.

## Summary Table

| Source | API? | **How the data is obtained** | Auth | Formats |
| --- | --- | --- | --- | --- |
| **CORDIS** | ✅ SPARQL + bulk | **Download the official H2020 and Horizon Europe CSV ZIPs, then stream and parse each CSV table** | None | CSV/JSON/XML/XLSX/RDF |
| **KEEP.eu** | ✅ REST (launched Nov 2024) | **Call `keep.eu/api/open-data` once with an API key and optional filters** | API key (email request) | JSON |
| **ERC grants** | ✅ via CORDIS | **Read ERC projects from the CORDIS bulk tables; download the dedicated ERC Principal Investigators XLSX separately** | None | CSV/JSON/XML/XLSX |
| **LIFE** | ⚠️ Unofficial REST | **POST the advanced-search form for paginated projects; GET the public reference-data endpoints** | None | JSON/XLSX |
| **Biodiversa+** | ⚠️ Undocumented | **Request the full CSV export for each database section using the Elasticsearch-backed download endpoint** | None | CSV/XLSX/ODS |
| **EUR-Lex** | ✅ CELLAR SPARQL | **Fetch in-force environmental policy documents, EuroVoc labels, English titles, and full text from CELLAR** | None | SPARQL JSON/RDF/XML |
| **EU Open Publications** | ✅ CELLAR SPARQL | **Fetch report metadata, identifiers, dates, access rights, and record URIs** | None | SPARQL JSON/RDF/XML |



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

- Access via the same CORDIS SPARQL endpoint or bulk ZIPs (filter by funding scheme, e.g. `ERC-STG`, `ERC-ADG`, `ERC-COG`, `ERC-SyG`) — already covered by `cordis.py`'s `funding_scheme` column.
- **Dedicated file**: ERC Principal Investigators list —
  `https://cordis.europa.eu/data/cordis-h2020-erc-pi.xlsx`
- Facet filtering also available via the CORDIS search UI (`cordis.europa.eu/search`).

**Auth:** None. **Licence:** Same as CORDIS (open reuse, attribution).

Implemented in [`erc.py`](erc.py) (fetches the dedicated Principal Investigators XLSX; the ERC project records themselves come from `datasync tokens cordis run`).

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
- **`/dissemination/search` vs `/dissemination/search/excel`:** both accept the same POST payload and return the same total project count, but `/excel` (used by `life.py`) returns a much richer per-project record -- `themes`, `keywords`, `legislatives`, `habitats`, `species`, `country`, `startDate`/`endDate`, `totalBudget`/`ecContribution`, `beneficiaryName`/`beneficiaryAddress`, and `participantNames`+`participantTypes` (the full coordinator/partner list) -- vs. just 8 sparse fields from the plain `/search` endpoint. Neither endpoint has ever included a free-text abstract/description field.
- **Licence:** Not explicitly stated; standard EC reuse (Decision 2011/833/EU) likely applies. Contact: `CINEA-LIFE-ENQUIRIES@ec.europa.eu`.

---

## 5. Biodiversa+ (biodiversa.eu)

- Marketing/info site: `biodiversa.eu` (funded-projects pages, database description at `biodiversa.eu/databases/`)
- Actual CRIS database (CERIF standard): `https://databases.biodiversa.eu/`
  - Sections (all under `/databases/`): `research-projects` (~13,919 projects), `research-organisations`, `funding-agencies`, `funding-programmes`, `research-infrastructures` (55 entries)

**Findings:**

- The SPA has no documented REST/SPARQL API, but its "Download" button on each section calls an **unauthenticated, undocumented export endpoint** backed by Elasticsearch:
  `GET https://databases.biodiversa.eu/databases/{section}/download?query={elasticsearch query}&format={csv|xlsx|ods}`
- Passing `query={"match_all":{}}` returns the full section in one request — no pagination, auth, or registration needed. Used here for the four reference sections (`research-organisations`, `funding-agencies`, `funding-programmes`, `research-infrastructures`).
- The SPA also queries `/es/projects/_msearch` directly (a proxied Elasticsearch endpoint, confirmed to be the *only* index exposed this way -- `/es/{section}/_msearch` 404s for every other section) for search-as-you-type filtering. `research_projects` is fetched from this endpoint instead of its CSV/download counterpart: the raw ES documents carry the full, unbounded `orgunits`/`fundings` lists (the CSV export truncates these to a fixed 5-column window) plus a `url` and `keywords` field the CSV export omits entirely. Pagination is plain `from`/`size` (confirmed to work past the default 10,000-document Elasticsearch window on this cluster).
- Implemented in [`biodiversa.py`](biodiversa.py).

## 6. EUR-Lex policy scope

The EUR-Lex source follows the data contract used by the TOKENS Shiny app. It
selects in-force legal resources in directory 15, excludes directory 15070000,
and restricts EuroVoc to the current agricultural-policy and environmental-
policy category hierarchies. Each output row contains `work`, `celex`,
`dateforce`, `eurovoc`, `title`, and `content`, matching `EURLex_docs.csv`.

English document text is retrieved from CELLAR `fmx4` manifestations. The
source supports both downloadable ZIP packages and direct XML items because
CELLAR uses both formats for different document vintages.

---

## Recommendation for Building an Aggregator

There is **no single unified API** across these five sources. A practical approach is to build one adapter per source and normalize into a common schema (e.g. project ID, title, funding programme, budget, partners/organisations, country, start/end dates):

1. **CORDIS** (+ ERC) → query via SPARQL endpoint, or download/parse the bulk JSON/CSV ZIPs. No auth needed. Best-supported source.
2. **KEEP.eu** → single authenticated JSON call via `keep.eu/api/open-data`, filtered by `period`/`ids` as needed. Requires requesting an API key by email first.
3. **LIFE** → POST requests to the search/excel endpoints, or start with the simpler reference-list GET endpoints; expect to reverse-engineer the Kendo grid payload for full search.
4. **Biodiversa+** → GET requests to the undocumented `/databases/{section}/download` export endpoint with `query={"match_all":{}}`. No auth needed; fully automatable.

Priority order for automation effort vs. payoff: **CORDIS/ERC and Biodiversa+ (easiest, no auth) → KEEP.eu (easy once key granted) → LIFE (moderate, undocumented)**.
