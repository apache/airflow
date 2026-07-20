<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Apache Airflow Provider Registry

A searchable catalog of all Apache Airflow providers and their modules (operators,
hooks, sensors, triggers, transfers, and more). Built with [Eleventy](https://www.11ty.dev/)
and deployed to `airflow.apache.org/registry/`.

## Quick Start

### Prerequisites

- Python 3.10+ (for metadata extraction)
- Node.js 20+ and pnpm 9+
- `pyyaml` Python package (`uv pip install pyyaml`)

### Local Development

```bash
# 1. Extract metadata from provider.yaml files into JSON
uv run python dev/registry/extract_metadata.py

# 2. Install Node.js dependencies
cd registry
pnpm install

# 3. Start dev server (http://localhost:8080)
pnpm dev
```

### Breeze Command

The full registry data extraction (metadata, parameters, and connections) is available
as a breeze subcommand:

```bash
breeze registry extract-data                          # Extract all registry data
breeze registry extract-data --python 3.12            # With a specific Python version
breeze registry extract-data --provider amazon        # Extract only one provider (incremental)
```

This runs inside the breeze CI container where all providers are installed. It is
the same command used by CI in `registry-build.yml`.

The `dev` script sets `REGISTRY_PATH_PREFIX=/` so links work at the root during local
development. In production the prefix defaults to `/registry/`.

## Architecture

### Data Pipeline

```
registry_tools/types.py      ← Single source of truth for module type definitions
        │
        ├─── generate_types_json.py  → registry/src/_data/types.json (for frontend)
        │
provider.yaml files (providers/*/provider.yaml)
        │
        ▼
extract_metadata.py          ← Parses YAML, fetches PyPI stats, resolves logos
        │                       → providers.json
        │
extract_parameters.py        ← Runtime class discovery + parameter extraction (breeze)
        │                       → modules.json + parameters.json
        ▼
registry/src/_data/
  ├── providers.json         ← Provider metadata (name, versions, downloads, lifecycle, ...)
  ├── modules.json           ← Individual modules (operators, hooks, sensors, ...)
  ├── types.json             ← Module type definitions (generated from types.py)
  └── versions/{id}/{ver}/   ← Per-version metadata, parameters, connections
        │
        ▼
Eleventy build (pnpm build)  ← Generates static HTML + Pagefind search index
        │
        ▼
registry/_site/              ← Deployable static site
```

The root-level JSON files (`providers.json`, `modules.json`)
are generated artifacts and are listed in `.gitignore`. The `versions/` directory is also
gitignored. Only `exploreCategories.js`, `statsData.js`, `latestVersionData.js`, and
`providerVersions.js` are checked in because they contain hand-authored or computed logic.

### Extraction Scripts (`dev/registry/`)

**`extract_metadata.py`** (runs on host) walks every `providers/*/provider.yaml` and:

1. **Parses provider metadata** — name, description, versions, dependencies
2. **Fetches PyPI download stats** — calls `pypistats.org/api/packages/{name}/recent`
3. **Resolves logos** — checks `public/logos/` for matching images
4. **Determines AIP-95 lifecycle stage** — reads `lifecycle` from `provider.yaml`
5. **Writes `providers.json`**

**`extract_parameters.py`** (runs inside breeze) handles module discovery and parameter
extraction:

1. **Discovers modules at runtime** — imports each module listed in `provider.yaml`,
   iterates over classes with `inspect.getmembers()`, and uses `issubclass()` to
   classify them (operator, hook, sensor, trigger, etc.)
2. **Resolves documentation URLs via Sphinx inventory** — downloads `objects.inv` files
   from S3 for each provider (cached locally with 12-hour TTL), parses them to get
   canonical documentation URLs for each class. Falls back to manual URL construction
   for providers not yet published. See [Documentation URL Resolution](#documentation-url-resolution) below.
3. **Produces `modules.json`** — the full module catalog with all 11 fields (id, name,
   type, import_path, module_path, short_description, docs_url, source_url, category,
   provider_id, provider_name)
4. **Extracts `__init__` parameters** — walks the MRO and extracts parameter names,
   types, defaults, and docstrings. Writes
   `versions/{provider_id}/{version}/parameters.json`.

**`registry_contract_models.py`** defines Pydantic models that validate the shape of
every JSON payload the registry produces. Each extraction script calls
`_validate(ModelType, payload)` before writing JSON — this catches schema drift at
generation time without a separate jsonschema layer. The same models generate the
OpenAPI 3.1 spec served at `/api/openapi.json`.

**`export_registry_schemas.py`** generates `registry/schemas/openapi.json` from the
contract models. It runs automatically via `pnpm prebuild` before Eleventy builds.

**`extract_connections.py`** (runs inside breeze) reads `connection-types` from
`provider.yaml`, falling back to runtime inspection of hook
`get_connection_form_widgets()` and `get_ui_field_behaviour()`. Writes
`versions/{provider_id}/{version}/connections.json`.

### Documentation URL Resolution

The `docs_url` field in `modules.json` links each class to its Sphinx-generated API
reference page. Rather than constructing these URLs manually (which breaks if the Sphinx
output structure changes), the extractor uses **Sphinx inventory files** (`objects.inv`).

**How it works:**

1. Every Sphinx build produces an `objects.inv` file that maps every documented symbol
   to its URL. Apache publishes these for all released providers at:
   `http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/{package_name}/stable/objects.inv`

2. Before discovering modules, `extract_parameters.py` fetches inventories for all
   providers in parallel using a thread pool.

3. For each discovered class, it looks up the fully qualified name (e.g.,
   `airflow.providers.amazon.hooks.s3.S3Hook`) in the inventory. If found, the
   inventory-sourced URL is used. If not (e.g., a brand new class not yet in a published
   docs build), it falls back to manual URL construction.

**Caching:**

Inventory files are cached locally in `dev/registry/.inventory_cache/` with a 12-hour
TTL. This matches the caching strategy used by
`devel-common/src/sphinx_exts/docs_build/fetch_inventories.py`. The cache directory is
gitignored.

**Why not just construct URLs manually?**

The previous approach assembled URLs like
`{base_docs_url}/_api/{module/path}/index.html#{full.class.path}`. This is fragile:
Sphinx can change its output layout, and some classes end up in different URL structures
than expected. The inventory file is the canonical source of truth — it's produced by
the same Sphinx build that generates the docs.

### Eleventy Data Files (`src/_data/`)

| File | Type | Purpose |
|---|---|---|
| `providers.json` | Generated | All providers with metadata, sorted alphabetically |
| `modules.json` | Generated | All extracted modules (operators, hooks, etc.) |
| `types.json` | Generated | Module type definitions (from `registry_tools/types.py`) |
| `versions/` | Generated | Per-provider, per-version metadata/parameters/connections |
| `exploreCategories.js` | Checked-in | Category definitions with keyword lists for the Explore page |
| `statsData.js` | Checked-in | Computed statistics (lifecycle counts, top providers, etc.) |
| `providerVersions.js` | Checked-in | Builds the provider × version page collection |
| `latestVersionData.js` | Checked-in | Latest version parameters/connections lookup |
| `openapiSpec.js` | Checked-in | Builds OpenAPI 3.1 spec from Pydantic contract models |
| `providerVersionPayloads.js` | Checked-in | Generates `/api/providers/{id}/versions.json` payloads |

### Pages

| Page | Template | URL |
|---|---|---|
| Home | `src/index.njk` | `/` |
| All Providers | `src/providers.njk` | `/providers/` |
| Explore by Category | `src/explore.njk` | `/explore/` |
| Statistics | `src/stats.njk` | `/stats/` |
| Provider Detail | `src/provider-detail.njk` | `/providers/{id}/` (redirects to latest) |
| Provider Version | `src/provider-version.njk` | `/providers/{id}/{version}/` |
| API Explorer | `src/api-explorer.njk` | `/api-explorer/` |

### Client-Side JavaScript

| Script | Purpose |
|---|---|
| `js/provider-filters.js` | Search, lifecycle filter, category filter, sort on `/providers/` |
| `js/search.js` | Global search modal (Cmd+K) powered by Pagefind |
| `js/provider-detail.js` | Module tabs, copy-to-clipboard on provider version pages |
| `js/connection-builder.js` | Interactive connection form builder on provider detail pages |
| `js/copy-button.js` | Generic copy button utility |
| `js/theme.js` | Dark/light mode toggle |
| `js/mobile-menu.js` | Responsive navigation |

### Path Prefix / Deployment

The site is deployed under `/registry/` on `airflow.apache.org`. Eleventy's `pathPrefix`
is configured via the `REGISTRY_PATH_PREFIX` environment variable:

- **Production**: `REGISTRY_PATH_PREFIX=/registry/` (the default)
- **Local dev**: `REGISTRY_PATH_PREFIX=/` (set automatically by `pnpm dev`)

All internal links in templates use the `| url` Nunjucks filter, which prepends the
prefix. Client-side JavaScript accesses the base path via `window.__REGISTRY_BASE__`
(injected in `base.njk`).

### Search

Full-text search is powered by [Pagefind](https://pagefind.app/). During `postbuild`:

1. `scripts/build-pagefind-index.mjs` creates a Pagefind index with custom records from
   `providers.json` and `modules.json`
2. URLs in the index are prefixed with `REGISTRY_PATH_PREFIX`
3. The client loads Pagefind lazily on first search interaction (`js/search.js`)

## Key Concepts

### Provider Lifecycle (AIP-95)

Providers follow the AIP-95 lifecycle stages:

| Stage | Meaning |
|---|---|
| `incubation` | New provider, API may change |
| `production` / `stable` | Stable, recommended for use |
| `mature` | Well-established, widely adopted |
| `deprecated` | No longer maintained, consider alternatives |

The UI displays `stable` for both `production` and `mature` stages.

### Categories

The Explore page and provider filtering use categories defined in
`src/_data/exploreCategories.js`. Each category has:

- `id` — URL-safe identifier (e.g., `cloud`, `databases`, `ai-ml`)
- `name` — Display name
- `keywords` — List of substrings matched against `provider.id`
- `icon`, `color`, `description` — Visual properties

Providers are assigned to categories at build time by checking if any keyword in a
category matches (substring) the provider's ID. A provider can belong to multiple
categories.

### Homepage "New Providers" Section

The homepage has a "New Providers" section powered by dates fetched from the PyPI JSON
API during extraction. It shows providers sorted by `first_released` (the upload date
of their earliest PyPI release) descending, highlighting providers that are new to the
ecosystem.

### API Endpoints

The `src/api/` directory contains Eleventy templates that generate JSON API endpoints,
providing programmatic access to provider and module data:

- `/api/providers.json` — All providers
- `/api/modules.json` — All modules
- `/api/providers/{id}/modules.json` — Modules for a specific provider
- `/api/providers/{id}/parameters.json` — Parameters for a provider
- `/api/providers/{id}/connections.json` — Connection types
- `/api/providers/{id}/versions.json` — Deployed versions (generated by `publish_versions.py` from S3)
- `/api/providers/{id}/{version}/modules.json` — Version-specific modules
- `/api/providers/{id}/{version}/parameters.json` — Version-specific parameters
- `/api/providers/{id}/{version}/connections.json` — Version-specific connections
- `/api/openapi.json` — OpenAPI 3.1 spec (generated at build time from Pydantic contracts)

An interactive **API Explorer** at `/api-explorer/` renders the OpenAPI spec using
swagger-ui (vendored from `node_modules/swagger-ui-dist`).

## Incremental Builds

The registry supports two build modes: **full builds** (all providers) and
**per-provider incremental builds** (single provider).

### Full build

Each full CI run builds pages for only the latest version of each provider. Old
version pages persist in S3 from previous deploys.

This follows the same pattern as Airflow docs (see `publish_docs_to_s3.py` and
`packages-metadata.json`): the source of truth for which versions exist is the S3
bucket itself, not git or a stored manifest.

1. **CI extracts latest data** — `extract_metadata.py` writes `providers.json` and
   `extract_parameters.py` writes `modules.json` with all known versions (from
   `provider.yaml`), but only the latest version gets a full page built by Eleventy.
2. **S3 sync without `--delete`** — new pages are uploaded; old version pages already
   in S3 are left untouched.
3. **`publish_versions.py`** — after sync, this script lists S3 directories under
   `providers/{id}/` to discover every deployed version, then writes
   `api/providers/{id}/versions.json` with the full version list.
4. **Client-side dropdown** — `provider-detail.js` fetches `versions.json` on page
   load and replaces the static `<select>` options, so even old pages get an
   up-to-date dropdown. The statically-rendered dropdown is the fallback if the
   fetch fails.

### Per-provider incremental build

When `publish-docs-to-s3.yml` publishes provider docs (e.g., `providers-amazon/9.22.0`),
it triggers `registry-build.yml` with the provider ID. The incremental flow:

1. **Download existing data** — `providers.json` and `modules.json` are fetched from
   the current S3 bucket (`/api/providers.json`, `/api/modules.json`).
2. **Extract single provider** — `extract_metadata.py --provider amazon` extracts
   metadata and PyPI stats; `extract_parameters.py` discovers modules for only the
   specified provider.
3. **Merge** — `merge_registry_data.py` replaces the updated provider's entries in
   the downloaded JSON while keeping all other providers intact. Only global files
   (`providers.json`, `modules.json`) are merged — per-version files like
   `connections.json` and `parameters.json` are not downloaded from S3.
4. **Build site** — Eleventy builds all pages from the merged data; Pagefind indexes
   all records. Because per-version data only exists for the target provider, Eleventy
   emits empty fallback JSON for other providers' `connections.json` and
   `parameters.json` API endpoints (see **Known limitation** below).
5. **S3 sync (selective)** — the main sync excludes the entire `api/providers/`
   subtree to avoid overwriting real data with incomplete/empty stubs. A second
   sync uploads only the target provider's API files.
6. **Publish versions** — `publish_versions.py` updates `api/providers/{id}/versions.json`.

The merge script (`dev/registry/merge_registry_data.py`) handles edge cases:

- First deploy (no existing data on S3): uses the single-provider output as-is.
- Missing modules file: treated as empty.

**Known limitation**: Eleventy's pagination templates generate API files for every
provider in `providers.json`, even when per-version data (connections, parameters) only
exists for the target provider. The templates emit empty fallback JSON
(`{"connection_types":[]}`) for providers without data. The S3 sync step works around
this with `--exclude` patterns during incremental builds. A proper template-level fix
(skipping file generation) is tracked as a follow-up — `permalink: false` does not work
with Eleventy 3.x pagination templates.

To run an incremental build locally:

```bash
# Extract only amazon
breeze registry extract-data --python 3.12 --provider amazon

# If you have existing full JSON from a previous build or S3 download:
uv run dev/registry/merge_registry_data.py \
  --existing-providers /tmp/existing/providers.json \
  --existing-modules /tmp/existing/modules.json \
  --new-providers dev/registry/providers.json \
  --new-modules dev/registry/modules.json \
  --output dev/registry/

# Build site from merged data
cd registry && pnpm build
```

### Backfill (one-time)

To populate S3 with historical version pages (e.g., when setting up a new bucket),
temporarily restore the older-versions loop in `providerVersions.js` so Eleventy
builds all version pages, then:

```bash
# Extract metadata (includes all versions from provider.yaml)
uv run python dev/registry/extract_metadata.py

# Build the full site (with older-versions loop enabled)
cd registry && pnpm build

# Sync everything, then generate versions.json
aws s3 sync registry/_site/ s3://bucket/registry/ --cache-control "public, max-age=300"
breeze registry publish-versions --s3-bucket s3://bucket/registry/
```

## CI/CD

### Workflows

- **`.github/workflows/registry-build.yml`** — Reusable workflow that extracts metadata
  (host), builds a breeze CI image to run parameter/connection extraction, builds the
  Eleventy site, syncs to S3 (without `--delete`), and runs `publish_versions.py` to
  update version metadata. Supports `staging` and `live` destinations. Accepts an
  optional `provider` input for incremental builds.
- **`.github/workflows/registry-tests.yml`** — Runs extraction script unit tests on PRs
  that touch `dev/registry/`, `registry/`, or `providers/*/provider.yaml`.
- **`.github/workflows/publish-docs-to-s3.yml`** — Main docs workflow. When publishing
  provider docs, the `update-registry` job automatically triggers `registry-build.yml`
  with the provider ID for an incremental registry update.

### Manual Trigger

The registry can be rebuilt independently via `workflow_dispatch` on `registry-build.yml`.
Only designated committers can trigger manual builds. The `provider` input can be set
to run an incremental build for a specific provider (e.g., `amazon`).

### Deployment Target

The built site is synced to:

- **Staging**: `s3://staging-docs-airflow-apache-org/registry/`
- **Live**: `s3://live-docs-airflow-apache-org/registry/`

## Design Decisions

### Why runtime discovery instead of AST parsing?

Module discovery (`modules.json`) uses runtime inspection inside Breeze, where all
providers are installed. `extract_parameters.py` imports each module listed in
`provider.yaml`, iterates over its classes with `inspect.getmembers()`, and uses
`issubclass()` checks against base classes like `BaseOperator` and `BaseHook` to
classify them.

Runtime discovery is more accurate than AST-based alternatives: it resolves dynamic
class definitions, runtime-computed attributes, and complex inheritance chains that
static analysis misses. Validation showed runtime discovery found 9 classes that AST
missed (triggers and a hook) with 0 type mismatches across 1600+ modules.

Since `extract_parameters.py` already runs inside Breeze for parameter inspection, module
discovery adds no extra infrastructure cost — the same Breeze session handles both.

### Relationship to `run_provider_yaml_files_check.py`

`scripts/in_container/run_provider_yaml_files_check.py` (run by the
`check-provider-yaml-valid` pre-commit hook inside Breeze) validates that `provider.yaml`
is correct and complete: modules exist, classes are importable, and every Python file in
the `operators/`/`hooks/`/`sensors/`/`triggers/` directories is listed. This is a
correctness guarantee that `extract_parameters.py` builds on.

The distinction: `provider.yaml` lists operators/hooks/sensors/triggers/transfers/bundles
at the **module level** (e.g., `airflow.providers.amazon.operators.s3`), while the
registry needs **individual class names** within each module. Runtime discovery fills
that gap by importing each module and inspecting its members. For class-level entries
(notifications, secrets-backends, logging, executors, task-decorators), `provider.yaml`
already has the full class path and `extract_parameters.py` uses it directly.

### Why four separate scripts?

`extract_parameters.py` and `extract_connections.py` need runtime access to provider
classes (to discover modules via `issubclass()`, inspect `__init__` signatures, and call
`get_connection_form_widgets()`). They run inside Breeze where all providers are installed.
`extract_parameters.py` produces both `modules.json` (the module catalog) and per-provider
`parameters.json` files. `extract_metadata.py` and `extract_versions.py` only need
filesystem access and run on the host. This split means the CI workflow can run the fast
scripts (metadata) without spinning up Breeze, while module discovery, parameter
extraction, and connection extraction are a separate step inside Breeze.

### Why Eleventy?

Static site generators produce zero-JS pages by default. The registry works without
JavaScript — filtering and search are layered on top progressively. Eleventy has no
opinion on frontend frameworks, which keeps the dependency surface small (~30 packages in
the lockfile).

### Path prefix handling

The site deploys at `/registry/` on airflow.apache.org but runs at `/` during local dev.
Eleventy's `pathPrefix` config handles this via the `REGISTRY_PATH_PREFIX` env var.
Templates use the `| url` filter, and client-side JS reads `window.__REGISTRY_BASE__`
(injected in `base.njk`).

### Module filtering via base class inheritance

Classes are discovered by runtime `issubclass()` checks against type-specific base
classes — e.g., `BaseOperator` for operators, `BaseHook` for hooks. Since
`extract_parameters.py` runs inside Breeze where all providers are installed, Python's
MRO handles transitive inheritance natively: chains like
`S3ListOperator → AwsBaseOperator → BaseOperator` are resolved without needing to build
a cross-file inheritance map. After inheritance filtering, a post-filter skips private,
`Base*`, `Abstract*`, and `*Mixin` classes. There is no suffix-based matching.

## Adding a New Provider

No registry-specific changes are needed. When `extract_metadata.py` runs during CI, it
automatically discovers all providers under `providers/*/provider.yaml`. To ensure your
provider appears well in the registry:

1. **Complete `provider.yaml`** — include description, integrations with `how-to-guide`
   links, and logo references
2. **Add a logo** — place a PNG/SVG in `registry/public/logos/{provider-id}-{Name}.png`
3. **Write docstrings** — the extraction script uses runtime inspection to pull
   class-level docstrings for module descriptions
4. **Publish to PyPI** — download stats are fetched automatically

## Adding a New Module Type

Module types (operator, hook, sensor, etc.) are defined in a single place:
`dev/registry/registry_tools/types.py`. To add a new type (e.g., `auth_manager`):

1. Add an entry to `MODULE_TYPES` in `dev/registry/registry_tools/types.py`
2. Run `uv run python dev/registry/generate_types_json.py` to update
   `registry/src/_data/types.json` (auto-propagates to frontend templates and JS)
3. Add CSS variable `--color-auth-manager` and class `.auth-manager` in
   `src/css/tokens.css` and `src/css/main.css`
4. If runtime discovery is needed, add a base class entry to `BASE_CLASS_IMPORTS`
   in `types.py`

## Development Tips

- Run `uv run python dev/registry/extract_metadata.py` whenever provider metadata changes
- The `pnpm dev` command runs both the Eleventy build and starts a live-reload dev server
- CSS uses custom properties defined in `src/css/tokens.css` for theming
- The site works without JavaScript (progressive enhancement); filters and search are
  layered on top via `js/` scripts
