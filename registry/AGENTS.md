 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Agent Guidelines for Airflow Registry

This document contains rules, patterns, and guidelines for working on the Airflow Registry project.

## Core Principles

### 0. Minimal JavaScript

- Keep JavaScript to minimum necessary
- Theme toggle only
- No frameworks (no React, Vue, etc.)
- Progressive enhancement approach

## 1. Quality Standards

- **Visual consistency**: Changes should match existing design system and styling
- **Semantic HTML**: Use proper elements and structure
- **Maintainable CSS**: Descriptive class names, leverage cascade
- **Light/Dark mode**: Full support with smooth transitions
- **Responsive**: Mobile-first, works on all screen sizes

### 2. Semantic CSS & HTML

Follow the principles from https://css-tricks.com/semantic-class-names/

#### 2.1 Class Names Describe CONTENT, Not Appearance

Class names should answer "what is this?" not "how should it look?"

**Good examples:**

- `.provider-card` - it's a card displaying provider information
- `.connection-types` - a section showing connection types
- `.dependencies` - shows dependency information
- `.stats` - displays statistical data

**Bad examples:**

- `.blue-box` - describes appearance (what if design changes to green?)
- `.flex-row` - describes layout implementation
- `.mt-4` - describes spacing value
- `.text-lg` - describes typography

**Why this matters:**

- Design changes shouldn't require HTML updates
- Code is self-documenting (you understand structure without seeing CSS)
- Multiple elements can share appearance without sharing meaning

#### 2.2 Use CSS Cascade to Avoid Redundant Prefixes

When an element only makes sense within its parent, use descendant selectors instead of prefixing class names.

**Good - leveraging cascade:**

```html
<section class="technical">
  <div class="python-version">...</div>
  <div class="connections">...</div>
</section>
```

```css
.technical { /* section styles */ }
.technical h3 { /* heading styles */ }
.technical .python-version { /* specific section */ }
.technical code { /* all code in technical section */ }
```

**Bad - redundant prefixes:**

```html
<section class="technical">
  <div class="technical-python">...</div>
  <div class="technical-connections">...</div>
</section>
```

```css
.technical { /* section styles */ }
.technical-heading { /* redundant prefix */ }
.technical-python { /* redundant prefix */ }
.technical-code { /* redundant prefix */ }
```

**Why this matters:**

- Less typing, less repetition
- Clearer hierarchy in CSS
- If `.python-version` only appears in `.technical`, the prefix adds no information

#### 2.3 Leverage Semantic HTML5 Elements

Use the right HTML element for the job. This improves accessibility, SEO, and reduces need for classes.

**Semantic elements and their uses:**

- `<header>` - Page or section header
- `<footer>` - Page or section footer
- `<nav>` - Navigation menus
- `<main>` - Main content of the page
- `<section>` - Thematic grouping of content
- `<article>` - Self-contained content
- `<aside>` - Side content/sidebars
- `<dl>`, `<dt>`, `<dd>` - Definition lists (key-value pairs)
- `<details>`, `<summary>` - Collapsible content

**Example - Statistics:**

```html
<!-- Good: Semantic -->
<dl class="stats">
  <div>
    <dt>1,000+</dt>
    <dd>Providers</dd>
  </div>
</dl>
```

```css
.stats dt { font-size: var(--text-3xl); }
.stats dd { font-size: var(--text-sm); }
```

**Example - Provider detail header:**

```html
<!-- Good: Semantic structure -->
<header class="card">
  <div class="top">
    <div class="logo">...</div>
    <div class="details">
      <h1>Amazon</h1>
      <p class="description">...</p>
    </div>
  </div>

  <div class="stats">...</div>
</header>
```

```css
.provider-detail-page header { /* styles */ }
.provider-detail-page header h1 { /* title */ }
.provider-detail-page header .stats { /* grid of stats */ }
```

#### 2.4 When to Add Classes vs Use Element Selectors

**Use element selectors when:**

- There's only one of that element in the scope
- The element's meaning is clear from context
- Example: `.provider-detail-page header h1` - there's only one main heading

**Add a class when:**

- Multiple instances need different styling
- The role isn't obvious from the element alone
- You need to target it from JavaScript
- Example: `.python-version`, `.connections`, `.dependencies` - all are `<div>` but serve different purposes

**Prefer simpler selectors:**

- `section code` over `section .code-snippet`
- `details summary` over `.details-trigger`
- `.connections a` over `.connection-badge`

#### 2.5 No Utility Classes

Utility classes describe HOW something looks, not WHAT it is.

**Remove these patterns:**

- Spacing: `.mb-2`, `.py-12`, `.gap-4`, `.px-6`, `.space-y-4`
- Layout: `.flex`, `.grid`, `.grid-cols-2`, `.items-center`
- Display: `.block`, `.inline-block`, `.hidden`
- Text: `.text-center`, `.text-lg`, `.font-bold`, `.uppercase`
- Colors: `.text-blue-500`, `.bg-gray-100`

**Instead, style contextually:**

```css
/* Good */
.technical h3 {
  font-size: var(--text-sm);
  font-weight: var(--font-semibold);
  margin-bottom: var(--space-3);
  display: flex;
  align-items: center;
}

/* Bad */
<h3 class="text-sm font-semibold mb-3 flex items-center">
```

#### 2.6 Contextual Component Styling

Components should be styled based on WHERE they appear, not through generic modifier classes.

**No generic button classes:**

```css
/* Bad */
.btn { }
.btn-primary { }
.btn-secondary { }
```

**Style contextually instead:**

```css
/* Good */
.hero .popular-providers a {
  padding: var(--space-2) var(--space-4);
  background: var(--bg-tertiary);
  border: 1px solid var(--border-primary);
  border-radius: var(--radius-lg);
}

.provider-detail-page .actions .btn-primary {
  background: var(--accent-primary);
  color: var(--color-navy-900);
}
```

**Why this matters:**

- Buttons in hero look different from buttons in provider detail
- Context determines appearance
- Reduces need for modifier classes

## CSS Architecture

### File Structure

```
src/css/
├── tokens.css          # Design tokens (colors, spacing, fonts)
├── main.css            # Main styles with imports
└── base/
    ├── reset.css       # CSS reset
    └── typography.css  # Base typography
```

### Semantic Section Pattern

Each major section follows this pattern:

```css
/* Section container */
.section-name {
  padding: var(--space-20) 0;
  background: var(--bg-primary);
}

/* Nested elements using cascade */
.section-name header {
  /* Header styles */
}

.section-name h2 {
  /* Title styles */
}

.section-name .some-element {
  /* Element styles */
}
```

### Design Tokens

Use CSS custom properties defined in `tokens.css`:

- Colors: `--color-navy-900`, `--color-cyan-400`, `--color-gray-700`, etc.
- Spacing: `--space-4` (1rem), `--space-8` (2rem), etc.
- Typography: `--text-base`, `--text-lg`, `--font-semibold`, etc.
- Theme variables: `--bg-primary`, `--text-primary`, `--border-primary`

**IMPORTANT**: Always use CSS custom properties instead of hardcoded color values:

- ✅ Good: `border: 1px solid var(--color-gray-700);`
- ❌ Bad: `border: 1px solid #334155;`

#### When to Use Semantic vs Direct Color Variables

When defining new variables in `tokens.css`, follow this hierarchy:

1. **Prefer semantic variables** (with -light/-dark suffixes) over direct color variables:
   - ✅ Good: `--color-input-bg: light-dark(var(--bg-tertiary-light), var(--bg-tertiary-dark));`
   - ❌ Bad: `--color-input-bg: light-dark(var(--color-gray-100), var(--color-navy-700));`

2. **Only use direct color variables** when:
   - Defining the base semantic variables themselves (e.g., `--bg-tertiary-light: var(--color-gray-100);`)
   - No appropriate semantic variable exists for your use case
   - The color combination is intentionally different from existing semantic patterns

3. **In component CSS** (main.css):
   - Use computed semantic variables: `background: var(--bg-primary);`
   - Or use inline `light-dark()` with semantic variables for contextual colors

This creates a clear hierarchy: base colors → semantic variables → computed variables → usage

### Light/Dark Mode

The theme system uses the CSS `color-scheme` property and `light-dark()` function:

- Default theme is dark mode
- Theme switching sets `document.documentElement.style.colorScheme = 'light' | 'dark'`

- Theme computed variables defined in `tokens.css` with progressive enhancement:

  ```css
  /* Progressive enhancement: fallback then light-dark() */
  --bg-primary: var(--bg-primary-light);
  --bg-primary: light-dark(var(--bg-primary-light), var(--bg-primary-dark));
  ```

- Most elements use computed theme variables:

  ```css
  .element {
    background: var(--bg-primary);
    color: var(--text-primary);
  }
  ```

- Contextual colors use inline `light-dark()`:

  ```css
  .kbd {
    border-color: light-dark(var(--border-primary-light), var(--color-navy-600));
  }
  ```

**Progressive Enhancement**: Each theme variable is declared twice in tokens.css:

1. First with light value as fallback for browsers without `light-dark()` support
2. Then with `light-dark()` which overrides in supporting browsers

## Development Workflow

### Feature Development Checklist

When adding or modifying features:

1. **Plan semantic HTML structure** using appropriate elements
2. **Write semantic CSS** using cascade pattern
3. **Add light/dark mode styles** for all elements
4. **Test responsiveness** across different screen sizes
5. **Verify consistency** with existing design system

### Common Issues

- **Colors not switching in dark mode**: Ensure element uses computed theme variables like `var(--bg-primary)` or inline `light-dark()`
- **Missing dark mode colors**: Check that theme variable has both `-light` and `-dark` variants defined in tokens.css
- **Wrong border colors**: Check color tokens are indigo not gray

## CSS Deletion Guidelines

When removing non-semantic CSS:

1. **Search for usage first**: Use grep to find where classes are used
2. **Confirm removal**: If only in CSS and not in refactored templates, delete it
3. **Test visually**: Take screenshots before and after
4. **Common deletions**:
   - Grid/flex utilities: `.grid`, `.flex`, `.items-center`, etc.
   - Spacing utilities: `.mb-*`, `.py-*`, `.gap-*`, etc.
   - Button classes: `.btn`, `.btn-primary`, etc.
   - Text utilities: `.text-center`, `.text-lg`, etc.

## Quality Standards

- **Visual consistency**: Changes should match existing design system and styling
- **Semantic HTML**: Use proper elements and structure
- **Maintainable CSS**: Descriptive class names, leverage cascade
- **Light/Dark mode**: Full support with smooth transitions (except during screenshots)
- **Responsive**: Mobile-first, works on all screen sizes

## Reference Files

Key files in the project:

- `src/css/main.css` - Main styles
- `src/css/tokens.css` - Design tokens
- `src/` - All page templates (index, provider-detail, providers, explore, stats)

## Development Server

Local development server runs at http://localhost:8080

## Deployment Architecture

The registry is built in the `apache/airflow` repo and served at `airflow.apache.org/registry/`.

### How it works

1. **Build**: `registry-build.yml` extracts metadata, builds the 11ty site, and syncs to S3.
   Supports two modes:
   - **Full build** (no `provider` input): extracts all ~99 providers (~12 min)
   - **Incremental build** (`provider=amazon`): extracts one provider (~30s), merges
     with existing data from S3 via `merge_registry_data.py`, then builds the full site
2. **S3 buckets**: `{live|staging}-docs-airflow-apache-org/registry/` (same bucket as docs, different prefix)
3. **Serving**: Apache HTTPD at `airflow.apache.org` rewrites `/registry/*` to CloudFront, which serves from S3
4. **Auto-trigger**: When `publish-docs-to-s3.yml` publishes provider docs, its
   `update-registry` job calls `registry-build.yml` with the provider ID for an
   incremental registry update

### Path prefix

- Production: `REGISTRY_PATH_PREFIX=/registry/` (default)
- Local dev: `REGISTRY_PATH_PREFIX=/` (set automatically by `pnpm dev`)
- All internal links use 11ty's `| url` filter, which prepends the prefix

### Changes needed in `apache/airflow-site` repo (separate PR)

1. **`.htaccess` rewrite rule** - Add a rule to proxy `/registry/*` through CloudFront:

```apache
RewriteRule ^registry/(.*)$ https://<cloudfront-distribution>.cloudfront.net/registry/$1 [P,L]
```

2. **Navigation link** - Add a "Registry" entry to the site header/nav in the Hugo templates,
   pointing to `/registry/`.

These changes mirror the existing `/docs/*` rewrite pattern.

## Data Extraction (`dev/registry/`)

The registry's JSON data is produced by four extraction scripts in `dev/registry/`,
which is a Python package (workspace member) with shared code in `registry_tools/`.

**Module type definitions** live in `dev/registry/registry_tools/types.py` — this is
the single source of truth for all module types (operator, hook, sensor, trigger, etc.).
The three Python extraction scripts and the frontend data file (`types.json`) all derive
from this module. To add a new type, add it to `MODULE_TYPES` in `types.py` and run
`generate_types_json.py`.

When modifying these scripts, understand the design decisions below.

### Why four separate scripts?

| Script | Runs on | Needs providers installed? | What it does |
|---|---|---|---|
| `extract_metadata.py` | Host | No | Provider metadata, class names (AST), PyPI stats, logos |
| `extract_versions.py` | Host | No | Per-version metadata from git tags |
| `extract_parameters.py` | Breeze | Yes | `__init__` parameter inspection via MRO |
| `extract_connections.py` | Breeze | Yes | Connection form metadata via ProvidersManager |

`extract_parameters.py` and `extract_connections.py` need runtime access to provider
classes (to inspect `__init__` signatures and call `get_connection_form_widgets()`).
They run inside Breeze where all providers are installed. `extract_metadata.py` and
`extract_versions.py` only need filesystem access and run on the host. This split means
the CI workflow can run the fast scripts (metadata, ~30s per provider) without spinning
up Breeze, while parameter/connection extraction is a separate step.

### Relationship to `run_provider_yaml_files_check.py`

`scripts/in_container/run_provider_yaml_files_check.py` (run by the
`check-provider-yaml-valid` pre-commit hook inside Breeze) validates that everything
listed in `provider.yaml` is correct: modules exist, classes are importable, and every
Python file in `operators/`/`hooks/`/`sensors/`/`triggers/` directories is accounted
for. This guarantees `provider.yaml` is the correct and complete manifest.

`extract_metadata.py` builds on that guarantee but solves a different problem: it needs
individual class names within each module. `provider.yaml` lists entries at two
granularities:

- **Module-level** (operators, hooks, sensors, triggers, transfers, bundles): e.g.,
  `airflow.providers.amazon.operators.s3` — a single module can contain many classes
  (`S3CopyObjectOperator`, `S3DeleteObjectsOperator`, `S3ListOperator`, etc.)
- **Class-level** (notifications, secrets-backends, logging, executors, task-decorators):
  e.g., `airflow.providers.amazon.notifications.chime.ChimeNotifier` — full class path
  already provided

For module-level entries, AST parsing discovers the individual classes and their
inheritance chains. For class-level entries, `extract_metadata.py` uses the paths from
`provider.yaml` directly — no discovery needed. The validation script ensures both
categories are correct before `extract_metadata.py` ever runs.

### Why AST parsing instead of runtime import?

`extract_metadata.py` runs on the CI host without installing 100+ provider packages.
It reads `.py` files and extracts class names, base classes, and docstrings from the
AST. This means it works with just `pyyaml` as a dependency.

The trade-off: AST parsing can't resolve dynamic class definitions or runtime-computed
attributes. For the 99 providers currently in the repo, AST parsing captures everything
because provider classes use standard `class Foo(Base):` definitions.

An approach using only Sphinx inventory files (without AST) was considered but rejected:
inventory files only exist for published providers, and they don't contain inheritance
information. The registry needs to know whether a class is an operator, hook, sensor,
etc., which requires resolving the inheritance chain back to base classes like
`BaseOperator` or `BaseHook`. AST parsing provides this.

### Module filtering via base class inheritance

Classes are discovered by checking if they inherit (transitively) from type-specific
base classes defined in `get_module_type_base_classes()`:

- **operators**: `BaseOperator`, `BaseSensorOperator`, `DecoratedOperator`, ...
- **hooks**: `BaseHook`, `DbApiHook`, `DiscoverableHook`
- **sensors**: `BaseSensorOperator`, `PokeSensorOperator`
- **triggers**: `BaseTrigger`, `TriggerEvent`
- **transfers**: `BaseOperator` (transfers are operators)
- **bundles**: `BaseDagBundle`

Inheritance resolution is transitive and cross-file. `build_global_inheritance_map()`
scans all provider `src/` directories to build a global `class_name → {base_names}`
map. This handles chains like `S3ListOperator → AwsBaseOperator → BaseOperator` where
the intermediate class lives in a different file.

After inheritance filtering, a post-filter skips private (`_`-prefixed), `Base*`,
`Abstract*`, and `*Mixin` classes to avoid indexing internal helper classes.

There is no suffix-based matching (e.g., checking if a class name ends with
"Operator"). The filtering is purely inheritance-based.

### Documentation URLs via Sphinx Inventory

Module `docs_url` values come from Sphinx `objects.inv` inventory files, not manually
constructed paths. The inventory is the canonical mapping of every documented class to
its URL, published on S3 alongside the docs.

- `read_inventory()` parses an `objects.inv` file, returning `{qualified_name: url_path}`
  for `py:class` entries
- `fetch_provider_inventory()` downloads the inventory from S3 with local file caching
  (12-hour TTL in `dev/registry/.inventory_cache/`)
- `extract_modules_from_yaml()` accepts an `inventory` dict and uses it for URL lookup,
  falling back to manual construction when a class isn't in the inventory

The fallback exists because unpublished providers (new, not yet released to PyPI) won't
have inventory files on S3.

If you're adding a new module type or changing how `docs_url` is constructed, prefer
extending the inventory lookup rather than hardcoding URL patterns.

### Why Eleventy for the static site?

Static site generators produce zero-JS pages by default. The registry works without
JavaScript — filtering and search are layered on top progressively. Eleventy has no
opinion on frontend frameworks, which keeps the dependency surface small (~30 packages
in the lockfile).

### Path prefix handling

The site deploys at `/registry/` on airflow.apache.org but runs at `/` during local
dev. Eleventy's `pathPrefix` config handles this via the `REGISTRY_PATH_PREFIX` env var.
Templates use the `| url` filter, and client-side JS reads `window.__REGISTRY_BASE__`
(injected in `base.njk`).

## Troubleshooting

### Can't find where a class is used

```bash
# Search in templates
grep -r "class-name" src/

# Search in CSS
grep "\.class-name" src/css/
```

### Color issues

1. Use browser DevTools to inspect computed styles
2. Check both light and dark mode
3. Verify color tokens in `tokens.css`
4. Ensure computed theme variables are used correctly
