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

# Pagefind Search Extension

A Sphinx extension providing fast, self-hosted search for Apache Airflow documentation using [Pagefind](https://pagefind.app/).

## Features

- **Automatic indexing** when docs are built
- **Cmd+K search** with modern UI and keyboard shortcuts
- **Self-hosted** - No third-party services
- **Content weighting** - Prioritizes titles and headings for better relevance
- **Optimized ranking** - Tuned for exact phrase matching and title matches
- **Playground support** - Debug and tune search behavior

## Usage

### Search Interface

- **Keyboard**: Press `Cmd+K` (Mac) or `Ctrl+K` (Windows/Linux)
- **Mouse**: Click the search button in the header
- **Navigation**: Arrow keys to navigate, Enter to select, Esc to close

## Configuration

In Sphinx's `conf.py`:

```python
# Enable/disable search (default: True)
pagefind_enabled = True

# Verbose logging (default: False)
pagefind_verbose = False

# Content selector (default: "main")
pagefind_root_selector = "main"

# Exclude selectors (default: see below)
# These elements won't be included in the search index
pagefind_exclude_selectors = [
    ".headerlink",  # Permalink icons
    ".toctree-wrapper",  # Table of contents navigation
    "nav",  # All navigation elements
    "footer",  # Footer content
    ".td-sidebar",  # Left sidebar
    ".breadcrumb",  # Breadcrumb navigation
    ".navbar",  # Top navigation bar
    ".dropdown-menu",  # Dropdown menus (version selector, etc.)
    ".docs-version-selector",  # Version selector widget
    "[role='navigation']",  # ARIA navigation landmarks
    ".d-print-none",  # Print-hidden elements (usually UI controls)
    ".pagefind-search-button",  # Search button itself
]

# File pattern (default: "**/*.html")
pagefind_glob = "**/*.html"

# Exclude patterns (default: [])
# Path patterns to exclude from indexing (e.g., auto-generated API docs)
# Note: File-by-file indexing is used when patterns are specified (slightly slower but precise)
# Pagefind does NOT automatically exclude underscore-prefixed directories
pagefind_exclude_patterns = [
    "_api/**",  # Exclude API documentation
    "_modules/**",  # Exclude source code modules
    "release_notes.html",  # Exclude specific files
    "genindex.html",  # Exclude generated index
]

# Content weighting (default: True)
# Uses lightweight regex to add data-pagefind-weight attributes to titles and headings
pagefind_content_weighting = True

# Enable playground (default: False)
# Creates a playground at /_pagefind/playground/ for debugging search
pagefind_enable_playground = False

# Custom records for non-HTML content (default: [])
pagefind_custom_records = [
    {
        "url": "/downloads/guide.pdf",
        "content": "PDF content...",
        "language": "en",
        "meta": {"title": "Guide PDF"},
    }
]
```

### Ranking Optimization

The extension uses optimized ranking parameters in `search.js`:

- **termFrequency: 1.0** - Standard term occurrence weighting
- **termSaturation: 0.7** - Moderate saturation to prevent over-rewarding repetition
- **termSimilarity: 7.5** - Maximum boost for exact phrase matches and similar terms
- **pageLength: 0** - No penalty for longer pages (important for reference documentation)

Combined with content weighting (10x for titles, 9x for h1, 7x for h2), these settings ensure exact title matches rank highly even for very long pages.

## Architecture

### Build Process

1. Sphinx builds HTML documentation
2. Extension copies CSS/JS to `_static/`
3. Extension injects search modal HTML into pages
4. (Optional) Extension adds content weights to HTML files
5. Extension builds Pagefind index with configured options

### Runtime

1. User presses Cmd+K or clicks search button
2. JavaScript loads Pagefind library dynamically
3. Search executes client-side
4. Results rendered in modal

## Troubleshooting

### Search not working

1. Check Pagefind is installed: `python -c "import pagefind; print('OK')"`
2. Check index exists: `ls generated/_build/docs/apache-airflow/stable/_pagefind/`
3. Enable verbose logging: `pagefind_verbose = True` in `conf.py`

### Index not created

- Ensure `pagefind_enabled = True` in `conf.py`
- Check build logs for errors
- If `pagefind[bin]` unavailable, use: `npx pagefind --site <build-dir>`

### Poor search ranking

1. Enable playground: `pagefind_enable_playground = True` in `conf.py`
2. Rebuild docs and access playground at `/_pagefind/playground/`
3. Test queries and analyze ranking scores
4. Ensure `pagefind_content_weighting = True` (default)
5. Check that titles and headings contain expected keywords

### Debugging with Playground

The playground provides detailed insights:

- View all indexed pages
- See ranking scores for each result
- Analyze impact of different search terms
- Verify content weighting is applied
- Test ranking parameter changes

Example, access at: `http://localhost:8000/docs/apache-airflow/stable/_pagefind/playground/`
