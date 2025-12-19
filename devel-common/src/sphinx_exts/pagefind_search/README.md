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
pagefind_exclude_selectors = [".headerlink", ".toctree-wrapper", "nav", "footer"]

# File pattern (default: "**/*.html")
pagefind_glob = "**/*.html"

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

## Architecture

### Build Process

1. Sphinx builds HTML documentation
2. Extension copies CSS/JS to `_static/`
3. Extension injects search modal HTML into pages
4. Extension builds Pagefind index (if Python API available)

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
