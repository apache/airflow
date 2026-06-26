#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Sync theme files from the published sphinx-airflow-theme wheel.

Extracts all theme files (templates, static assets, config) except __init__.py,
which is owned locally for version management via upgrade_important_versions.py.
Idempotent: skips if a .version stamp file matches the current version.
"""

from __future__ import annotations

import re
import sys
import time
import zipfile
from io import BytesIO
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

AIRFLOW_ROOT = Path(__file__).resolve().parents[2]
THEME_DIR = AIRFLOW_ROOT / "docs-theme" / "sphinx_airflow_theme"
GEN_DIR = THEME_DIR / "static" / "_gen"
VERSION_STAMP = GEN_DIR / ".version"
WHEEL_URL = "https://airflow.apache.org/sphinx-airflow-theme/sphinx_airflow_theme-{version}-py3-none-any.whl"


def get_theme_version() -> str:
    init_py = THEME_DIR / "__init__.py"
    match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', init_py.read_text(), re.MULTILINE)
    if not match:
        print("ERROR: Could not read __version__ from", init_py, file=sys.stderr)
        sys.exit(1)
    return match.group(1)


def is_up_to_date(version: str) -> bool:
    if not GEN_DIR.is_dir() or not VERSION_STAMP.is_file():
        return False
    return VERSION_STAMP.read_text().strip() == version


def fetch_and_extract(version: str) -> None:
    url = WHEEL_URL.format(version=version)
    print(f"Fetching theme assets from {url}")
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = urlopen(url, timeout=30)
            break
        except URLError as e:
            if attempt < max_retries:
                delay = 2**attempt
                print(f"WARNING: Attempt {attempt}/{max_retries} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"ERROR: Failed to download {url} after {max_retries} attempts: {e}", file=sys.stderr)
                sys.exit(1)
    wheel_bytes = BytesIO(response.read())

    pkg_prefix = "sphinx_airflow_theme/"
    skip = {f"{pkg_prefix}__init__.py"}
    with zipfile.ZipFile(wheel_bytes) as whl:
        members = [
            m for m in whl.namelist() if m.startswith(pkg_prefix) and not m.endswith("/") and m not in skip
        ]
        if not members:
            print(f"ERROR: No theme files found in wheel {url}", file=sys.stderr)
            sys.exit(1)
        for member in members:
            rel_path = member[len(pkg_prefix) :]
            target = THEME_DIR / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(whl.read(member))

    VERSION_STAMP.write_text(version + "\n")
    print(f"Extracted {len(members)} theme files into {THEME_DIR}")


def main() -> None:
    version = get_theme_version()
    if is_up_to_date(version):
        print(f"Theme assets already up to date (v{version})")
        return
    fetch_and_extract(version)


if __name__ == "__main__":
    main()
