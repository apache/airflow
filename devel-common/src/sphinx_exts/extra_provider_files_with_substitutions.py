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
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from packaging.version import Version


def _get_rc_matching_version(releases: dict, version: str):
    """
    Get the latest release candidate version matching the given version.
    """

    matching_versions = (v for v in releases.keys() if Version(v).base_version == version)
    latest_version = max(matching_versions, key=Version, default=None)

    return releases.get(latest_version, []) if latest_version else []


def get_release_date(package_name: str, version) -> str:
    """Get the release date of the current version."""
    if package_name == "":
        return ""
    import requests

    resp = requests.get(f"https://pypi.org/pypi/{package_name}/json")
    resp_json = resp.json()
    releases = resp_json.get("releases", {})
    release_info = releases.get(version, [])

    # If no exact version found, try to find the latest release candidate version
    if not release_info and len(releases) > 0:
        release_info = _get_rc_matching_version(releases, version)

    if release_info:
        release_date = datetime.fromisoformat(release_info[0].get("upload_time")).date()
        return str(release_date)

    return "Release date unknown"


def _manual_substitution(line: str, replacements: dict[str, str]) -> str:
    for value, repl in replacements.items():
        line = line.replace(value, repl)
    return line


def fix_provider_references(app, exception):
    """Sphinx "build-finished" event handler."""
    from sphinx.builders import html as builders

    if exception or not isinstance(app.builder, builders.StandaloneHTMLBuilder):
        return

    substitutions = {
        "|version|": app.config.version,
        "|PypiReleaseDate|": get_release_date(os.environ.get("AIRFLOW_PACKAGE_NAME", ""), app.config.version),
    }

    # Replace `|version|` in the files that require manual substitution
    for path in Path(app.outdir).rglob("*.html"):
        if path.exists():
            lines = path.read_text().splitlines(True)
            with path.open("w") as output_file:
                for line in lines:
                    output_file.write(_manual_substitution(line, substitutions))


def setup(app):
    """Setup plugin"""
    app.connect("build-finished", fix_provider_references)

    return {
        "parallel_write_safe": True,
        "parallel_read_safe": True,
    }
