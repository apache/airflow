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


def _get_all_versions(package_name: str):
    import requests

    versions_with_dates = []
    resp = requests.get(f"https://pypi.org/pypi/{package_name}/json")

    resp_json = resp.json()
    releases = resp_json.get("releases", {})
    for version, release_data in releases.items():
        base_version = Version(version).base_version
        if release_data:
            upload_time = release_data[0].get("upload_time")
            release_date = datetime.fromisoformat(upload_time).date()
            versions_with_dates.append((base_version, str(release_date)))
    return versions_with_dates


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
    }

    versions_dates = _get_all_versions(os.environ.get("AIRFLOW_PACKAGE_NAME", ""))
    # Replace `|version|` in the files that require manual substitution
    for path in Path(app.outdir).rglob("*.html"):
        is_changelog = str(path).endswith("changelog.html")

        if path.exists():
            lines = path.read_text().splitlines(True)
            with path.open("w") as output_file:
                for line in lines:
                    # Check if |PypiReleaseDate| format is in the line and skip it, old changelog has this template format
                    if "|PypiReleaseDate|" in line:
                        continue

                    output_file.write(_manual_substitution(line, substitutions))

                    if is_changelog:
                        for version_date in versions_dates:
                            if line.startswith(f"<h2>{version_date[0]}<a"):
                                output_file.write(
                                    f"""<p>Release Date: <code class="docutils literal notranslate"><span class="pre">{version_date[1]}</span></code></p>"""
                                )
                                versions_dates.remove(version_date)
                                break


def setup(app):
    """Setup plugin"""
    app.connect("build-finished", fix_provider_references)

    return {
        "parallel_write_safe": True,
        "parallel_read_safe": True,
    }
