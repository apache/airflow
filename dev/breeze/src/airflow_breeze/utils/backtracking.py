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

import datetime
import json
import re

from airflow_breeze.utils.console import get_console

DATE_MATCHER = re.compile(r"automatically generated on (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z")


def match_generated_date(string: str) -> datetime.datetime:
    match = DATE_MATCHER.search(string)
    if match:
        return datetime.datetime.fromisoformat(match.group(1))
    else:
        raise RuntimeError("Could not find date in constraints file")


def print_backtracking_candidates():
    import requests

    all_latest_dependencies_response = requests.get(
        "https://raw.githubusercontent.com/apache/airflow/"
        "constraints-main/constraints-source-providers-3.9.txt"
    )
    all_latest_dependencies_response.raise_for_status()
    constraints_text = all_latest_dependencies_response.text
    last_constraint_date = match_generated_date(constraints_text)
    get_console().print(f"Last constraint date: {last_constraint_date}")
    dependency_array = [
        dep for dep in constraints_text.splitlines() if not dep.startswith("#") and dep.strip()
    ]
    candidates_for_backtracking = []
    for x in dependency_array:
        dep, version = x.split("==")
        response = requests.get(f"https://pypi.org/pypi/{dep}/json")
        info = json.loads(response.text)
        latest_version = info["info"]["version"]
        release_date_str = info["releases"][latest_version][0]["upload_time"]
        release_date = datetime.datetime.fromisoformat(release_date_str)
        if latest_version != version and release_date > last_constraint_date:
            get_console().print(
                f"Latest version {dep}=={latest_version} release date: {release_date}. "
                f"In current constraints: {version})"
            )
            candidates_for_backtracking.append(f"{dep}<={version}")
    get_console().print(f"\nFound {len(candidates_for_backtracking)} candidates for backtracking")
    get_console().print("")
    formatted_candidates = " ".join(candidates_for_backtracking)
    get_console().print(
        "1. Run `breeze ci-image build --upgrade-to-newer-dependencies "
        "--eager-upgrade-additional-requirements "
        f'"{formatted_candidates}"`. It should succeed.\n'
    )
    get_console().print(
        "2. Bisect the candidate list. Remove half of the backtracking candidates. Run. If it fails switch "
        "to the other half. Continue bisecting the list until there is one candidate (ideally) left.\n"
    )
    get_console().print(
        "3. When it still succeeds but you have one candidate only, you found the culprit. "
        "Attempt to figure out the root cause. Use `dependency==<latest released version>` "
        "from the list above as `--eager-upgrade-additional-requirements` flag value.\n"
    )
    get_console().print(
        "4. Check the minimum version of the dependency that causes backtracking by using "
        '`--eager-upgrade-additional-requirements "dependency<latest version"` - usually it is '
        "the latest version released unless there were few releases. .\n"
    )
    get_console().print(
        "5. Add and commit the dependency<version to `Dockerfile.ci` in "
        "EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS ARG definition.\n"
    )
