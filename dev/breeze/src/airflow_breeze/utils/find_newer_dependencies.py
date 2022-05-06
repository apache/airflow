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
"""
Finds which newer dependencies were used to build that build and prints them for better diagnostics.

This is a common problem that currently `pip` does not produce "perfect" information about the errors,
and we sometimes need to guess what new dependency caused long backtracking. Once we know short
list of candidates, we can (following a manual process) pinpoint the actual culprit.

This small tool is run in CI whenever the image build timed out - so that we can easier guess
which dependency caused the problem.

The process to follow once you see the backtracking is described in:

https://github.com/apache/airflow/blob/main/dev/TRACKING_BACKTRACKING_ISSUES.md
"""
import json
from datetime import timedelta
from typing import Any, Dict, List, Tuple

from rich.console import Console
from rich.progress import Progress

console = Console(width=400, color_system="standard")


def find_newer_dependencies(
    constraints_branch: str, python: str, timezone: str, updated_on_or_after: str, max_age: int
):
    import pendulum
    import requests
    from packaging import version

    constraints = requests.get(
        f"https://raw.githubusercontent.com/" f"apache/airflow/{constraints_branch}/constraints-{python}.txt"
    ).text
    package_lines = list(filter(lambda x: not x.startswith("#"), constraints.splitlines()))
    constrained_packages: Dict[str, Any] = {}
    count_packages = len(package_lines)
    tz = pendulum.timezone(timezone)  # type: ignore[operator]
    if updated_on_or_after:
        min_date = pendulum.parse(updated_on_or_after, tz=tz)
    else:
        min_date = (pendulum.now(tz=tz) - timedelta(days=max_age)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    console.print(
        "\n[bright_yellow]Those are possible candidates that broke current "
        "`pip` resolution mechanisms by falling back to long backtracking[/]\n"
    )
    console.print(f"\n[bright_yellow]We are limiting to packages updated after {min_date} ({timezone})[/]\n")
    with Progress(console=console) as progress:
        task = progress.add_task(f"Processing {count_packages} packages.", total=count_packages)
        for package_line in package_lines:
            package, _, constraints_package_version_string = package_line.split("=")
            constraints_package_version = version.parse(constraints_package_version_string)
            for (package_version, upload_date) in get_releases_and_upload_times(
                package, min_date=min_date, current_version=constraints_package_version, tz=tz
            ):
                progress.console.print(
                    f"Package: {package}. Constraints version: {constraints_package_version}, "
                    f"Uploaded version: {package_version}, "
                    f"Upload date: {tz.convert(upload_date)} ({timezone})"
                )
                constrained_packages[package] = constraints_package_version
            progress.advance(task)
            progress.refresh()
    console.print(
        "\n[bright_yellow]If you see long running builds with `pip` backtracking, you should follow[/]"
    )
    console.print(
        "[bright_yellow]https://github.com/apache/airflow/blob/main/dev/TRACKING_BACKTRACKING_ISSUES.md[/]\n"
    )
    constraint_string = ""
    for package, constrained_version in constrained_packages.items():
        constraint_string += f' "{package}=={constrained_version}"'
    console.print("[bright_yellow]Use the following pip install command (see the doc above for details)\n")
    console.print(
        'pip install ".[devel_all]" --upgrade --upgrade-strategy eager '
        '"dill<0.3.3" "certifi<2021.0.0" "google-ads<14.0.1"' + constraint_string,
        markup=False,
        soft_wrap=True,
    )


def get_releases_and_upload_times(package, min_date, current_version, tz) -> List[Tuple[str, Any]]:
    import requests
    from dateutil.parser import isoparse
    from packaging import version

    package_info = json.loads(requests.get(f"https://pypi.python.org/pypi/{package}/json").text)
    releases: List[Tuple[Any, Any]] = []
    for release_version, release_info in package_info['releases'].items():
        if release_info and not release_info[0]['yanked']:
            parsed_version = version.parse(release_version)
            if (
                parsed_version.is_prerelease
                or parsed_version.is_devrelease
                or parsed_version == current_version
            ):
                continue
            upload_date = tz.convert(isoparse(release_info[0]['upload_time_iso_8601'])).replace(microsecond=0)
            if upload_date >= min_date:
                releases.append((parsed_version, upload_date))
    return releases
