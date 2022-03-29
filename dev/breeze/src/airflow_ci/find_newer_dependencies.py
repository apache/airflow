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

import json
from datetime import timedelta
from typing import Any, Dict, List, Tuple

import click
import pendulum
import requests
from dateutil.parser import isoparse
from packaging import version
from rich.console import Console
from rich.progress import Progress

console = Console(width=400, color_system="standard")

option_branch = click.option(
    "--constraints-branch",
    default='constraints-main',
    help="Constraint branch to use to find newer dependencies",
)

option_python = click.option(
    "--python",
    type=click.Choice(["3.7", "3.8", "3.9", "3.10"]),
    default="3.7",
    help="Python version used",
)

option_timezone = click.option(
    "--timezone",
    default="UTC",
    type=str,
    help="Timezone to use during the check",
)

option_updated_on_or_after = click.option(
    "--updated-on-or-after",
    type=str,
    help="Date when the release was updated after",
)

option_max_age = click.option(
    "--max-age",
    type=int,
    default=3,
    help="Max age of the last release (used if no updated-on-or-after if specified)",
)


def get_releases_and_upload_times(package, min_date, current_version, tz) -> List[Tuple[str, Any]]:
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


@option_timezone
@option_branch
@option_python
@option_updated_on_or_after
@option_max_age
@click.command()
def main(constraints_branch: str, python: str, timezone: str, updated_on_or_after: str, max_age: int):
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
        "\n[yellow]Those are possible candidates that broke current "
        "`pip` resolution mechanisms by falling back to long backtracking[/]\n"
    )
    console.print(f"\n[yellow]We are limiting to packages updated after {min_date} ({timezone})[/]\n")
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
    console.print("\n[yellow]If you see long running builds with `pip` backtracking, you should follow[/]")
    console.print(
        "[yellow]https://github.com/apache/airflow/blob/main/dev/TRACKING_BACKTRACKING_ISSUES.md[/]\n"
    )
    constraint_string = ""
    for package, constrained_version in constrained_packages.items():
        constraint_string += f' "{package}=={constrained_version}"'
    console.print("[yellow]Use the following pip install command (see the doc above for details)\n")
    console.print(
        'pip install ".[devel_all]" --upgrade --upgrade-strategy eager '
        '"dill<0.3.3" "certifi<2021.0.0" "google-ads<14.0.1"' + constraint_string,
        markup=False,
        soft_wrap=True,
    )


if __name__ == '__main__':
    main()  # type: ignore[misc]
