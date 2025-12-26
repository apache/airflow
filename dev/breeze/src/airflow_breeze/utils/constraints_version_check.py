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
from __future__ import annotations

import json
import re
import shlex
import sys
import tempfile
import urllib.request
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError, URLError

from rich.console import Console
from rich.syntax import Syntax

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import MOUNT_SELECTED
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.docker_command_utils import execute_command_in_shell
from airflow_breeze.utils.github import download_constraints_file
from airflow_breeze.utils.parallel import get_temp_file_name
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.shared_options import get_verbose

console = Console(color_system="standard")

if TYPE_CHECKING:
    from packaging.version import Version


def parse_constraints_generation_date(lines):
    for line in lines[:5]:
        if "automatically generated on" in line:
            date_str = line.split("generated on")[-1].strip()
            try:
                return datetime.fromisoformat(date_str).replace(tzinfo=None)
            except ValueError:
                get_console().print(
                    f"[yellow]Warning: Could not parse constraints generation date from: {date_str}[/]"
                )
                return None
    return None


def is_valid_version(version_str: str, latest_version: Version) -> bool:
    """Check if the version string is a valid one.

    The version should not ve pre-release or dev release and should be below the latest version"""
    from packaging import version

    try:
        parsed_version = version.parse(version_str)
        return (
            not parsed_version.is_prerelease
            and not parsed_version.is_devrelease
            and parsed_version <= latest_version
        )
    except version.InvalidVersion:
        return False


def count_versions_between(releases: dict[str, Any], current_version: str, latest_version: str):
    from packaging import version

    current = version.parse(current_version)
    latest = version.parse(latest_version)

    if current == latest:
        return 0

    versions_between = [
        v
        for v in releases.keys()
        if releases[v]
        and is_valid_version(version_str=v, latest_version=latest)
        and current < version.parse(v) <= latest
    ]
    return len(versions_between)


def get_status_emoji(constraint_date, latest_date, is_latest_version):
    """Determine status emoji based on how outdated the package is"""
    if is_latest_version:
        return "✅ OK             "  # Package is up to date (15 chars padding)

    try:
        constraint_dt = datetime.strptime(constraint_date, "%Y-%m-%d")
        latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
        days_diff = (latest_dt - constraint_dt).days

        if days_diff <= 5:
            return "📢 <5d          "
        if days_diff <= 30:
            return "⚠️ <30d           "
        return f"🚨 >{days_diff}d".ljust(15)
    except Exception:
        return "📢 N/A           "


def get_max_package_length(packages: list[tuple[str, str]]) -> int:
    return max(len(pkg) for pkg, _ in packages)


def should_show_package(releases, latest_version, constraints_date, mode, is_latest_version):
    if mode == "full":
        return True
    if mode == "diff-all":
        return not is_latest_version
    # diff-constraints
    if is_latest_version:
        return False

    if not constraints_date:
        return True

    for version_info in releases.values():
        if not version_info:
            continue
        try:
            release_date = datetime.fromisoformat(
                version_info[0]["upload_time_iso_8601"].replace("Z", "+00:00")
            ).replace(tzinfo=None)
            if release_date > constraints_date:
                return False
        except (KeyError, IndexError, ValueError):
            continue

    return True


def get_first_newer_release_date_str(releases, current_version):
    from packaging import version

    try:
        current = version.parse(current_version)

        # Filter and parse versions, excluding pre-releases and invalid versions
        valid_versions = []
        for v in releases:
            try:
                parsed_v = version.parse(v)
                if not parsed_v.is_prerelease and releases[v]:  # Check if release data exists
                    valid_versions.append(parsed_v)
            except version.InvalidVersion:
                continue

        # Find newer versions
        newer_versions = [v for v in valid_versions if v > current]

        if not newer_versions:
            return None

        # Get the immediate next version
        first_newer_version = str(min(newer_versions))
        upload_time_str = releases[first_newer_version][0]["upload_time_iso_8601"]
        return datetime.fromisoformat(upload_time_str.replace("Z", "+00:00")).strftime("%Y-%m-%d")

    except version.InvalidVersion as e:
        get_console().print(
            f"[yellow]Warning: Invalid version format for {current_version}. Skipping date check. Error: {str(e)}[/]"
        )
        return None


def constraints_version_check(
    python: str,
    airflow_constraints_mode: str,
    diff_mode: str,
    selected_packages: set[str] | None = None,
    explain_why: bool = False,
    github_token: str | None = None,
    github_repository: str | None = None,
):
    get_console().print(f"[bold cyan]Python version:[/] [white]{python}[/]")
    get_console().print(f"[bold cyan]Constraints mode:[/] [white]{airflow_constraints_mode}[/]\n")
    with tempfile.TemporaryDirectory() as temp_dir:
        constraints_file = Path(temp_dir) / "constraints.txt"
        download_constraints_file(
            constraints_reference=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
            python_version=python,
            airflow_constraints_mode=airflow_constraints_mode,
            github_token=github_token,
            output_file=constraints_file,
        )
        lines = constraints_file.read_text().splitlines()
    constraints_date = parse_constraints_generation_date(lines)
    if constraints_date:
        get_console().print(
            f"[bold cyan]Constraints file generation date:[/] [white]{constraints_date.strftime('%Y-%m-%d %H:%M:%S')}[/]"
        )
        get_console().print()
    if selected_packages:
        get_console().print("selected_packages:", selected_packages)
    packages = parse_packages_from_lines(lines, selected_packages)
    if not packages:
        get_console().print("[bold red]No matching packages found in constraints file.[/]")
        sys.exit(1)
    col_widths, format_str, headers, total_width = get_table_format(packages)
    print_table_header(format_str, headers, total_width)

    outdated_count, skipped_count, explanations = process_packages(
        packages=packages,
        constraints_date=constraints_date,
        mode=diff_mode,
        explain_why=explain_why,
        col_widths=col_widths,
        format_str=format_str,
        python_version=python,
        airflow_constraints_mode=airflow_constraints_mode,
        github_repository=github_repository,
    )

    print_table_footer(
        total_width=total_width,
        total_pkgs=len(packages),
        outdated_count=outdated_count,
        skipped_count=skipped_count,
        mode=diff_mode,
    )
    if explain_why and explanations:
        print_explanations(explanations)


def parse_packages_from_lines(lines: list[str], selected_packages: set[str] | None) -> list[tuple[str, str]]:
    remaining_packages: set[str] = selected_packages.copy() if selected_packages else set()
    packages = []
    for line_raw in lines:
        line = line_raw.strip()
        if line and not line.startswith("#") and "@" not in line:
            match = re.match(r"^([a-zA-Z0-9_.\-]+)==([\w.\-]+)$", line)
            if match:
                pkg_name = match.group(1)
                if not selected_packages or (pkg_name in selected_packages):
                    packages.append((pkg_name, match.group(2)))
                if pkg_name and selected_packages and pkg_name in selected_packages:
                    remaining_packages.remove(pkg_name)
    if remaining_packages:
        get_console().print(
            f"[bold yellow]Warning:[/] [white]{len(remaining_packages)}[/] packages were selected but not found in constraints file: {', '.join(remaining_packages)}"
        )
    return packages


def get_table_format(packages: list[tuple[str, str]]):
    max_pkg_length = get_max_package_length(packages)
    col_widths = {
        "Library Name": max(35, max_pkg_length),
        "Constraint Version": 18,
        "Constraint Date": 15,
        "Latest Version": 15,
        "Latest Date": 12,
        "📢 Status": 17,
        "# Versions Behind": 19,
        "PyPI Link": 60,
    }
    format_str = (
        f"{{:<{col_widths['Library Name']}}} | "
        f"{{:<{col_widths['Constraint Version']}}} | "
        f"{{:<{col_widths['Constraint Date']}}} | "
        f"{{:<{col_widths['Latest Version']}}} | "
        f"{{:<{col_widths['Latest Date']}}} | "
        f"{{:<{col_widths['📢 Status']}}} | "
        f"{{:<{col_widths['# Versions Behind']}}} | "
        f"{{:<{col_widths['PyPI Link']}}}"
    )
    headers = [
        "Library Name",
        "Constraint Version",
        "Constraint Date",
        "Latest Version",
        "Latest Date",
        "📢 Status",
        "# Versions Behind",
        "PyPI Link",
    ]
    total_width = sum(col_widths.values()) + (len(col_widths) - 1) * 3
    return col_widths, format_str, headers, total_width


def print_table_header(format_str: str, headers: list[str], total_width: int):
    get_console().print(f"[bold magenta]{format_str.format(*headers)}[/]")
    get_console().print(f"[magenta]{'=' * total_width}[/]")


def print_table_footer(total_width: int, total_pkgs: int, outdated_count: int, skipped_count: int, mode: str):
    get_console().print(f"[magenta]{'=' * total_width}[/]")
    get_console().print(f"[bold cyan]\nTotal packages checked:[/] [white]{total_pkgs}[/]")
    get_console().print(f"[bold yellow]Outdated packages found:[/] [white]{outdated_count}[/]")
    if mode == "diff-constraints":
        get_console().print(
            f"[bold blue]Skipped packages (updated after constraints generation):[/] [white]{skipped_count}[/]"
        )


def print_explanations(explanations: list[str]):
    get_console().print("\n[bold magenta]Upgrade Explanations:[/]")
    for explanation in explanations:
        get_console().print(explanation)


def update_pyproject_dependency(pyproject_path: Path, pkg: str, latest_version: str):
    lines = pyproject_path.read_text().splitlines()
    new_lines = []
    in_deps = False
    dep_added = False
    for line in lines:
        new_lines.append(line)
        if line.strip() == "dependencies = [":
            in_deps = True
        elif in_deps and line.strip().startswith("]") and not dep_added:
            new_lines.insert(-1, f'    "{pkg}=={latest_version}",')
            dep_added = True
            in_deps = False
    if not dep_added:
        new_lines.append(f'    "{pkg}=={latest_version}",')
    pyproject_path.write_text("\n".join(new_lines) + "\n")
    if get_verbose():
        get_console().print(
            f"[cyan]Fixed {pkg} at {latest_version} in [white]{pyproject_path}[/] [dim](pyproject.toml)[/]"
        )


def process_packages(
    packages: list[tuple[str, str]],
    constraints_date: datetime | None,
    mode: str,
    explain_why: bool,
    col_widths: dict,
    format_str: str,
    python_version: str,
    airflow_constraints_mode: str,
    github_repository: str | None,
) -> tuple[int, int, list[str]]:
    @contextmanager
    def preserve_pyproject_file(pyproject_path: Path):
        original_content = pyproject_path.read_text()
        try:
            yield
        finally:
            pyproject_path.write_text(original_content)

    def fetch_pypi_data(pkg: str) -> dict:
        pypi_url = f"https://pypi.org/pypi/{pkg}/json"
        with urllib.request.urlopen(pypi_url) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def get_release_dates(releases: dict, version: str) -> str:
        if releases.get(version):
            return (
                datetime.fromisoformat(releases[version][0]["upload_time_iso_8601"].replace("Z", "+00:00"))
                .replace(tzinfo=None)
                .strftime("%Y-%m-%d")
            )
        return "N/A"

    outdated_count = 0
    skipped_count = 0
    explanations = []

    for pkg, pinned_version in packages:
        try:
            data = fetch_pypi_data(pkg)
            latest_version = data["info"]["version"]
            releases = data["releases"]
            latest_release_date = get_release_dates(releases, latest_version)
            constraint_release_date = get_release_dates(releases, pinned_version)
            is_latest_version = pinned_version == latest_version
            versions_behind = count_versions_between(releases, pinned_version, latest_version)
            versions_behind_str = str(versions_behind) if versions_behind > 0 else ""
            if should_show_package(releases, latest_version, constraints_date, mode, is_latest_version):
                print_package_table_row(
                    pkg=pkg,
                    pinned_version=pinned_version,
                    constraint_release_date=constraint_release_date,
                    latest_version=latest_version,
                    latest_release_date=latest_release_date,
                    releases=releases,
                    col_widths=col_widths,
                    format_str=format_str,
                    is_latest_version=is_latest_version,
                    versions_behind_str=versions_behind_str,
                )
                if not is_latest_version:
                    outdated_count += 1
            else:
                skipped_count += 1

            if explain_why and not is_latest_version:
                explanation = explain_package_upgrade(
                    pkg=pkg,
                    pinned_version=pinned_version,
                    latest_version=latest_version,
                    python_version=python_version,
                    airflow_constraints_mode=airflow_constraints_mode,
                    github_repository=github_repository,
                )
                explanations.append(explanation)
        except HTTPError as e:
            get_console().print(f"[bold red]Error fetching {pkg} from PyPI: HTTP {e.code}[/]")
            continue
        except URLError as e:
            get_console().print(f"[bold red]Error fetching {pkg} from PyPI: {e.reason}[/]")
            continue
    return outdated_count, skipped_count, explanations


def print_package_table_row(
    pkg: str,
    pinned_version: str,
    constraint_release_date: str,
    latest_version: str,
    latest_release_date: str,
    releases: dict,
    col_widths: dict,
    format_str: str,
    is_latest_version: bool,
    versions_behind_str: str,
):
    first_newer_date_str = get_first_newer_release_date_str(releases, pinned_version)
    status = get_status_emoji(
        first_newer_date_str or constraint_release_date,
        datetime.now().strftime("%Y-%m-%d"),
        is_latest_version,
    )
    pypi_link = f"https://pypi.org/project/{pkg}/{latest_version}"
    color = (
        "green"
        if is_latest_version
        else ("yellow" if status.startswith("📢") or status.startswith("⚠️") else "red")
    )
    offset = 1 if status.startswith("⚠️") else 0
    string_to_print = format_str.format(
        pkg,
        pinned_version[: col_widths["Constraint Version"]],
        constraint_release_date[: col_widths["Constraint Date"]],
        latest_version[: col_widths["Latest Version"]],
        latest_release_date[: col_widths["Latest Date"]],
        status[: (col_widths["📢 Status"] + offset)],
        versions_behind_str,
        pypi_link,
    )
    get_console().print(f"[{color}]{string_to_print}[/]")


def explain_package_upgrade(
    pkg: str,
    pinned_version: str,
    latest_version: str,
    python_version: str,
    airflow_constraints_mode: str,
    github_repository: str | None,
) -> str:
    explanation = (
        f"[bold blue]\n--- Explaining for {pkg} (current: {pinned_version}, latest: {latest_version}) ---[/]"
    )

    @contextmanager
    def preserve_pyproject_file(pyproject_path: Path):
        original_content = pyproject_path.read_text()
        try:
            yield pyproject_path
        finally:
            pyproject_path.write_text(original_content)

    additional_args = []
    if airflow_constraints_mode == "constraints-source-providers":
        # In case of source constraints we also need to add all development dependencies
        # to reflect exactly what is installed in the CI image by default
        additional_args.extend(
            ["--group", "dev", "--group", "docs", "--group", "docs-gen", "--group", "leveldb"]
        )
    with preserve_pyproject_file(AIRFLOW_ROOT_PATH / "pyproject.toml") as airflow_pyproject:
        shell_params = ShellParams(
            github_repository=github_repository,
            python=python_version,
            mount_sources=MOUNT_SELECTED,
        )
        output_before = Output(title="output_before", file_name=get_temp_file_name())
        execute_command_in_shell(
            shell_params,
            project_name="constraints",
            command=shlex.join(
                [
                    "uv",
                    "sync",
                    "--all-packages",
                    *additional_args,
                    "--resolution",
                    "highest",
                    "--refresh",
                    "--python",
                    python_version,
                ]
            ),
            output=output_before,
            signal_error=False,
        )
        update_pyproject_dependency(airflow_pyproject, pkg, latest_version)
        if get_verbose():
            syntax = Syntax(
                airflow_pyproject.read_text(), "toml", theme="monokai", line_numbers=True, word_wrap=False
            )
            explanation += "\n" + str(syntax)
        output_after = Output(title="output_after", file_name=get_temp_file_name())
        after_result = execute_command_in_shell(
            shell_params,
            project_name="constraints",
            command=shlex.join(
                [
                    "uv",
                    "sync",
                    "--all-packages",
                    "--resolution",
                    "highest",
                    "--refresh",
                    "--python",
                    python_version,
                ],
            ),
            output=output_after,
            signal_error=False,
        )
        if after_result.returncode == 0:
            explanation += f"\n[bold yellow]Package {pkg} can be upgraded from {pinned_version} to {latest_version} without conflicts.[/]."
            if airflow_constraints_mode == "constraints-source-providers":
                explanation += Path(output_after.file_name).read_text()
        if after_result.returncode != 0 or get_verbose():
            explanation += f"\n[yellow]uv sync output for {pkg}=={latest_version}:[/]\n"
            explanation += Path(output_after.file_name).read_text()
    return explanation
