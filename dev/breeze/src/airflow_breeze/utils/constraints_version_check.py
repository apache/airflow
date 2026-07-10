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
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError, URLError

from rich.console import Console
from rich.syntax import Syntax

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import MOUNT_SELECTED
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.console import Output, console_print
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
                console_print(
                    f"[yellow]Warning: Could not parse constraints generation date from: {date_str}[/]"
                )
                return None
    return None


def is_yanked_release(release_files: list[dict] | None) -> bool:
    """Return True if the release has files and all of them are yanked on PyPI."""
    if not release_files:
        return False
    return all(f.get("yanked", False) for f in release_files)


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
        and not is_yanked_release(releases[v])
        and is_valid_version(version_str=v, latest_version=latest)
        and current < version.parse(v) <= latest
    ]
    return len(versions_between)


def get_status_emoji(constraint_date, latest_date, is_latest_version, cooldown_days: int = 0):
    """Determine status emoji based on how outdated the package is.

    The ``cooldown_days`` value shifts the thresholds so that time a package
    spent in the cooldown window is not counted against its staleness — a
    package that was released just after the cooldown period should still be
    reported as "new" rather than immediately as "warning".

    All emojis used here (✅, 📢, 🔶, 🚨) are single Python chars with ~2 visual cells,
    so ljust produces consistent alignment without any offset workarounds.

    Returns a tuple of (formatted_status_string, status_category) where status_category
    is one of "ok", "new", "warning", "critical".
    """
    col_target = 11
    if is_latest_version:
        return "✅ OK".ljust(col_target), "ok"

    new_threshold = 5 + cooldown_days
    warning_threshold = 30 + cooldown_days
    try:
        constraint_dt = datetime.strptime(constraint_date, "%Y-%m-%d")
        latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
        days_diff = (latest_dt - constraint_dt).days

        if days_diff <= new_threshold:
            return f"📢 <{new_threshold}d".ljust(col_target), "new"
        if days_diff <= warning_threshold:
            return f"🔶 <{warning_threshold}d".ljust(col_target), "warning"
        return f"🚨 >{days_diff}d".ljust(col_target), "critical"
    except Exception:
        return "📢 N/A".ljust(col_target), "new"


def get_days_stale(latest_release_date: str) -> str:
    """Return the number of days since the latest release if >365, else empty string."""
    try:
        latest_release_dt = datetime.strptime(latest_release_date, "%Y-%m-%d")
        days_since = (datetime.now() - latest_release_dt).days
        if days_since > 365:
            return str(days_since)
    except Exception:
        pass
    return ""


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
        if is_yanked_release(version_info):
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


def get_latest_version_with_cooldown(releases: dict[str, Any], cooldown_days: int) -> str | None:
    """Find the latest non-prerelease version whose release date is outside the cooldown period.

    Returns the version string, or None if no version qualifies.
    """
    from packaging import version

    cutoff = datetime.now() - timedelta(days=cooldown_days)
    candidates: list[tuple[version.Version, str]] = []
    for v, release_files in releases.items():
        if not release_files:
            continue
        if is_yanked_release(release_files):
            continue
        try:
            parsed_v = version.parse(v)
        except version.InvalidVersion:
            continue
        if parsed_v.is_prerelease or parsed_v.is_devrelease:
            continue
        try:
            upload_time = datetime.fromisoformat(
                release_files[0]["upload_time_iso_8601"].replace("Z", "+00:00")
            ).replace(tzinfo=None)
        except (KeyError, IndexError, ValueError):
            continue
        if upload_time <= cutoff:
            candidates.append((parsed_v, v))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def get_first_newer_release_date_str(releases, current_version):
    from packaging import version

    try:
        current = version.parse(current_version)

        # Filter and parse versions, excluding pre-releases, yanked, and invalid versions
        valid_versions = []
        for v in releases:
            try:
                parsed_v = version.parse(v)
                if not parsed_v.is_prerelease and releases[v] and not is_yanked_release(releases[v]):
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
        console_print(
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
    cooldown_days: int = 4,
):
    console_print(f"[bold cyan]Python version:[/] [white]{python}[/]")
    console_print(f"[bold cyan]Constraints mode:[/] [white]{airflow_constraints_mode}[/]")
    console_print(f"[bold cyan]Cooldown period:[/] [white]{cooldown_days} days[/]\n")
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
        console_print(
            f"[bold cyan]Constraints file generation date:[/] [white]{constraints_date.strftime('%Y-%m-%d %H:%M:%S')}[/]"
        )
        console_print()
    if selected_packages:
        console_print("selected_packages:", selected_packages)
    packages = parse_packages_from_lines(lines, selected_packages)
    if not packages:
        console_print("[bold red]No matching packages found in constraints file.[/]")
        sys.exit(1)
    col_widths, format_str, headers, total_width = get_table_format(packages)
    print_table_header(format_str, headers, total_width)

    outdated_count, skipped_count, explanations, status_counts = process_packages(
        packages=packages,
        constraints_date=constraints_date,
        mode=diff_mode,
        explain_why=explain_why,
        col_widths=col_widths,
        format_str=format_str,
        python_version=python,
        airflow_constraints_mode=airflow_constraints_mode,
        github_repository=github_repository,
        cooldown_days=cooldown_days,
    )

    print_table_footer(
        total_width=total_width,
        total_pkgs=len(packages),
        outdated_count=outdated_count,
        skipped_count=skipped_count,
        mode=diff_mode,
        status_counts=status_counts,
        cooldown_days=cooldown_days,
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
        console_print(
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
        "📢 Status": 12,
        "# Days Stale": 12,
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
        f"{{:<{col_widths['# Days Stale']}}} | "
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
        "# Days Stale",
        "# Versions Behind",
        "PyPI Link",
    ]
    total_width = sum(col_widths.values()) + (len(col_widths) - 1) * 3
    return col_widths, format_str, headers, total_width


def print_table_header(format_str: str, headers: list[str], total_width: int):
    console_print(f"[bold magenta]{format_str.format(*headers)}[/]")
    console_print(f"[magenta]{'=' * total_width}[/]")


def print_table_footer(
    total_width: int,
    total_pkgs: int,
    outdated_count: int,
    skipped_count: int,
    mode: str,
    status_counts: dict[str, int],
    cooldown_days: int = 0,
):
    new_threshold = 5 + cooldown_days
    warning_threshold = 30 + cooldown_days
    console_print(f"[magenta]{'=' * total_width}[/]")
    console_print(f"[bold cyan]\nTotal packages checked:[/] [white]{total_pkgs}[/]")
    console_print(f"  [green]✅ Up to date:[/] [white]{status_counts['ok']}[/]")
    console_print(f"  [yellow]📢 New (<{new_threshold}d):[/] [white]{status_counts['new']}[/]")
    console_print(f"  [magenta]🔶 Warning (<{warning_threshold}d):[/] [white]{status_counts['warning']}[/]")
    console_print(f"  [red]🚨 Critical (>{warning_threshold}d):[/] [white]{status_counts['critical']}[/]")
    console_print(f"[bold yellow]Outdated packages found:[/] [white]{outdated_count}[/]")
    if mode == "diff-constraints":
        console_print(
            f"[bold blue]Skipped packages (updated after constraints generation):[/] [white]{skipped_count}[/]"
        )


def print_explanations(explanations: list[str]):
    console_print("\n[bold magenta]Upgrade Explanations:[/]")
    for explanation in explanations:
        console_print(explanation)


def update_pyproject_dependency(pyproject_path: Path, pkg: str, latest_version: str, python_version: str):
    lines = pyproject_path.read_text().splitlines()
    new_lines = []
    in_deps = False
    dep_added = False
    dep_string = f"    \"{pkg}=={latest_version}; python_version=='{python_version}'\","
    for line in lines:
        new_lines.append(line)
        if line.strip() == "dependencies = [":
            in_deps = True
        elif in_deps and line.strip().startswith("]") and not dep_added:
            new_lines.insert(-1, dep_string)
            dep_added = True
            in_deps = False
    if not dep_added:
        new_lines.append(dep_string)
    pyproject_path.write_text("\n".join(new_lines) + "\n")
    if get_verbose():
        console_print(
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
    cooldown_days: int = 4,
) -> tuple[int, int, list[str], dict[str, int]]:
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
    status_counts: dict[str, int] = {"ok": 0, "new": 0, "warning": 0, "critical": 0}

    for pkg, pinned_version in packages:
        try:
            data = fetch_pypi_data(pkg)
            releases = data["releases"]
            latest_version_with_cooldown = get_latest_version_with_cooldown(releases, cooldown_days)
            latest_version = latest_version_with_cooldown or data["info"]["version"]
            latest_release_date = get_release_dates(releases, latest_version)
            constraint_release_date = get_release_dates(releases, pinned_version)
            is_latest_version = pinned_version == latest_version
            versions_behind = count_versions_between(releases, pinned_version, latest_version)
            versions_behind_str = str(versions_behind) if versions_behind > 0 else ""
            if should_show_package(releases, latest_version, constraints_date, mode, is_latest_version):
                status_category = print_package_table_row(
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
                    cooldown_days=cooldown_days,
                )
                status_counts[status_category] += 1
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
            console_print(f"[bold red]Error fetching {pkg} from PyPI: HTTP {e.code}[/]")
            continue
        except URLError as e:
            console_print(f"[bold red]Error fetching {pkg} from PyPI: {e.reason}[/]")
            continue
    return outdated_count, skipped_count, explanations, status_counts


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
    cooldown_days: int = 0,
) -> str:
    first_newer_date_str = get_first_newer_release_date_str(releases, pinned_version)
    status, status_category = get_status_emoji(
        first_newer_date_str or constraint_release_date,
        datetime.now().strftime("%Y-%m-%d"),
        is_latest_version,
        cooldown_days=cooldown_days,
    )
    days_stale_str = get_days_stale(latest_release_date)
    pypi_link = f"https://pypi.org/project/{pkg}/{latest_version}"
    if status_category == "ok":
        color = "green"
    elif status_category == "new":
        color = "yellow"
    elif status_category == "warning":
        color = "magenta"
    elif status_category == "critical":
        color = "red"
    else:
        color = "white"
    string_to_print = format_str.format(
        pkg,
        pinned_version[: col_widths["Constraint Version"]],
        constraint_release_date[: col_widths["Constraint Date"]],
        latest_version[: col_widths["Latest Version"]],
        latest_release_date[: col_widths["Latest Date"]],
        status[: col_widths["📢 Status"]],
        days_stale_str,
        versions_behind_str,
        pypi_link,
    )
    console_print(f"[{color}]{string_to_print}[/]")
    return status_category


def parse_freeze(freeze_text: str) -> dict[str, str]:
    """Parse ``uv pip freeze`` output into a ``{canonical_name: version}`` mapping.

    Lines that are not simple ``name==version`` pins (editable installs, ``@`` URLs,
    log noise emitted by uv) are ignored.
    """
    from packaging.utils import canonicalize_name

    versions: dict[str, str] = {}
    for line in freeze_text.splitlines():
        match = re.match(r"^([A-Za-z0-9_.\-]+)==([\w.\-]+)$", line.strip())
        if match:
            versions[str(canonicalize_name(match.group(1)))] = match.group(2)
    return versions


def extract_uv_conflict(text: str) -> str:
    """Slice uv's resolver-conflict narrative out of a noisy command log.

    uv prints unsatisfiable resolutions as a block that starts with a line containing
    ``No solution found`` followed by ``Because ... we can conclude ...`` lines. The breeze
    shell wraps every command with an Airflow (re)install, so the conflict is buried in a lot
    of unrelated build/install output — this returns just the conflict block (with ANSI color
    codes stripped), or an empty string if no conflict was reported.
    """
    ansi_re = re.compile(r"\x1b\[[0-9;]*m")
    clean = ansi_re.sub("", text)
    lines = clean.splitlines()
    for index, line in enumerate(lines):
        if "No solution found" in line:
            return "\n".join(lines[index:]).strip()
    return ""


def find_downgrades(
    before: dict[str, str], after: dict[str, str], exclude: str
) -> list[tuple[str, str, str]]:
    """Return ``(name, before_version, after_version)`` for packages that went *down*.

    ``exclude`` is the canonical name of the package being explained (it is expected
    to go up, so it is never reported as a downgrade).
    """
    from packaging import version

    downgrades: list[tuple[str, str, str]] = []
    for name, before_version in before.items():
        if name == exclude:
            continue
        after_version = after.get(name)
        if after_version is None:
            continue
        try:
            if version.parse(after_version) < version.parse(before_version):
                downgrades.append((name, before_version, after_version))
        except version.InvalidVersion:
            continue
    return sorted(downgrades)


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
        # to reflect exactly what is installed in the CI image by default. The ``ci-image``
        # group aggregates dev/docs/docs-gen plus any hard-to-install provider extras
        # (see root pyproject.toml).
        additional_args.extend(["--group", "ci-image"])
    with (
        preserve_pyproject_file(AIRFLOW_ROOT_PATH / "pyproject.toml") as airflow_pyproject,
        preserve_pyproject_file(AIRFLOW_ROOT_PATH / "uv.lock"),
    ):
        from packaging.utils import canonicalize_name

        canonical_pkg = str(canonicalize_name(pkg))

        shell_params = ShellParams(
            github_repository=github_repository,
            python=python_version,
            mount_sources=MOUNT_SELECTED,
        )

        # Marker echoed between ``uv sync`` and ``uv pip freeze`` so the freeze output can be
        # sliced out of the combined shell log.
        freeze_marker = "===BREEZE_RESOLVED_FREEZE==="

        def sync_and_freeze(title: str):
            """Resolve at --resolution highest and, in the *same* shell, freeze the result.

            Each ``execute_command_in_shell`` call is a fresh ``docker compose run --rm``
            container, so running ``uv pip freeze`` as a separate call would not reliably see
            the environment the sync just populated. Chaining both in one ``bash -c`` keeps
            the freeze in the same shell/venv as the sync. ``&&`` ensures the freeze only runs
            when the sync succeeds and that a sync failure is still reflected in the return
            code. Returns ``(result, combined_output_text, {canonical_name: version})``.
            """
            sync = shlex.join(
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
            )
            output = Output(title=title, file_name=get_temp_file_name())
            result = execute_command_in_shell(
                shell_params,
                project_name="breeze-constraints",
                command=shlex.join(["bash", "-c", f"{sync} && echo {freeze_marker} && uv pip freeze"]),
                output=output,
                signal_error=False,
            )
            text = Path(output.file_name).read_text()
            versions = parse_freeze(text.split(freeze_marker, 1)[1]) if freeze_marker in text else {}
            return result, text, versions

        # Baseline: resolve the workspace at --resolution highest *without* any pin and
        # record what version that resolution naturally selects for the package. This is
        # the resolution that actually generates the constraints, so it is the ground truth
        # for "what would the constraints pick".
        _, before_text, before_versions = sync_and_freeze("output_before")
        baseline_version = before_versions.get(canonical_pkg)

        update_pyproject_dependency(airflow_pyproject, pkg, latest_version, python_version)
        if get_verbose():
            syntax = Syntax(
                airflow_pyproject.read_text(), "toml", theme="monokai", line_numbers=True, word_wrap=False
            )
            explanation += "\n" + str(syntax)
        after_result, after_text, after_versions = sync_and_freeze("output_after")

        # A zero exit code only proves that *some* valid resolution exists with the pin — not
        # that --resolution highest would ever select it. Inspect what was actually resolved:
        # if honouring the pin forced *other* packages to be downgraded, the unpinned highest
        # resolution (i.e. the constraints) keeps the package at its lower version, so this is
        # NOT a clean upgrade.
        resolved_version = after_versions.get(canonical_pkg)
        downgrades = find_downgrades(before_versions, after_versions, exclude=canonical_pkg)

        if after_result.returncode != 0:
            # Forcing the package to its latest version produced no valid resolution at all:
            # a genuine hard conflict. Surface uv's own conflict narrative from the sync log.
            explanation += (
                f"\n[bold red]Package {pkg} CANNOT be upgraded to {latest_version}: "
                f"uv could not resolve the workspace with {pkg}=={latest_version} pinned "
                f"(hard conflict).[/]"
            )
            conflict = extract_uv_conflict(after_text)
            if conflict:
                explanation += f"\n\n[bold yellow]Conflict as reported by uv:[/]\n{conflict}"
        elif not before_versions or not after_versions:
            # Without the resolved version lists we cannot tell a clean upgrade apart from one
            # that only works by downgrading other packages — never silently claim success.
            explanation += (
                f"\n[bold yellow]uv sync succeeded but the resolved package versions could not "
                f"be read (empty freeze output), so the upgrade of {pkg} to {latest_version} "
                f"could not be classified.[/]"
            )
        elif baseline_version == latest_version:
            explanation += (
                f"\n[bold green]Package {pkg} already resolves to {latest_version} under "
                f"--resolution highest. The constraints file appears to be stale.[/]"
            )
        elif resolved_version != latest_version:
            explanation += (
                f"\n[bold yellow]uv sync succeeded but {pkg} still resolved to "
                f"{resolved_version or 'an unknown version'}, not {latest_version} — "
                f"the pin did not take effect, so this is not a real upgrade.[/]"
            )
        elif downgrades:
            explanation += (
                f"\n[bold yellow]Package {pkg} can reach {latest_version} only by DOWNGRADING "
                f"other packages, so --resolution highest keeps it at "
                f"{baseline_version or pinned_version}. Required downgrades:[/]"
            )
            for name, before_version, after_version in downgrades:
                explanation += f"\n  - {name}: {before_version} -> {after_version}"
            # Reproduce the conflict explicitly so uv's own resolver narrative is visible.
            # A fresh `uv pip compile` of just the package at its target version plus the
            # packages it would otherwise displace (held at their current versions) is a
            # contradiction, so uv fails and prints exactly why they cannot coexist. Running
            # it from scratch (rather than against the workspace) keeps the output to the
            # conflict itself, and we filter to uv's narrative regardless of shell noise.
            conflict_pins = [f"{pkg}=={latest_version}"]
            conflict_pins += [f"{name}=={before_version}" for name, before_version, _ in downgrades]
            printf_cmd = "printf '%s\\n' " + " ".join(shlex.quote(pin) for pin in conflict_pins)
            probe_output = Output(title="conflict_probe", file_name=get_temp_file_name())
            execute_command_in_shell(
                shell_params,
                project_name="breeze-constraints",
                command=shlex.join(
                    [
                        "bash",
                        "-c",
                        f"{printf_cmd} | uv pip compile - --python {shlex.quote(python_version)}",
                    ]
                ),
                output=probe_output,
                signal_error=False,
            )
            conflict = extract_uv_conflict(Path(probe_output.file_name).read_text())
            explanation += (
                f"\n\n[bold yellow]Conflict as reported by uv "
                f"(uv pip compile {' '.join(conflict_pins)}):[/]\n"
            )
            explanation += conflict or "[dim](uv did not emit a conflict narrative)[/]"
        else:
            explanation += (
                f"\n[bold green]Package {pkg} can be upgraded from {pinned_version} to "
                f"{latest_version} without conflicts and without downgrading other packages.[/]"
                f"\n[dim]If this result is unexpected, run 'uv cache clean' and retry — a stale "
                f"uv cache can make breeze resolve against an out-of-date environment.[/]"
            )

        if get_verbose():
            # Full resolver logs of both phases — only when explicitly requested, since they
            # are very long (each is a complete uv sync plus freeze).
            explanation += (
                f"\n\n[yellow]--- uv resolver output: phase 1, baseline (no pin) ---[/]\n{before_text}"
                f"\n[yellow]--- uv resolver output: phase 2, with {pkg}=={latest_version} pinned ---[/]"
                f"\n{after_text}"
            )
    return explanation
