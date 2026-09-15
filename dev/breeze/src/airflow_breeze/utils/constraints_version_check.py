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
import shutil
import sys
import tempfile
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError, URLError

from rich.console import Console

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import MOUNT_SELECTED
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.console import Output, console_print
from airflow_breeze.utils.docker_command_utils import execute_command_in_shell
from airflow_breeze.utils.github import download_constraints_file
from airflow_breeze.utils.parallel import get_temp_file_name
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH, FILES_PATH
from airflow_breeze.utils.shared_options import get_verbose

console = Console(color_system="standard")

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")

# A constraints file pins ~770 packages, each one a PyPI round-trip. The fetches are
# independent, so they run on a small pool; the table is still printed in constraints-file
# order because the results are consumed in that order afterwards.
PYPI_FETCH_PARALLELISM = 10
PYPI_FETCH_TIMEOUT_SECONDS = 30

# Ten requests in flight is polite enough that PyPI's CDN serves them without complaint, but
# a burst can still come back throttled (429) or fail transiently (5xx) on a bad day. Those
# are retried a few times rather than failing the package, obeying Retry-After when PyPI
# sends one so the wait matches what it is actually asking for.
PYPI_FETCH_MAX_ATTEMPTS = 3
PYPI_FETCH_BACKOFF_SECONDS = 1.0
PYPI_RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

if TYPE_CHECKING:
    from collections.abc import Iterator

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


def pin_dependency_in_pyproject(
    pyproject_text: str, pkg: str, latest_version: str, python_version: str
) -> str:
    """Return the pyproject text with ``pkg`` pinned to ``latest_version`` for that Python."""
    new_lines = []
    in_deps = False
    dep_added = False
    dep_string = f"    \"{pkg}=={latest_version}; python_version=='{python_version}'\","
    for line in pyproject_text.splitlines():
        new_lines.append(line)
        if line.strip() == "dependencies = [":
            in_deps = True
        elif in_deps and line.strip().startswith("]") and not dep_added:
            new_lines.insert(-1, dep_string)
            dep_added = True
            in_deps = False
    if not dep_added:
        new_lines.append(dep_string)
    return "\n".join(new_lines) + "\n"


def read_pypi_json(pypi_url: str) -> dict:
    with urllib.request.urlopen(pypi_url, timeout=PYPI_FETCH_TIMEOUT_SECONDS) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_retry_after_seconds(error: HTTPError) -> float | None:
    """Return the Retry-After delay PyPI asked for, or None if it did not ask in seconds."""
    retry_after = error.headers.get("Retry-After") if error.headers else None
    if not retry_after:
        return None
    try:
        return max(0.0, float(retry_after))
    except ValueError:
        # The header also has an HTTP-date form; fall back to the backoff schedule for it.
        return None


def compute_backoff_seconds(attempt: int) -> float:
    return PYPI_FETCH_BACKOFF_SECONDS * 2**attempt


def fetch_pypi_data(pkg: str) -> dict:
    """Fetch one package's PyPI metadata, retrying while PyPI is throttling or erroring.

    The final attempt is made outside the loop so whatever it raises reaches the caller
    unchanged - a package that is genuinely gone still fails against itself.
    """
    pypi_url = f"https://pypi.org/pypi/{pkg}/json"
    for attempt in range(PYPI_FETCH_MAX_ATTEMPTS - 1):
        try:
            return read_pypi_json(pypi_url)
        except HTTPError as e:
            if e.code not in PYPI_RETRIABLE_STATUS_CODES:
                raise
            time.sleep(get_retry_after_seconds(e) or compute_backoff_seconds(attempt))
        except (URLError, OSError):
            time.sleep(compute_backoff_seconds(attempt))
    return read_pypi_json(pypi_url)


def iter_pypi_data(package_names: list[str]) -> Iterator[dict | BaseException]:
    """Yield each package's PyPI metadata in order, fetching a batch at a time.

    A failed fetch is yielded as its exception rather than raised, so one unreachable
    package is reported against that package alone, as it was when the fetches ran one
    at a time. Fetching stays batched instead of prefetching everything because a single
    project's release history can be tens of megabytes - only one batch is ever alive.
    """

    def fetch(pkg: str) -> dict | BaseException:
        try:
            return fetch_pypi_data(pkg)
        except (HTTPError, URLError, OSError, json.JSONDecodeError) as e:
            return e

    with ThreadPoolExecutor(max_workers=PYPI_FETCH_PARALLELISM) as executor:
        for start in range(0, len(package_names), PYPI_FETCH_PARALLELISM):
            yield from executor.map(fetch, package_names[start : start + PYPI_FETCH_PARALLELISM])


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
    status_counts: dict[str, int] = {"ok": 0, "new": 0, "warning": 0, "critical": 0}
    # Collected while the table is printed and explained together afterwards, in one container.
    candidates: list[UpgradeCandidate] = []

    pypi_data = iter_pypi_data([pkg for pkg, _ in packages])
    for (pkg, pinned_version), data in zip(packages, pypi_data):
        try:
            if isinstance(data, BaseException):
                raise data
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
                candidates.append(UpgradeCandidate(pkg, pinned_version, latest_version))
        except HTTPError as e:
            console_print(f"[bold red]Error fetching {pkg} from PyPI: HTTP {e.code}[/]")
            continue
        except URLError as e:
            console_print(f"[bold red]Error fetching {pkg} from PyPI: {e.reason}[/]")
            continue
        except (OSError, json.JSONDecodeError) as e:
            console_print(f"[bold red]Error fetching {pkg} from PyPI: {e}[/]")
            continue
    explanations = (
        explain_upgrades(
            candidates=candidates,
            python_version=python_version,
            airflow_constraints_mode=airflow_constraints_mode,
            github_repository=github_repository,
        )
        if candidates
        else []
    )
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
    clean = ANSI_ESCAPE_RE.sub("", text)
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


@contextmanager
def preserve_files(*paths: Path):
    """Restore the given files' contents on exit — ``uv sync`` rewrites ``uv.lock``."""
    originals = {path: path.read_text() for path in paths}
    try:
        yield
    finally:
        for path, content in originals.items():
            path.write_text(content)


def get_additional_sync_args(airflow_constraints_mode: str) -> list[str]:
    if airflow_constraints_mode == "constraints-source-providers":
        # In case of source constraints we also need to add all development dependencies
        # to reflect exactly what is installed in the CI image by default. The ``ci-image``
        # group aggregates dev/docs/docs-gen plus any hard-to-install provider extras
        # (see root pyproject.toml).
        return ["--group", "ci-image"]
    return []


# Every ``execute_command_in_shell`` call is a fresh ``docker compose run --rm`` container,
# which costs more than the resolution it wraps. All the explanations therefore run as one
# script in a single container, with these markers echoed around each step so the combined
# log can be sliced back into one section per package.
SECTION_MARKER = "===BREEZE_EXPLAIN_SECTION==="
RETURN_CODE_MARKER = "===BREEZE_EXPLAIN_RC==="
FREEZE_MARKER = "===BREEZE_RESOLVED_FREEZE==="
BASELINE_SECTION = "baseline"
CONTAINER_WORKSPACE_PATH = "/opt/airflow"
CONTAINER_FILES_PATH = "/files"


@dataclass
class UpgradeCandidate:
    """An outdated package and the version it would have to reach to be up to date."""

    pkg: str
    pinned_version: str
    latest_version: str


@dataclass
class ResolvedSection:
    """One step's slice of the batched run: what uv said, and what it resolved to."""

    name: str
    returncode: int
    text: str = ""
    versions: dict[str, str] = field(default_factory=dict)


def build_section(
    name: str, command: str, *, prepare: list[str] | None = None, freeze: bool = False
) -> list[str]:
    """Wrap one command in the markers that make its output findable in the combined log.

    The return code is echoed rather than acted on: one package failing to resolve is a
    result to report, not a reason to abandon the remaining packages.
    """
    lines = [f"echo {shlex.quote(f'{SECTION_MARKER} {name}')}"]
    lines.extend(prepare or [])
    lines.append(command)
    lines.append(f'echo "{RETURN_CODE_MARKER} $?"')
    if freeze:
        lines.append(f"echo {shlex.quote(FREEZE_MARKER)}")
        lines.append("uv pip freeze")
    return lines


def build_uv_sync_command(python_version: str, airflow_constraints_mode: str, refresh: bool) -> str:
    return shlex.join(
        [
            "uv",
            "sync",
            "--all-packages",
            *get_additional_sync_args(airflow_constraints_mode),
            "--resolution",
            "highest",
            *(["--refresh"] if refresh else []),
            "--python",
            python_version,
        ]
    )


def get_pyproject_copy_name(index: int) -> str:
    return f"pyproject-{index}.toml"


def get_section_name(index: int, pkg: str) -> str:
    """Name a package's section by position as well as name, so two sections can never collide."""
    return f"{index}-{pkg}"


def build_resolution_script(
    *,
    plan_path: str,
    candidates: list[UpgradeCandidate],
    python_version: str,
    airflow_constraints_mode: str,
) -> str:
    """Chain the baseline resolution and one resolution per candidate into a single script.

    Only the baseline refreshes uv's caches. It re-reads every index page immediately before
    the pinned resolutions run, so they already see today's releases; refreshing again per
    package would re-download the metadata of the whole workspace for each one.
    """
    lines = [f"cd {CONTAINER_WORKSPACE_PATH}"]
    lines.extend(
        build_section(
            BASELINE_SECTION,
            build_uv_sync_command(python_version, airflow_constraints_mode, refresh=True),
            freeze=True,
        )
    )
    pinned_sync = build_uv_sync_command(python_version, airflow_constraints_mode, refresh=False)
    for index, candidate in enumerate(candidates):
        lines.extend(
            build_section(
                get_section_name(index, candidate.pkg),
                pinned_sync,
                # ``cp``, never ``mv``: both files are bind-mounted individually, so replacing
                # the inode would detach the container's copy from the host's. Restoring the
                # lock matters as much as pinning - uv prefers versions already locked, so a
                # lock left behind by the previous package would skew this resolution.
                prepare=[
                    f"cp {plan_path}/{get_pyproject_copy_name(index)} pyproject.toml",
                    f"cp {plan_path}/uv.lock uv.lock",
                ],
                freeze=True,
            )
        )
    return "\n".join(lines)


def build_conflict_probe_script(probe_pins: list[list[str]], python_version: str) -> str:
    """Chain the conflict probes of every package that needs one into a single script.

    A fresh ``uv pip compile`` of just the package at its target version plus the packages it
    would otherwise displace (held at their current versions) is a contradiction, so uv fails
    and prints exactly why they cannot coexist. Running it from scratch (rather than against
    the workspace) keeps the output to the conflict itself.
    """
    lines = [f"cd {CONTAINER_WORKSPACE_PATH}"]
    for index, pins in enumerate(probe_pins):
        printf_cmd = "printf '%s\\n' " + " ".join(shlex.quote(pin) for pin in pins)
        lines.extend(
            build_section(
                str(index),
                f"{printf_cmd} | uv pip compile - --python {shlex.quote(python_version)}",
            )
        )
    return "\n".join(lines)


def split_sections(text: str) -> dict[str, ResolvedSection]:
    """Slice a batched run's log into ``{section name: section}``.

    A section whose marker never appears is simply absent from the result - the caller reports
    that package as unexplained rather than attributing another package's resolution to it.
    """
    sections: dict[str, ResolvedSection] = {}
    current: ResolvedSection | None = None
    body: list[str] = []

    def close_section():
        if current is None:
            return
        current.text = "\n".join(body)
        if FREEZE_MARKER in current.text:
            current.versions = parse_freeze(current.text.split(FREEZE_MARKER, 1)[1])
        sections[current.name] = current

    for line in ANSI_ESCAPE_RE.sub("", text).splitlines():
        stripped = line.strip()
        if stripped.startswith(SECTION_MARKER):
            close_section()
            current = ResolvedSection(name=stripped[len(SECTION_MARKER) :].strip(), returncode=-1)
            body = []
        elif current is None:
            continue
        elif stripped.startswith(RETURN_CODE_MARKER):
            return_code = stripped[len(RETURN_CODE_MARKER) :].strip()
            current.returncode = int(return_code) if return_code.isdigit() else -1
        else:
            body.append(line)
    close_section()
    return sections


def run_script_in_container(
    *, script: str, title: str, python_version: str, github_repository: str | None
) -> str:
    output = Output(title=title, file_name=get_temp_file_name())
    execute_command_in_shell(
        ShellParams(
            github_repository=github_repository,
            python=python_version,
            mount_sources=MOUNT_SELECTED,
        ),
        project_name="breeze-constraints",
        command=shlex.join(["bash", "-c", script]),
        output=output,
        signal_error=False,
    )
    return Path(output.file_name).read_text()


@contextmanager
def write_resolution_plan(candidates: list[UpgradeCandidate], python_version: str):
    """Lay out one patched ``pyproject.toml`` per candidate where the container can read them.

    ``files/`` is mounted into the container, so the whole batch's inputs are written once and
    the script only has to copy them into place.
    """
    FILES_PATH.mkdir(parents=True, exist_ok=True)
    plan_path = Path(tempfile.mkdtemp(prefix="constraints-explain-", dir=FILES_PATH))
    try:
        pyproject_text = (AIRFLOW_ROOT_PATH / "pyproject.toml").read_text()
        shutil.copy(AIRFLOW_ROOT_PATH / "uv.lock", plan_path / "uv.lock")
        for index, candidate in enumerate(candidates):
            (plan_path / get_pyproject_copy_name(index)).write_text(
                pin_dependency_in_pyproject(
                    pyproject_text, candidate.pkg, candidate.latest_version, python_version
                )
            )
        yield plan_path
    finally:
        shutil.rmtree(plan_path, ignore_errors=True)


def resolve_upgrade_candidates(
    *,
    candidates: list[UpgradeCandidate],
    python_version: str,
    airflow_constraints_mode: str,
    github_repository: str | None,
) -> dict[str, ResolvedSection]:
    """Resolve the workspace once as it stands and once per candidate, all in one container.

    The unpinned resolution is what actually generates the constraints, so it is the ground
    truth for "what would the constraints pick" and is shared by every explanation. Each
    candidate is then resolved with its own version pinned, always from the same starting
    point: the script restores ``pyproject.toml`` and ``uv.lock`` before every step.
    """
    with (
        preserve_files(AIRFLOW_ROOT_PATH / "pyproject.toml", AIRFLOW_ROOT_PATH / "uv.lock"),
        write_resolution_plan(candidates, python_version) as plan_path,
    ):
        text = run_script_in_container(
            script=build_resolution_script(
                plan_path=f"{CONTAINER_FILES_PATH}/{plan_path.name}",
                candidates=candidates,
                python_version=python_version,
                airflow_constraints_mode=airflow_constraints_mode,
            ),
            title="output_resolutions",
            python_version=python_version,
            github_repository=github_repository,
        )
    return split_sections(text)


def run_conflict_probes(
    *, probe_pins: list[list[str]], python_version: str, github_repository: str | None
) -> list[str]:
    """Return uv's own conflict narrative for each set of pins that cannot coexist."""
    text = run_script_in_container(
        script=build_conflict_probe_script(probe_pins, python_version),
        title="output_conflict_probes",
        python_version=python_version,
        github_repository=github_repository,
    )
    sections = split_sections(text)
    conflicts = []
    for index in range(len(probe_pins)):
        section = sections.get(str(index))
        conflicts.append(extract_uv_conflict(section.text) if section else "")
    return conflicts


def classify_package_upgrade(
    *, candidate: UpgradeCandidate, baseline: ResolvedSection, section: ResolvedSection | None
) -> tuple[str, list[str]]:
    """Turn one candidate's resolution into a verdict, plus the pins that would prove it.

    The pins are returned rather than probed here so that every package that needs uv's
    conflict narrative can be probed together, in one more container rather than one each.
    """
    from packaging.utils import canonicalize_name

    pkg, pinned_version, latest_version = (
        candidate.pkg,
        candidate.pinned_version,
        candidate.latest_version,
    )
    explanation = (
        f"[bold blue]\n--- Explaining for {pkg} (current: {pinned_version}, latest: {latest_version}) ---[/]"
    )
    if section is None:
        return (
            explanation + f"\n[bold yellow]The resolver run produced no output for {pkg}, so the "
            f"upgrade to {latest_version} could not be classified.[/]",
            [],
        )

    canonical_pkg = str(canonicalize_name(pkg))
    baseline_version = baseline.versions.get(canonical_pkg)
    # A zero exit code only proves that *some* valid resolution exists with the pin — not
    # that --resolution highest would ever select it. Inspect what was actually resolved:
    # if honouring the pin forced *other* packages to be downgraded, the unpinned highest
    # resolution (i.e. the constraints) keeps the package at its lower version, so this is
    # NOT a clean upgrade.
    resolved_version = section.versions.get(canonical_pkg)
    downgrades = find_downgrades(baseline.versions, section.versions, exclude=canonical_pkg)

    if section.returncode != 0:
        # Forcing the package to its latest version produced no valid resolution at all:
        # a genuine hard conflict. Surface uv's own conflict narrative from the sync log.
        explanation += (
            f"\n[bold red]Package {pkg} CANNOT be upgraded to {latest_version}: "
            f"uv could not resolve the workspace with {pkg}=={latest_version} pinned "
            f"(hard conflict).[/]"
        )
        conflict = extract_uv_conflict(section.text)
        if conflict:
            explanation += f"\n\n[bold yellow]Conflict as reported by uv:[/]\n{conflict}"
    elif not baseline.versions or not section.versions:
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
        return explanation, [
            f"{pkg}=={latest_version}",
            *(f"{name}=={before_version}" for name, before_version, _ in downgrades),
        ]
    else:
        explanation += (
            f"\n[bold green]Package {pkg} can be upgraded from {pinned_version} to "
            f"{latest_version} without conflicts and without downgrading other packages.[/]"
            f"\n[dim]If this result is unexpected, run 'uv cache clean' and retry — a stale "
            f"uv cache can make breeze resolve against an out-of-date environment.[/]"
        )
    return explanation, []


def explain_upgrades(
    *,
    candidates: list[UpgradeCandidate],
    python_version: str,
    airflow_constraints_mode: str,
    github_repository: str | None,
) -> list[str]:
    """Explain why each outdated package is not at its latest version."""
    sections = resolve_upgrade_candidates(
        candidates=candidates,
        python_version=python_version,
        airflow_constraints_mode=airflow_constraints_mode,
        github_repository=github_repository,
    )
    baseline = sections.get(BASELINE_SECTION) or ResolvedSection(name=BASELINE_SECTION, returncode=-1)
    explanations: list[str] = []
    probe_indexes: list[int] = []
    probe_pins: list[list[str]] = []
    for index, candidate in enumerate(candidates):
        explanation, pins = classify_package_upgrade(
            candidate=candidate,
            baseline=baseline,
            section=sections.get(get_section_name(index, candidate.pkg)),
        )
        if pins:
            probe_indexes.append(len(explanations))
            probe_pins.append(pins)
        explanations.append(explanation)

    if probe_pins:
        conflicts = run_conflict_probes(
            probe_pins=probe_pins, python_version=python_version, github_repository=github_repository
        )
        for index, pins, conflict in zip(probe_indexes, probe_pins, conflicts):
            explanations[index] += (
                f"\n\n[bold yellow]Conflict as reported by uv (uv pip compile {' '.join(pins)}):[/]\n"
            )
            explanations[index] += conflict or "[dim](uv did not emit a conflict narrative)[/]"

    if get_verbose():
        # Full resolver logs of both phases — only when explicitly requested, since they
        # are very long (each is a complete uv sync plus freeze).
        for index, candidate in enumerate(candidates):
            section = sections.get(get_section_name(index, candidate.pkg))
            explanations[index] += (
                f"\n\n[yellow]--- uv resolver output: phase 1, baseline (no pin) ---[/]\n{baseline.text}"
                f"\n[yellow]--- uv resolver output: phase 2, with "
                f"{candidate.pkg}=={candidate.latest_version} pinned ---[/]"
                f"\n{section.text if section else ''}"
            )
    return explanations
