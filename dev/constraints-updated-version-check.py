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
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "rich",
#   "rich-click",
#   "packaging",
# ]
# ///
from __future__ import annotations

import json
import os
import re
import urllib.request
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError

import rich_click as click
from packaging import version
from rich.console import Console
from rich.syntax import Syntax

console = Console(color_system="standard")


def parse_constraints_generation_date(lines):
    for line in lines[:5]:
        if "automatically generated on" in line:
            date_str = line.split("generated on")[-1].strip()
            try:
                return datetime.fromisoformat(date_str).replace(tzinfo=None)
            except ValueError:
                console.print(
                    f"[yellow]Warning: Could not parse constraints generation date from: {date_str}[/]"
                )
                return None
    return None


def get_constraints_file(python_version, airflow_constraints_mode="constraints"):
    url = f"https://raw.githubusercontent.com/apache/airflow/refs/heads/constraints-main/{airflow_constraints_mode}-{python_version}.txt"
    try:
        response = urllib.request.urlopen(url)
        return response.read().decode("utf-8").splitlines()
    except HTTPError as e:
        if e.code == 404:
            console.print(
                f"[bold red]Error: Constraints file for Python {python_version} and mode {airflow_constraints_mode} not found.[/]"
            )
            console.print(f"[bold red]URL: {url}[/]")
            console.print(
                "[bold red]Please check if the Python version and constraints mode are correct and the file exists.[/]"
            )
        else:
            console.print(f"[bold red]HTTP Error: {e.code} - {e.reason}[/]")
        exit(1)
    except URLError as e:
        console.print(f"[bold red]Error connecting to GitHub: {e.reason}[/]")
        exit(1)
    except Exception as e:
        console.print(f"[bold red]Unexpected error: {str(e)}[/]")
        exit(1)


def count_versions_between(releases, current_version, latest_version):
    try:
        current = version.parse(current_version)
        latest = version.parse(latest_version)

        if current == latest:
            return 0

        valid_versions = [
            v
            for v in releases.keys()
            if releases[v] and not version.parse(v).is_prerelease and current < version.parse(v) <= latest
        ]

        return max(len(valid_versions), 1) if current < latest else 0
    except Exception:
        return 0


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
            return "⚠ <30d          "
        return f"🚨 >{days_diff}d".ljust(15)
    except Exception:
        return "📢 N/A           "


def get_max_package_length(packages):
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
    current = version.parse(current_version)
    newer_versions = [
        version.parse(v)
        for v in releases
        if version.parse(v) > current and releases[v] and not version.parse(v).is_prerelease
    ]
    if not newer_versions:
        return None

    first_newer_version = min(newer_versions)
    upload_time_str = releases[str(first_newer_version)][0]["upload_time_iso_8601"]
    return datetime.fromisoformat(upload_time_str.replace("Z", "+00:00")).strftime("%Y-%m-%d")


def main(
    python_version: str,
    airflow_constraints_mode: str,
    mode: str,
    selected_packages: set[str] | None = None,
    explain_why: bool = False,
    verbose: bool = False,
):
    lines = get_constraints_file(python_version, airflow_constraints_mode)
    constraints_date = parse_constraints_generation_date(lines)
    console.print(f"[bold cyan]Python version:[/] [white]{python_version}[/]")
    console.print(f"[bold cyan]Constraints mode:[/] [white]{airflow_constraints_mode}[/]\n")
    if constraints_date:
        console.print(
            f"[bold cyan]Constraints file generation date:[/] [white]{constraints_date.strftime('%Y-%m-%d %H:%M:%S')}[/]"
        )
        console.print()

    packages = parse_packages_from_lines(lines, selected_packages)
    col_widths, format_str, headers, total_width = get_table_format(packages)
    print_table_header(format_str, headers, total_width)

    outdated_count, skipped_count, explanations = process_packages(
        packages,
        constraints_date,
        mode,
        explain_why,
        verbose,
        col_widths,
        format_str,
        python_version,
        airflow_constraints_mode,
    )

    print_table_footer(total_width, len(packages), outdated_count, skipped_count, mode)
    if explain_why and explanations:
        print_explanations(explanations)


def parse_packages_from_lines(lines: list[str], selected_packages: set[str] | None) -> list[tuple[str, str]]:
    packages = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#") and "@" not in line:
            match = re.match(r"^([a-zA-Z0-9_.\-]+)==([\w.\-]+)$", line)
            if match:
                pkg_name = match.group(1)
                if not selected_packages or (pkg_name in selected_packages):
                    packages.append((pkg_name, match.group(2)))
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
    console.print(f"[bold magenta]{format_str.format(*headers)}[/]")
    console.print(f"[magenta]{'=' * total_width}[/]")


def print_table_footer(total_width: int, total_pkgs: int, outdated_count: int, skipped_count: int, mode: str):
    console.print(f"[magenta]{'=' * total_width}[/]")
    console.print(f"[bold cyan]\nTotal packages checked:[/] [white]{total_pkgs}[/]")
    console.print(f"[bold yellow]Outdated packages found:[/] [white]{outdated_count}[/]")
    if mode == "diff-constraints":
        console.print(
            f"[bold blue]Skipped packages (updated after constraints generation):[/] [white]{skipped_count}[/]"
        )


def print_explanations(explanations: list[str]):
    console.print("\n[bold magenta]Upgrade Explanations:[/]")
    for explanation in explanations:
        console.print(explanation)


def process_packages(
    packages: list[tuple[str, str]],
    constraints_date: datetime | None,
    mode: str,
    explain_why: bool,
    verbose: bool,
    col_widths: dict,
    format_str: str,
    python_version: str,
    airflow_constraints_mode: str,
) -> tuple[int, int, list[str]]:
    import subprocess
    import tempfile
    from contextlib import contextmanager
    from pathlib import Path

    @contextmanager
    def preserve_pyproject_file(pyproject_path: Path):
        original_content = pyproject_path.read_text()
        try:
            yield
        finally:
            pyproject_path.write_text(original_content)

    def run_uv_sync(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env.pop("VIRTUAL_ENV", None)
        if verbose:
            console.print(f"[cyan]Running command:[/] [white]{' '.join(cmd)}[/] [dim]in {cwd}[/]")
        result = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
        return result

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
        if verbose:
            console.print(
                f"[cyan]Fixed {pkg} at {latest_version} in [white]{pyproject_path}[/] [dim](pyproject.toml)[/]"
            )

    airflow_pyproject = Path(__file__).parent.parent / "pyproject.toml"
    airflow_pyproject = airflow_pyproject.resolve()
    repo_root = airflow_pyproject.parent

    outdated_count = 0
    skipped_count = 0
    explanations = []

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
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
                        pkg,
                        pinned_version,
                        latest_version,
                        airflow_pyproject,
                        repo_root,
                        temp_dir_path,
                        run_uv_sync,
                        update_pyproject_dependency,
                        verbose,
                        python_version,
                        airflow_constraints_mode,
                    )
                    explanations.append(explanation)
            except HTTPError as e:
                console.print(f"[bold red]Error fetching {pkg} from PyPI: HTTP {e.code}[/]")
                continue
            except URLError as e:
                console.print(f"[bold red]Error fetching {pkg} from PyPI: {e.reason}[/]")
                continue
            except Exception as e:
                console.print(f"[bold red]Error processing {pkg}: {str(e)}[/]")
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
        else ("yellow" if status.startswith("📢") or status.startswith("⚠") else "red")
    )
    string_to_print = format_str.format(
        pkg,
        pinned_version[: col_widths["Constraint Version"]],
        constraint_release_date[: col_widths["Constraint Date"]],
        latest_version[: col_widths["Latest Version"]],
        latest_release_date[: col_widths["Latest Date"]],
        status[: col_widths["📢 Status"]],
        versions_behind_str,
        pypi_link,
    )
    console.print(f"[{color}]{string_to_print}[/]")


def explain_package_upgrade(
    pkg: str,
    pinned_version: str,
    latest_version: str,
    airflow_pyproject: Path,
    repo_root: Path,
    temp_dir_path: Path,
    run_uv_sync,
    update_pyproject_dependency,
    verbose: bool,
    python_version: str,
    airflow_constraints_mode: str,
) -> str:
    explanation = (
        f"[bold blue]\n--- Explaining for {pkg} (current: {pinned_version}, latest: {latest_version}) ---[/]"
    )

    @contextmanager
    def preserve_pyproject_file(pyproject_path: Path):
        original_content = pyproject_path.read_text()
        try:
            yield
        finally:
            pyproject_path.write_text(original_content)

    additional_args = []
    if airflow_constraints_mode == "constraints-source-providers":
        # In case of source constraints we also need to add all development dependencies
        # to reflect exactly what is installed in the CI image by default
        additional_args.extend(["--group dev", "--group docs", "--group docs-gen", "--group leveldb"])
    with preserve_pyproject_file(airflow_pyproject):
        before_result = run_uv_sync(
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
            ],
            cwd=repo_root,
        )
        (temp_dir_path / "uv_sync_before.txt").write_text(before_result.stdout + before_result.stderr)
        update_pyproject_dependency(airflow_pyproject, pkg, latest_version)
        if verbose:
            syntax = Syntax(
                airflow_pyproject.read_text(), "toml", theme="monokai", line_numbers=True, word_wrap=False
            )
            explanation += "\n" + str(syntax)
        after_result = run_uv_sync(
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
            cwd=repo_root,
        )
        (temp_dir_path / "uv_sync_after.txt").write_text(after_result.stdout + after_result.stderr)
        if after_result.returncode == 0:
            explanation += f"\n[bold yellow]Package {pkg} can be upgraded from {pinned_version} to {latest_version} without conflicts.[/]."
        if after_result.returncode != 0 or verbose:
            explanation += f"\n[yellow]uv sync output for {pkg}=={latest_version}:[/]\n"
            explanation += after_result.stdout + after_result.stderr
    return explanation


@click.command()
@click.option(
    "--python-version",
    type=click.Choice(["3.10", "3.11", "3.12", "3.13"]),
    default="3.10",
    help="Python version to check constraints for (e.g. 3.10, 3.11, 3.12, 3.13).",
)
@click.option(
    "--airflow-constraints-mode",
    type=click.Choice(
        ["constraints", "constraints-no-providers", "constraints-source-providers"], case_sensitive=False
    ),
    default="constraints",
    show_default=True,
    help="Constraints mode to use: constraints, constraints-no-providers, constraints-source-providers.",
)
@click.option(
    "--mode",
    type=click.Choice(["full", "diff-all", "diff-constraints"], case_sensitive=False),
    default="full",
    show_default=True,
    help="Report mode: full, diff-all, diff-constraints.",
)
@click.option(
    "--package",
    multiple=True,
    help="Only check specific package(s). Can be used multiple times.",
)
@click.option(
    "--explain-why/--no-explain-why",
    default=False,
    help="Show explanations for outdated packages.",
)
@click.option(
    "--verbose/--no-verbose",
    default=False,
    help="Show verbose output.",
)
def cli(python_version, airflow_constraints_mode, mode, package, explain_why, verbose):
    if os.environ.get("CI", "false") == "true":
        # Show output outside the group in CI
        print("::endgroup::")
    selected_packages = set(package) if package else None
    main(python_version, airflow_constraints_mode, mode, selected_packages, explain_why, verbose)


if __name__ == "__main__":
    cli()
