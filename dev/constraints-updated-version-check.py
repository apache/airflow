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
from datetime import datetime
from urllib.error import HTTPError, URLError

import rich_click as click
from packaging import version
from rich.console import Console

console = Console(width=400, color_system="standard")


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


def get_constraints_file(python_version):
    url = f"https://raw.githubusercontent.com/apache/airflow/refs/heads/constraints-main/constraints-{python_version}.txt"
    try:
        response = urllib.request.urlopen(url)
        return response.read().decode("utf-8").splitlines()
    except HTTPError as e:
        if e.code == 404:
            console.print(f"[bold red]Error: Constraints file for Python {python_version} not found.[/]")
            console.print(f"[bold red]URL: {url}[/]")
            console.print("[bold red]Please check if the Python version is correct and the file exists.[/]")
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


def main(python_version, mode, selected_packages=None, explain_why=False):
    lines = get_constraints_file(python_version)

    constraints_date = parse_constraints_generation_date(lines)
    if constraints_date:
        console.print(
            f"[bold cyan]Constraints file generation date:[/] [white]{constraints_date.strftime('%Y-%m-%d %H:%M:%S')}[/]"
        )
        console.print()

    packages = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#") and "@" not in line:
            match = re.match(r"^([a-zA-Z0-9_.\-]+)==([\w.\-]+)$", line)
            if match:
                pkg_name = match.group(1)
                if not selected_packages or (pkg_name in selected_packages):
                    packages.append((pkg_name, match.group(2)))

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

    if not explain_why:
        console.print(f"[bold magenta]{format_str.format(*headers)}[/]")
        total_width = sum(col_widths.values()) + (len(col_widths) - 1) * 3
        console.print(f"[magenta]{'=' * total_width}[/]")
    else:
        total_width = sum(col_widths.values()) + (len(col_widths) - 1) * 3

    outdated_count = 0
    skipped_count = 0

    if explain_why and packages:
        import shutil
        import subprocess
        from contextlib import contextmanager
        from pathlib import Path

        @contextmanager
        def pyproject_backup_restore(pyproject_path):
            backup_path = pyproject_path.with_suffix(pyproject_path.suffix + ".bak")
            shutil.copyfile(pyproject_path, backup_path)
            try:
                yield
            finally:
                if backup_path.exists():
                    shutil.copyfile(backup_path, pyproject_path)
                    backup_path.unlink()

        def run_uv_sync(cmd, cwd):
            env = os.environ.copy()
            env.pop("VIRTUAL_ENV", None)
            result = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
            return result.stdout + result.stderr

        airflow_pyproject = Path(__file__).parent.parent / "pyproject.toml"
        airflow_pyproject = airflow_pyproject.resolve()
        repo_root = airflow_pyproject.parent
        with pyproject_backup_restore(airflow_pyproject):
            for pkg, _ in packages:
                console.print(f"[bold blue]\n--- Explaining for {pkg} ---[/]")
                # 1. Run uv sync --all-packages
                before_output = run_uv_sync(
                    ["uv", "sync", "--all-packages", "--resolution", "highest"], cwd=repo_root
                )
                with open("/tmp/uv_sync_before.txt", "w") as f:
                    f.write(before_output)
                # 2. Get latest version from PyPI
                pypi_url = f"https://pypi.org/pypi/{pkg}/json"
                with urllib.request.urlopen(pypi_url) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    latest_version = data["info"]["version"]
                # 3. Update pyproject.toml (append dependency line)
                lines = airflow_pyproject.read_text().splitlines()
                new_lines = []
                in_deps = False
                dep_added = False
                for line in lines:
                    new_lines.append(line)
                    if line.strip() == "dependencies = [":
                        in_deps = True
                    elif in_deps and line.strip().startswith("]") and not dep_added:
                        # Add the new dependency just before closing bracket
                        new_lines.insert(-1, f'    "{pkg}=={latest_version}",')
                        dep_added = True
                        in_deps = False
                if not dep_added:
                    # fallback: just append at the end
                    new_lines.append(f'    "{pkg}=={latest_version}",')
                airflow_pyproject.write_text("\n".join(new_lines) + "\n")
                # 4. Run uv sync --all-packages and capture output
                after_output = run_uv_sync(
                    ["uv", "sync", "--all-packages", "--resolution", "highest"], cwd=repo_root
                )
                with open("/tmp/uv_sync_after.txt", "w") as f:
                    f.write(after_output)
                console.print(f"[yellow]uv sync output for {pkg}=={latest_version}:[/]")
                console.print(after_output)
        return

    for pkg, pinned_version in packages:
        try:
            pypi_url = f"https://pypi.org/pypi/{pkg}/json"
            with urllib.request.urlopen(pypi_url) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                latest_version = data["info"]["version"]
                releases = data["releases"]

                latest_release_date = "N/A"
                constraint_release_date = "N/A"

                if releases.get(latest_version):
                    latest_release_date = (
                        datetime.fromisoformat(
                            releases[latest_version][0]["upload_time_iso_8601"].replace("Z", "+00:00")
                        )
                        .replace(tzinfo=None)
                        .strftime("%Y-%m-%d")
                    )

                if releases.get(pinned_version):
                    constraint_release_date = (
                        datetime.fromisoformat(
                            releases[pinned_version][0]["upload_time_iso_8601"].replace("Z", "+00:00")
                        )
                        .replace(tzinfo=None)
                        .strftime("%Y-%m-%d")
                    )

                is_latest_version = pinned_version == latest_version
                versions_behind = count_versions_between(releases, pinned_version, latest_version)
                versions_behind_str = str(versions_behind) if versions_behind > 0 else ""

                if should_show_package(releases, latest_version, constraints_date, mode, is_latest_version):
                    # Use the first newer release date instead of latest version date
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
                        # The utf character takes 2 bytes so we need to subtract 1
                        status[: col_widths["📢 Status"]],
                        versions_behind_str,
                        pypi_link,
                    )
                    console.print(f"[{color}]{string_to_print}[/]")
                    if not is_latest_version:
                        outdated_count += 1
                else:
                    skipped_count += 1

        except HTTPError as e:
            console.print(f"[bold red]Error fetching {pkg} from PyPI: HTTP {e.code}[/]")
            continue
        except URLError as e:
            console.print(f"[bold red]Error fetching {pkg} from PyPI: {e.reason}[/]")
            continue
        except Exception as e:
            console.print(f"[bold red]Error processing {pkg}: {str(e)}[/]")
            continue

    console.print(f"[magenta]{'=' * total_width}[/]")
    console.print(f"[bold cyan]\nTotal packages checked:[/] [white]{len(packages)}[/]")
    console.print(f"[bold yellow]Outdated packages found:[/] [white]{outdated_count}[/]")
    if mode == "diff-constraints":
        console.print(
            f"[bold blue]Skipped packages (updated after constraints generation):[/] [white]{skipped_count}[/]"
        )


@click.command()
@click.option(
    "--python-version",
    required=False,
    default="3.10",
    help="Python version to check constraints for (e.g., 3.12)",
)
@click.option(
    "--mode",
    type=click.Choice(["full", "diff-constraints", "diff-all"]),
    default="diff-constraints",
    show_default=True,
    help="Operation mode: full, diff-constraints, or diff-all.",
)
@click.option(
    "--selected-packages",
    required=False,
    default=None,
    help="Comma-separated list of package names to check (e.g., 'requests,flask'). If not set, all packages are checked.",
)
@click.option(
    "--explain-why",
    is_flag=True,
    default=False,
    help="For each selected package, attempts to upgrade to the latest version and explains why it cannot be upgraded.",
)
def cli(python_version, mode, selected_packages, explain_why):
    selected_packages_set = None
    if selected_packages:
        selected_packages_set = set(pkg.strip() for pkg in selected_packages.split(",") if pkg.strip())
    main(python_version, mode, selected_packages_set, explain_why)


if __name__ == "__main__":
    cli()
