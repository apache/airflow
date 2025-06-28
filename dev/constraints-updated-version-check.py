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

import argparse
import json
import re
import urllib.request
from datetime import datetime
from urllib.error import HTTPError, URLError

from packaging import version


def parse_constraints_generation_date(lines):
    for line in lines[:5]:
        if "automatically generated on" in line:
            date_str = line.split("generated on")[-1].strip()
            try:
                return datetime.fromisoformat(date_str).replace(tzinfo=None)
            except ValueError:
                print(f"Warning: Could not parse constraints generation date from: {date_str}")
                return None
    return None


def get_constraints_file(python_version):
    url = f"https://raw.githubusercontent.com/apache/airflow/refs/heads/constraints-main/constraints-{python_version}.txt"
    try:
        response = urllib.request.urlopen(url)
        return response.read().decode("utf-8").splitlines()
    except HTTPError as e:
        if e.code == 404:
            print(f"Error: Constraints file for Python {python_version} not found.")
            print(f"URL: {url}")
            print("Please check if the Python version is correct and the file exists.")
        else:
            print(f"HTTP Error: {e.code} - {e.reason}")
        exit(1)
    except URLError as e:
        print(f"Error connecting to GitHub: {e.reason}")
        exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
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


def main():
    parser = argparse.ArgumentParser(
        description="""
Python Package Version Checker for Airflow Constraints

This script checks Python package versions against the Airflow constraints file and reports:
- Current constrained version vs latest available version
- Release dates for both versions
- Status indicator showing how outdated the package is
- Number of versions between constrained and latest version
- Direct PyPI link to the package

Status Indicators:
✅ OK          - Package is up to date
📢 <5d         - Less than 5 days behind latest version
⚠ <30d         - Between 5-30 days behind latest version
🚨 >Xd         - More than X days behind latest version (X = actual days)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--python-version", required=True, help="Python version to check constraints for (e.g., 3.12)"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "diff-constraints", "diff-all"],
        default="diff-constraints",
        help="""
Operation modes:
  full            : Show all packages, including up-to-date ones
  diff-constraints: (Default) Show only outdated packages with updates
                   before constraints generation
  diff-all       : Show all outdated packages regardless of update timing
                       """,
    )

    args = parser.parse_args()

    lines = get_constraints_file(args.python_version)

    constraints_date = parse_constraints_generation_date(lines)
    if constraints_date:
        print(f"Constraints file generation date: {constraints_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print()

    packages = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#") and "@" not in line:
            match = re.match(r"^([a-zA-Z0-9_.\-]+)==([\w.\-]+)$", line)
            if match:
                packages.append((match.group(1), match.group(2)))

    max_pkg_length = get_max_package_length(packages)

    col_widths = {
        "Library Name": max(35, max_pkg_length),
        "Constraint Version": 18,
        "Constraint Date": 12,
        "Latest Version": 15,
        "Latest Date": 12,
        "Status": 15,
        "# Versions Behind": 16,
        "PyPI Link": 60,
    }

    format_str = (
        f"{{:<{col_widths['Library Name']}}} | "
        f"{{:<{col_widths['Constraint Version']}}} | "
        f"{{:<{col_widths['Constraint Date']}}} | "
        f"{{:<{col_widths['Latest Version']}}} | "
        f"{{:<{col_widths['Latest Date']}}} | "
        f"{{:<{col_widths['Status']}}} | "
        f"{{:>15}} | "
        f"{{:<{col_widths['PyPI Link']}}}"
    )

    headers = [
        "Library Name",
        "Constraint Version",
        "Constraint Date",
        "Latest Version",
        "Latest Date",
        "Status",
        "# Versions Behind",
        "PyPI Link",
    ]

    print(format_str.format(*headers))
    total_width = sum(col_widths.values()) + (len(col_widths) - 1) * 3
    print("=" * total_width)

    outdated_count = 0
    skipped_count = 0

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

                if should_show_package(
                    releases, latest_version, constraints_date, args.mode, is_latest_version
                ):
                    # Use the first newer release date instead of latest version date
                    first_newer_date_str = get_first_newer_release_date_str(releases, pinned_version)
                    status = get_status_emoji(
                        first_newer_date_str or constraint_release_date,
                        datetime.utcnow().strftime("%Y-%m-%d"),  # noqa: TID251
                        is_latest_version,
                    )
                    pypi_link = f"https://pypi.org/project/{pkg}/{latest_version}"

                    print(
                        format_str.format(
                            pkg,
                            pinned_version[: col_widths["Constraint Version"]],
                            constraint_release_date[: col_widths["Constraint Date"]],
                            latest_version[: col_widths["Latest Version"]],
                            latest_release_date[: col_widths["Latest Date"]],
                            status[: col_widths["Status"]],
                            versions_behind_str,
                            pypi_link,
                        )
                    )
                    if not is_latest_version:
                        outdated_count += 1
                    else:
                        skipped_count += 1

        except HTTPError as e:
            print(f"Error fetching {pkg} from PyPI: HTTP {e.code}")
            continue
        except URLError as e:
            print(f"Error fetching {pkg} from PyPI: {e.reason}")
            continue
        except Exception as e:
            print(f"Error processing {pkg}: {str(e)}")
            continue

    print("=" * total_width)
    print(f"\nTotal packages checked: {len(packages)}")
    print(f"Outdated packages found: {outdated_count}")
    if args.mode == "diff-constraints":
        print(f"Skipped packages (updated after constraints generation): {skipped_count}")


if __name__ == "__main__":
    main()
