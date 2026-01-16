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

"""
Verify NOTICE files in Airflow packages.

This script checks:
1. Required ASF notice text is present
2. Copyright year range ends with current year
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

# ANSI color codes for output
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def check_notice_file(notice_path: Path, current_year: int, verbose: bool = False) -> tuple[bool, list[str]]:
    """
    Check a single NOTICE file for required content.
    """
    errors = []

    try:
        content = notice_path.read_text()
    except Exception as e:
        return False, [f"Failed to read file: {e}"]

    # Check 1: ASF notice is present
    required_asf_text = "Apache Software Foundation"
    if required_asf_text not in content:
        errors.append(f"Missing required text: '{required_asf_text}'")

    # Check 2: Copyright year ends with current year
    # Only check the main "Apache Airflow" copyright line, not third-party components
    import re

    # Look for Apache Airflow copyright specifically
    # This should be in the first few lines and contain "Apache" or "The Apache Software Foundation"
    lines = content.split('\n')
    apache_copyright_found = False
    
    for i, line in enumerate(lines[:10]):  # Check first 10 lines only for ASF copyright
        if 'Apache' in line and 'Copyright' in line:
            # Look for year pattern in this line
            copyright_pattern = r"Copyright\s+(\d{4})-(\d{4})"
            range_matches = re.findall(copyright_pattern, line)
            
            if range_matches:
                apache_copyright_found = True
                for start_year, end_year in range_matches:
                    end_year_int = int(end_year)
                    if end_year_int != current_year:
                        errors.append(
                            f"Apache Airflow copyright year range ends with {end_year_int}, should end with {current_year}"
                        )
                break
            else:
                # Try single year pattern
                single_year_pattern = r"Copyright\s+(\d{4})\s"
                single_matches = re.findall(single_year_pattern, line)
                if single_matches:
                    apache_copyright_found = True
                    year = int(single_matches[0])
                    if year != current_year:
                        errors.append(
                            f"Apache Airflow copyright year is {year}, should be {current_year}"
                        )
                    break
    
    if not apache_copyright_found:
        errors.append("No Apache Airflow copyright year found in NOTICE file")

    if verbose and not errors:
        try:
            rel_path = notice_path.relative_to(Path.cwd())
        except ValueError:
            # For distribution files in /tmp, just use the filename
            rel_path = notice_path.name
        print(f"{GREEN}✓{RESET} {rel_path}")

    return len(errors) == 0, errors


def find_notice_files(root_dir: Path, check_sources: bool = True, check_dist: bool = False) -> list[Path]:
    """
    Find all NOTICE files to verify.
    """
    notice_files = []

    if check_sources:
        # Find NOTICE files in source tree
        # Root NOTICE
        root_notice = root_dir / "NOTICE"
        if root_notice.exists():
            notice_files.append(root_notice)

        # Provider NOTICE files
        providers_dir = root_dir / "providers"
        if providers_dir.exists():
            for notice in providers_dir.rglob("NOTICE"):
                notice_files.append(notice)

        # Other component NOTICE files
        for component in ["airflow-core", "airflow-ctl", "chart", "clients/python", "go-sdk"]:
            component_notice = root_dir / component / "NOTICE"
            if component_notice.exists():
                notice_files.append(component_notice)

    if check_dist:
        # Check NOTICE files in built distributions
        dist_dir = root_dir / "dist"
        if dist_dir.exists():
            # Extract and check NOTICE from .tar.gz and .whl files
            import tarfile
            import zipfile

            for archive in dist_dir.glob("*.tar.gz"):
                try:
                    with tarfile.open(archive, "r:gz") as tar:
                        for member in tar.getmembers():
                            if member.name.endswith("NOTICE"):
                                # Extract to temp location and check
                                notice_content = tar.extractfile(member)
                                if notice_content:
                                    temp_path = Path(f"/tmp/NOTICE_{archive.name}")
                                    temp_path.write_bytes(notice_content.read())
                                    notice_files.append(temp_path)
                except Exception:
                    pass  # Skip archives that can't be read

            for wheel in dist_dir.glob("*.whl"):
                try:
                    with zipfile.ZipFile(wheel, "r") as zip_file:
                        for name in zip_file.namelist():
                            if name.endswith("NOTICE"):
                                notice_content = zip_file.read(name)
                                temp_path = Path(f"/tmp/NOTICE_{wheel.name}")
                                temp_path.write_bytes(notice_content)
                                notice_files.append(temp_path)
                except Exception:
                    pass  # Skip wheels that can't be read

    return notice_files


def main():
    """Main verification function."""
    parser = argparse.ArgumentParser(description="Verify NOTICE files in Airflow packages")
    parser.add_argument(
        "--year",
        type=int,
        default=datetime.now().year,
        help="Expected copyright year (default: current year)",
    )
    parser.add_argument(
        "--sources-only",
        action="store_true",
        help="Only check NOTICE files in source tree",
    )
    parser.add_argument(
        "--dist-only",
        action="store_true",
        help="Only check NOTICE files in distribution packages",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose output",
    )
    parser.add_argument(
        "--root-dir",
        type=Path,
        default=Path.cwd(),
        help="Root directory of Airflow repository (default: current directory)",
    )

    args = parser.parse_args()

    # Determine what to check
    check_sources = not args.dist_only
    check_dist = not args.sources_only

    print(f"\n{'='*60}")
    print(f"NOTICE Files Verification")
    print(f"{'='*60}")
    print(f"Expected copyright year: {args.year}")
    print(f"Checking sources: {check_sources}")
    print(f"Checking distributions: {check_dist}")
    print(f"{'='*60}\n")

    # Find all NOTICE files
    notice_files = find_notice_files(args.root_dir, check_sources, check_dist)

    if not notice_files:
        print(f"{RED}✗ No NOTICE files found!{RESET}")
        return 1

    print(f"Found {len(notice_files)} NOTICE file(s) to verify\n")

    # Check each NOTICE file
    failed_files = []
    for notice_path in notice_files:
        is_valid, errors = check_notice_file(notice_path, args.year, args.verbose)

        if not is_valid:
            rel_path = notice_path.relative_to(args.root_dir) if notice_path.is_relative_to(
                args.root_dir
            ) else notice_path
            failed_files.append((rel_path, errors))
            print(f"{RED}✗{RESET} {rel_path}")
            for error in errors:
                print(f"  - {error}")

    # Summary
    print(f"\n{'='*60}")
    if failed_files:
        print(f"{RED}VERIFICATION FAILED{RESET}")
        print(f"Files with issues: {len(failed_files)}/{len(notice_files)}")
        print(f"\nFailed files:")
        for rel_path, _ in failed_files:
            print(f"  - {rel_path}")
        return 1
    else:
        print(f"{GREEN}✓ ALL NOTICE FILES VALID{RESET}")
        print(f"Verified {len(notice_files)} file(s) successfully")
        return 0


if __name__ == "__main__":
    sys.exit(main())
