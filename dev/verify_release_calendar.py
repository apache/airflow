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
# requires-python = ">=3.8"
# dependencies = [
#     "requests",
#     "beautifulsoup4",
#     "icalendar",
#     "rich",
# ]
# ///
"""
Verify that releases planned in Confluence wiki have matching Google Calendar entries.

This script fetches the release plan from the Confluence wiki page and compares it
with the Google Calendar entries to ensure they match.

Release Plan: https://cwiki.apache.org/confluence/display/AIRFLOW/Release+Plan
Calendar iCal: https://calendar.google.com/calendar/ical/c_de214e92df3b759779cb65f3e49e562796c6126e7500cfa7e524bf78186d8b5e%40group.calendar.google.com/public/basic.ics
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from icalendar import Calendar
from rich.console import Console

# Configure console
console = Console()

# Constants
CONFLUENCE_URL = "https://cwiki.apache.org/confluence/display/AIRFLOW/Release+Plan"
CALENDAR_ICAL_URL = (
    "https://calendar.google.com/calendar/ical/"
    "c_de214e92df3b759779cb65f3e49e562796c6126e7500cfa7e524bf78186d8b5e%40group.calendar.google.com/"
    "public/basic.ics"
)


@dataclass
class Release:
    """Represents a planned release."""

    release_type: str  # "Airflow Ctl" or "Providers"
    version: str
    date: datetime
    release_manager: str

    def __str__(self):
        return f"{self.release_type} {self.version} on {self.date.strftime('%Y-%m-%d')} by {self.release_manager}"


@dataclass
class CalendarEntry:
    """Represents a calendar entry."""

    summary: str
    start_date: datetime
    description: str | None = None

    def __str__(self):
        return f"{self.summary} on {self.start_date.strftime('%Y-%m-%d')}"


def fetch_confluence_page() -> str:
    """Fetch the Confluence release plan page with retry logic."""
    console.print(f"[cyan]Fetching Confluence page:[/cyan] {CONFLUENCE_URL}")

    max_retries = 3
    retry_delay = 10

    for attempt in range(1, 1 + max_retries):
        try:
            response = requests.get(CONFLUENCE_URL, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            if attempt < max_retries:
                console.print(f"[yellow]Attempt {attempt}/{max_retries} failed: {e}[/yellow]")
                console.print(f"[yellow]Retrying in {retry_delay} seconds...[/yellow]")
                time.sleep(retry_delay)
            else:
                console.print(f"[red]Failed to fetch Confluence page after {max_retries} attempts:[/red] {e}")
                sys.exit(1)
    return ""


def print_confluence_debug_info(soup: BeautifulSoup) -> None:
    """Print debug information about the Confluence page structure."""
    all_headings = soup.find_all(["h1", "h2", "h3", "h4", "h5"])
    console.print(f"[dim]Found {len(all_headings)} headings in page[/dim]")
    if all_headings:
        console.print("[dim]First 10 headings:[/dim]")
        for heading in all_headings[:10]:
            console.print(f"  [dim]{heading.name}: {heading.get_text(strip=True)[:80]}[/dim]")

    all_tables = soup.find_all("table")
    console.print(f"[dim]Found {len(all_tables)} tables in page[/dim]")


def get_release_sections() -> dict[str, list[str]]:
    """Return the mapping of release types to their possible section names."""
    return {
        "Airflow Ctl": ["Airflow Ctl", "airflow-ctl", "airflow ctl"],
        "Providers": [
            "Support for Airflow in Providers",
            "Provider Releases",
            "Providers",
            "Provider",
            "provider release",
        ],
    }


def find_table_for_heading(heading: Any) -> Any | None:
    """Find the table associated with a heading."""
    # Try to find table as sibling first
    current = heading.find_next_sibling()
    while current:
        if current.name == "table":
            console.print("  [dim]Found table directly after heading[/dim]")
            return current
        if current.name in ["h1", "h2", "h3", "h4", "h5"]:
            # Stop if we hit another heading
            break
        current = current.find_next_sibling()

    # If no table found as sibling, try finding next table in document
    next_table = heading.find_next("table")
    if next_table:
        console.print("  [dim]Found table via find_next[/dim]")
        return next_table

    return None


def find_section_and_parse(soup: BeautifulSoup, release_type: str, section_names: list[str]) -> list[Release]:
    """Find a section by name and parse its table."""
    headings = soup.find_all(["h1", "h2", "h3", "h4", "h5"])
    for section_name in section_names:
        for heading in headings:
            heading_text = heading.get_text(strip=True)
            if section_name.lower() in heading_text.lower():
                console.print(f"[green]Found section:[/green] {heading_text}")
                table = find_table_for_heading(heading)
                if table is not None:
                    return parse_table(table, release_type)
                break
    return []


def parse_confluence_releases(html_content: str) -> list[Release]:
    """Parse releases from Confluence HTML content."""
    console.print("[cyan]Parsing Confluence releases...[/cyan]")
    soup = BeautifulSoup(html_content, "html.parser")

    print_confluence_debug_info(soup)

    releases: list[Release] = []
    release_sections = get_release_sections()

    for release_type, section_names in release_sections.items():
        section_releases = find_section_and_parse(soup, release_type, section_names)
        if section_releases:
            releases.extend(section_releases)
        else:
            console.print(f"[yellow]Could not find section for {release_type}[/yellow]")

    console.print(f"[green]Found {len(releases)} releases in Confluence[/green]")
    return releases


def get_table_headers(rows: list[Any]) -> tuple[list[str], bool]:
    """Extract and normalize table headers. Returns headers and whether table is valid."""
    if len(rows) < 2:
        console.print("  [yellow]Table has no data rows[/yellow]")
        return [], False

    header_cells = rows[0].find_all(["td", "th"])
    headers = [cell.get_text(strip=True).lower() for cell in header_cells]
    console.print(f"  [dim]Headers: {headers}[/dim]")
    return headers, True


def find_column_indices(headers: list[str]) -> tuple[int | None, int | None, int | None]:
    """Find the indices of version, date, and manager columns."""
    version_idx = None
    date_idx = None
    manager_idx = None

    for idx, header in enumerate(headers):
        if "version" in header and "suffix" not in header:
            version_idx = idx
        elif any(word in header for word in ["date", "cut date", "planned cut date"]):
            date_idx = idx
        elif any(word in header for word in ["manager", "release manager"]):
            manager_idx = idx

    console.print(
        f"  [dim]Column mapping - version: {version_idx}, date: {date_idx}, manager: {manager_idx}[/dim]"
    )
    return version_idx, date_idx, manager_idx


def parse_date_string(date_str: str) -> datetime | None:
    """Parse a date string in various formats."""
    date_formats = [
        "%d %b %Y",  # 09 Dec 2025
        "%d %B %Y",  # 09 December 2025
        "%Y-%m-%d",  # 2025-12-06
        "%Y/%m/%d",  # 2025/12/06
        "%m/%d/%Y",  # 12/06/2025
        "%d-%m-%Y",  # 06-12-2025
        "%b %d, %Y",  # Dec 09, 2025
        "%B %d, %Y",  # December 09, 2025
    ]

    # Handle "Week of DD Mon YYYY" format
    clean_date_str = date_str
    if "week of" in date_str.lower():
        clean_date_str = date_str.lower().replace("week of", "").strip()

    for date_format in date_formats:
        try:
            return datetime.strptime(clean_date_str, date_format)
        except ValueError:
            continue

    console.print(
        f"  [yellow]Could not parse date:[/yellow] '{date_str}' (tried {len(date_formats)} formats)"
    )
    return None


def extract_manager_first_name(release_manager: str) -> str:
    """Extract the first name from a release manager string."""
    if "+" in release_manager:
        return release_manager.split("+")[0].strip().split()[0]
    return release_manager.split()[0] if release_manager else ""


def generate_version_from_date(date: datetime) -> str:
    """Generate a version string from a date for releases without explicit versions."""
    return date.strftime("%Y.%m.%d")


def parse_table_row(
    cells: list[Any],
    row_num: int,
    version_idx: int | None,
    date_idx: int | None,
    manager_idx: int | None,
    release_type: str,
) -> Release | None:
    """Parse a single table row into a Release object."""
    try:
        # Extract data from cells
        date_str = cells[date_idx].get_text(strip=True) if date_idx is not None else ""
        release_manager = cells[manager_idx].get_text(strip=True) if manager_idx is not None else ""
        version = cells[version_idx].get_text(strip=True) if version_idx is not None else None

        # Skip empty rows
        if not date_str or not release_manager:
            console.print(f"  [dim]Row {row_num}: Skipping empty row[/dim]")
            return None

        # Parse date
        date = parse_date_string(date_str)
        if not date:
            return None

        # Extract manager name
        release_manager_first = extract_manager_first_name(release_manager)

        # Generate version if needed
        if version_idx is None or not version:
            version = generate_version_from_date(date)

        release = Release(
            release_type=release_type,
            version=version,
            date=date,
            release_manager=release_manager_first,
        )
        console.print(f"  [green]Parsed:[/green] {release}")
        return release

    except (IndexError, ValueError) as e:
        console.print(f"[yellow]Error parsing row {row_num}:[/yellow] {e}")
        return None


def parse_table(table: Any, release_type: str) -> list[Release]:
    """Parse a release table from HTML."""
    releases: list[Release] = []
    rows = table.find_all("tr")

    console.print(f"  [dim]Table has {len(rows)} rows[/dim]")

    # Get and validate headers
    headers, is_valid = get_table_headers(rows)
    if not is_valid:
        return releases

    # Find column indices
    version_idx, date_idx, manager_idx = find_column_indices(headers)

    if date_idx is None or manager_idx is None:
        console.print("  [yellow]Could not find required columns (date and manager)[/yellow]")
        return releases

    # Parse data rows
    for i, row in enumerate(rows[1:], start=1):
        cells = row.find_all(["td", "th"])
        if len(cells) < max(filter(None, [version_idx, date_idx, manager_idx])) + 1:
            console.print(f"  [dim]Row {i}: Skipping (not enough cells)[/dim]")
            continue

        release = parse_table_row(cells, i, version_idx, date_idx, manager_idx, release_type)
        if release:
            releases.append(release)

    return releases


def parse_calendar_component(component: Any) -> CalendarEntry | None:
    """Parse a calendar component into a CalendarEntry."""
    if component.name != "VEVENT":
        return None

    summary = str(component.get("summary", ""))
    dtstart = component.get("dtstart")
    description = component.get("description", "")

    if not dtstart:
        return None

    # Handle both date and datetime objects
    if hasattr(dtstart.dt, "date"):
        start_date = datetime.combine(dtstart.dt.date(), datetime.min.time())
    elif isinstance(dtstart.dt, datetime):
        start_date = dtstart.dt
    else:
        start_date = datetime.combine(dtstart.dt, datetime.min.time())

    return CalendarEntry(
        summary=summary,
        start_date=start_date,
        description=str(description) if description else None,
    )


def fetch_calendar_entries() -> list[CalendarEntry]:
    """Fetch and parse calendar entries from iCal feed with retry logic."""
    console.print(f"[cyan]Fetching calendar:[/cyan] {CALENDAR_ICAL_URL}")

    max_retries = 3
    retry_delay = 10
    calendar_data = b""

    for attempt in range(1, 1 + max_retries):
        try:
            response = requests.get(CALENDAR_ICAL_URL, timeout=30)
            response.raise_for_status()
            calendar_data = response.content
            break
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                console.print(f"[yellow]Attempt {attempt}/{max_retries} failed: {e}[/yellow]")
                console.print(f"[yellow]Retrying in {retry_delay} seconds...[/yellow]")
                time.sleep(retry_delay)
            else:
                console.print(f"[red]Failed to fetch calendar after {max_retries} attempts:[/red] {e}")
                sys.exit(1)

    console.print("[cyan]Parsing calendar entries...[/cyan]")
    calendar = Calendar.from_ical(calendar_data)
    entries = []

    for component in calendar.walk():
        entry = parse_calendar_component(component)
        if entry:
            entries.append(entry)

    console.print(f"[green]Found {len(entries)} calendar entries[/green]")
    return entries


def normalize_name(name: str) -> str:
    """Normalize a name by removing accents and converting to lowercase."""
    import unicodedata

    # Normalize unicode characters (NFD = decompose, then filter out combining marks)
    nfd = unicodedata.normalize("NFD", name)
    # Remove combining characters (accents)
    without_accents = "".join(char for char in nfd if unicodedata.category(char) != "Mn")
    return without_accents.lower().strip()


def dates_match(release_date: datetime, entry_date: datetime) -> bool:
    """Check if two dates match (same year, month, and day)."""
    return (
        entry_date.year == release_date.year
        and entry_date.month == release_date.month
        and entry_date.day == release_date.day
    )


def check_release_type_match(release_type: str, summary: str) -> bool:
    """Check if release type matches the calendar entry summary."""
    normalized_summary = normalize_name(summary)
    normalized_release_type = normalize_name(release_type)

    # Check if release type is in the summary (case-insensitive, accent-insensitive)
    if normalized_release_type in normalized_summary:
        return True

    # Handle "Airflow Ctl" vs "Airflow CTL" variations
    if "airflow" in normalized_release_type and "ctl" in normalized_release_type:
        return "airflow" in normalized_summary and "ctl" in normalized_summary

    return False


def check_version_match(version: str, summary: str) -> bool:
    """Check if version appears in the calendar entry summary."""
    return version in summary


def check_manager_match(manager_name: str, summary: str) -> bool:
    """Check if manager's name appears in the calendar entry summary."""
    import re

    normalized_manager = normalize_name(manager_name)
    normalized_summary = normalize_name(summary)

    # Check if manager name appears anywhere in summary
    if normalized_manager in normalized_summary:
        return True

    # Check if the manager appears as a word (not just substring)
    manager_pattern = r"\b" + re.escape(normalized_manager) + r"\b"
    return bool(re.search(manager_pattern, normalized_summary))


def is_matching_entry(release: Release, entry: CalendarEntry) -> bool:
    """
    Check if a calendar entry matches a release.

    A match requires:
    - Matching dates
    - Matching release type OR version
    - Matching release manager name
    """
    if not dates_match(release.date, entry.start_date):
        return False

    release_type_match = check_release_type_match(release.release_type, entry.summary)
    version_match = check_version_match(release.version, entry.summary)
    manager_match = check_manager_match(release.release_manager, entry.summary)

    # Consider it a match if date + (type or version) + manager match
    return (release_type_match or version_match) and manager_match


def find_matching_entry(release: Release, calendar_entries: list[CalendarEntry]) -> CalendarEntry | None:
    """Find a calendar entry that matches the given release, or None if not found."""
    for entry in calendar_entries:
        if is_matching_entry(release, entry):
            return entry
    return None


def print_verification_header() -> None:
    """Print the verification results header."""
    console.print("\n" + "=" * 80)
    console.print("[bold cyan]VERIFICATION RESULTS[/bold cyan]")
    console.print("=" * 80 + "\n")


def print_matched_release(release: Release, entry: CalendarEntry) -> None:
    """Print information about a matched release."""
    console.print(f"[green]✓ MATCHED:[/green] {release}")
    console.print(f"  [dim]Calendar: {entry.summary}[/dim]")


def print_unmatched_release(release: Release) -> None:
    """Print information about an unmatched release."""
    console.print(f"[red]✗ NOT MATCHED:[/red] {release}")


def print_verification_summary(
    total_releases: int, matched_count: int, unmatched_releases: list[Release]
) -> None:
    """Print the verification summary."""
    console.print("\n" + "=" * 80)
    console.print("[bold cyan]SUMMARY[/bold cyan]")
    console.print("=" * 80)
    console.print(f"Total releases in Confluence: {total_releases}")
    console.print(f"Matched releases: [green]{matched_count}[/green]")
    console.print(f"Unmatched releases: [red]{len(unmatched_releases)}[/red]")

    if unmatched_releases:
        console.print("\n[yellow]Unmatched releases:[/yellow]")
        for release in unmatched_releases:
            console.print(f"  [yellow]•[/yellow] {release}")

    console.print("=" * 80 + "\n")


def verify_releases(releases: list[Release], calendar_entries: list[CalendarEntry]) -> bool:
    """Verify that all releases have matching calendar entries."""
    print_verification_header()

    all_matched = True
    unmatched_releases: list[Release] = []
    matched_count = 0

    for release in releases:
        matching_entry = find_matching_entry(release, calendar_entries)

        if matching_entry:
            print_matched_release(release, matching_entry)
            matched_count += 1
        else:
            all_matched = False
            unmatched_releases.append(release)
            print_unmatched_release(release)

    print_verification_summary(len(releases), matched_count, unmatched_releases)

    return all_matched


def load_html_content(args: argparse.Namespace) -> str:
    """Load HTML content from file or fetch from Confluence."""
    if args.load_html:
        console.print(f"[cyan]Loading HTML from file:[/cyan] {args.load_html}")
        return Path(args.load_html).read_text(encoding="utf-8")

    html_content = fetch_confluence_page()
    if args.save_html:
        console.print(f"[cyan]Saving HTML to file:[/cyan] {args.save_html}")
        Path(args.save_html).write_text(html_content, encoding="utf-8")
    return html_content


def validate_releases(releases: list[Release]) -> None:
    """Validate that releases were found, exit if not."""
    if not releases:
        console.print("[red]No releases found in Confluence page![/red]")
        sys.exit(1)


def validate_calendar_entries(calendar_entries: list[CalendarEntry]) -> None:
    """Validate that calendar entries were found, exit if not."""
    if not calendar_entries:
        console.print("[red]No calendar entries found![/red]")
        sys.exit(1)


def print_final_result(all_matched: bool) -> None:
    """Print the final result and exit with appropriate code."""
    if all_matched:
        console.print("[bold green]✓ All releases have matching calendar entries![/bold green]")
        sys.exit(0)
    else:
        console.print("[bold red]✗ Some releases do not have matching calendar entries![/bold red]")
        sys.exit(1)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Verify that planned releases in Confluence match Google Calendar entries"
    )
    parser.add_argument(
        "--save-html", metavar="FILE", help="Save the fetched Confluence HTML to a file for debugging"
    )
    parser.add_argument(
        "--load-html", metavar="FILE", help="Load Confluence HTML from a file instead of fetching"
    )
    args = parser.parse_args()

    # Fetch and parse data
    html_content = load_html_content(args)
    releases = parse_confluence_releases(html_content)
    validate_releases(releases)

    calendar_entries = fetch_calendar_entries()
    validate_calendar_entries(calendar_entries)

    # Verify and exit with appropriate code
    all_matched = verify_releases(releases, calendar_entries)
    print_final_result(all_matched)


if __name__ == "__main__":
    main()
