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
Airflow Registry Metadata Extractor

Extracts provider metadata from:
1. provider.yaml files - Rich metadata (integrations, logos, categories)
2. pyproject.toml files - Dependencies, Python version constraints
3. PyPI API - Download statistics and release dates (optional, requires network)

Output: providers.json for the Astro static site generator

Module discovery (modules.json) is handled by extract_parameters.py, which uses
runtime inspection inside breeze for accurate class discovery.
"""

from __future__ import annotations

import datetime
import json
import re
import shutil
import subprocess
import urllib.request
import zlib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

try:
    import tomllib  # Python 3.11+ stdlib
except ModuleNotFoundError:  # pragma: no cover -- Python 3.10 fallback
    import tomli as tomllib  # type: ignore[no-redef]

import yaml
from registry_contract_models import validate_providers_catalog

# External endpoints used by metadata extraction.
PYPISTATS_RECENT_URL = "https://pypistats.org/api/packages/{package_name}/recent"
PYPI_PACKAGE_JSON_URL = "https://pypi.org/pypi/{package_name}/json"
S3_DOC_URL = "http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com"
AIRFLOW_PROVIDER_DOCS_URL = "https://airflow.apache.org/docs/{package_name}/stable/"
AIRFLOW_PROVIDER_SOURCE_URL = (
    "https://github.com/apache/airflow/tree/providers-{provider_id}/{version}/providers/{provider_path}"
)
PYPI_PACKAGE_URL = "https://pypi.org/project/{package_name}/"


def fetch_pypi_downloads(package_name: str) -> dict[str, int]:
    """Fetch download statistics from pypistats.org API."""
    try:
        url = PYPISTATS_RECENT_URL.format(package_name=package_name)
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())
            return {
                "weekly": data["data"].get("last_week", 0),
                "monthly": data["data"].get("last_month", 0),
                "total": 0,  # Total not available in recent endpoint
            }
    except Exception as e:
        print(f"    Warning: Could not fetch PyPI stats for {package_name}: {e}")
        return {"weekly": 0, "monthly": 0, "total": 0}


def fetch_pypi_dates(package_name: str) -> dict[str, str]:
    """Fetch first release and latest release dates from PyPI JSON API."""
    try:
        url = PYPI_PACKAGE_JSON_URL.format(package_name=package_name)
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())

        earliest = None
        latest = None
        for version_files in data.get("releases", {}).values():
            for file_info in version_files:
                upload = file_info.get("upload_time_iso_8601") or file_info.get("upload_time")
                if not upload:
                    continue
                ts = upload[:10]
                if earliest is None or ts < earliest:
                    earliest = ts
                if latest is None or ts > latest:
                    latest = ts

        return {
            "first_released": earliest or "",
            "last_updated": latest or "",
        }
    except Exception as e:
        print(f"    Warning: Could not fetch PyPI dates for {package_name}: {e}")
        return {"first_released": "", "last_updated": ""}


def _parse_inventory_lines(inv_path: Path) -> list[str]:
    """Read and decompress the body of a Sphinx objects.inv file."""
    with inv_path.open("rb") as f:
        for _ in range(4):
            f.readline()
        return zlib.decompress(f.read()).decode("utf-8").splitlines()


def read_inventory(inv_path: Path) -> dict[str, str]:
    """Parse a Sphinx objects.inv file and return {qualified_name: url_path} for py:class entries."""
    result: dict[str, str] = {}
    for line in _parse_inventory_lines(inv_path):
        parts = line.split(None, 4)
        if len(parts) != 5:
            continue
        name, domain_role, _prio, location, _dispname = parts
        if domain_role == "py:class":
            # "$" in location means "use the name as anchor"
            result[name] = location.replace("$", name)
    return result


def read_connection_urls(inv_path: Path) -> dict[str, str]:
    """Parse a Sphinx objects.inv and return {conn_type: relative_url} for connection pages.

    Uses two inventory entry types:
    - ``std:label howto/connection:{conn_type}`` — maps conn_type directly to a page
    - ``std:doc connections/{name}`` — fallback by matching conn_type to doc name
    """
    label_map: dict[str, str] = {}  # conn_type -> page URL (from std:label)
    doc_map: dict[str, str] = {}  # doc_name -> page URL (from std:doc)
    for line in _parse_inventory_lines(inv_path):
        parts = line.split(None, 4)
        if len(parts) != 5:
            continue
        name, domain_role, _prio, location, _dispname = parts
        if domain_role == "std:label" and name.startswith("howto/connection:"):
            label_key = name[len("howto/connection:") :]
            # Skip sub-section labels like "gcp:configuring_the_connection"
            if ":" not in label_key:
                label_map[label_key] = location.split("#")[0]
        elif domain_role == "std:doc" and name.startswith("connections/"):
            doc_name = name[len("connections/") :]
            if doc_name != "index":
                doc_map[doc_name] = location

    # Merge: label_map takes precedence, doc_map fills gaps
    result: dict[str, str] = {}
    result.update(label_map)
    for doc_name, url in doc_map.items():
        if doc_name not in result:
            result[doc_name] = url
    return result


INVENTORY_CACHE_DIR = Path(__file__).parent / ".inventory_cache"
INVENTORY_TTL = datetime.timedelta(hours=12)


def fetch_provider_inventory(package_name: str, cache_dir: Path = INVENTORY_CACHE_DIR) -> Path | None:
    """Download a provider's objects.inv from S3, caching locally with a 12-hour TTL.

    Returns the local cache path on success, or None if the fetch fails
    (e.g. provider not yet published).
    """
    cache_path = cache_dir / package_name / "objects.inv"
    if cache_path.exists():
        age = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.datetime.fromtimestamp(
            cache_path.stat().st_mtime, tz=datetime.timezone.utc
        )
        if age < INVENTORY_TTL:
            return cache_path

    url = f"{S3_DOC_URL}/docs/{package_name}/stable/objects.inv"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            content = response.read()
        # Validate it's a Sphinx inventory
        if not content.startswith(b"# Sphinx inventory version"):
            print(f"    Warning: Invalid inventory header for {package_name}")
            return None
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(content)
        return cache_path
    except Exception as e:
        print(f"    Warning: Could not fetch inventory for {package_name}: {e}")
        # On refetch failure, serve stale cache rather than nothing
        if cache_path.exists():
            print(f"    Using stale cache for {package_name}")
            return cache_path
        return None


def resolve_connection_docs_url(conn_type: str, conn_url_map: dict[str, str], base_docs_url: str) -> str:
    """Resolve the docs URL for a connection type using the inventory map.

    Lookup order:
    1. Exact match on conn_type in the inventory map
    2. Fallback to connections/ directory listing
    """
    if conn_type in conn_url_map:
        return f"{base_docs_url}/{conn_url_map[conn_type]}"
    return f"{base_docs_url}/connections/"


# Base paths
AIRFLOW_ROOT = Path(__file__).parent.parent.parent
SCRIPT_DIR = Path(__file__).parent
PROVIDERS_DIR = AIRFLOW_ROOT / "providers"
REGISTRY_DIR = AIRFLOW_ROOT / "registry"
OUTPUT_DIR = REGISTRY_DIR / "src" / "_data"


@dataclass
class Category:
    """Category within a provider."""

    id: str
    name: str
    module_count: int = 0


@dataclass
class Provider:
    """Provider metadata."""

    id: str
    name: str
    package_name: str
    description: str
    lifecycle: str = "production"  # AIP-95: incubation, production, mature, deprecated
    logo: str | None = None
    version: str = ""
    versions: list[str] = field(default_factory=list)
    airflow_versions: list[str] = field(default_factory=list)
    pypi_downloads: dict[str, int] = field(default_factory=lambda: {"weekly": 0, "monthly": 0, "total": 0})
    module_counts: dict[str, int] = field(
        default_factory=lambda: {
            "operator": 0,
            "hook": 0,
            "sensor": 0,
            "trigger": 0,
            "transfer": 0,
            "notifier": 0,
            "secret": 0,
            "logging": 0,
            "executor": 0,
            "bundle": 0,
            "decorator": 0,
        }
    )
    categories: list[dict] = field(default_factory=list)
    connection_types: list[dict] = field(default_factory=list)  # {conn_type, hook_class, docs_url}
    requires_python: str = ""  # e.g., ">=3.10"
    dependencies: list[str] = field(default_factory=list)  # from pyproject.toml
    optional_extras: dict[str, list[str]] = field(default_factory=dict)  # {extra_name: [deps]}
    dependents: list[str] = field(default_factory=list)
    related_providers: list[str] = field(default_factory=list)
    docs_url: str = ""
    source_url: str = ""
    pypi_url: str = ""
    first_released: str = ""
    last_updated: str = ""


def parse_provider_yaml(yaml_path: Path) -> dict[str, Any]:
    """Parse a provider.yaml file."""
    with open(yaml_path) as f:
        return yaml.safe_load(f)


def parse_pyproject_toml(pyproject_path: Path) -> dict[str, Any]:
    """Parse pyproject.toml and extract requires-python, dependencies, and optional extras."""
    result: dict[str, Any] = {"requires_python": "", "dependencies": [], "optional_extras": {}}

    if not pyproject_path.exists():
        return result

    try:
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)

        project = data.get("project", {})

        result["requires_python"] = project.get("requires-python", "")
        result["dependencies"] = [d.strip() for d in project.get("dependencies", [])][:20]

        optional_deps = project.get("optional-dependencies", {})
        for extra_name, extra_deps in optional_deps.items():
            clean = [d.strip() for d in extra_deps if d.strip()]
            if clean:
                result["optional_extras"][extra_name] = clean[:5]

    except Exception as e:
        print(f"    Warning: Could not parse {pyproject_path}: {e}")

    return result


def extract_integrations_as_categories(provider_yaml: dict[str, Any]) -> list[Category]:
    """Extract integrations from provider.yaml as categories."""
    categories: dict[str, Category] = {}

    for integration in provider_yaml.get("integrations", []):
        name = integration.get("integration-name", "")
        if not name:
            continue

        # Create a slug for the category ID
        cat_id = name.lower().replace(" ", "-").replace("(", "").replace(")", "")
        cat_id = re.sub(r"[^a-z0-9-]", "", cat_id)

        if cat_id not in categories:
            categories[cat_id] = Category(id=cat_id, name=name, module_count=0)

    return list(categories.values())


def module_path_to_file_path(module_path: str, provider_path: Path) -> Path:
    """Convert a Python module path to an actual file path.

    provider_path is the actual filesystem directory (e.g., providers/microsoft/azure/),
    not the dash-joined provider_id.
    """
    # e.g., airflow.providers.amazon.operators.s3 -> providers/amazon/src/airflow/providers/amazon/operators/s3.py
    parts = module_path.split(".")
    file_path = provider_path / "src" / "/".join(parts)
    return file_path.with_suffix(".py")


def determine_airflow_versions(dependencies: list[str]) -> list[str]:
    """Determine minimum Airflow version from pyproject.toml dependencies."""
    for dep in dependencies:
        if dep.startswith("apache-airflow>="):
            version_str = dep.split(">=")[1].split(",")[0].strip()
            parts = version_str.split(".")
            if len(parts) >= 2:
                return [f"{parts[0]}.{parts[1]}+"]
    return ["3.0+"]


def find_related_providers(provider_id: str, all_provider_yamls: dict[str, dict]) -> list[str]:
    """Find related providers based on shared integrations or dependencies."""
    current = all_provider_yamls.get(provider_id, {})
    current_integrations = {i.get("integration-name") for i in current.get("integrations", [])}

    related = []
    for other_id, other_yaml in all_provider_yamls.items():
        if other_id == provider_id:
            continue
        other_integrations = {i.get("integration-name") for i in other_yaml.get("integrations", [])}
        overlap = current_integrations & other_integrations
        if overlap:
            related.append(other_id)

    return related[:5]  # Limit to 5 related providers


def load_release_tags() -> set[str]:
    """Return all ``providers-<id>/<version>`` git tags as a set for fast lookup.

    Used to filter ``provider.yaml`` ``versions:`` lists to only entries that
    correspond to a real release (excludes phantom version bumps where the
    next-version entry was prepended to ``versions:`` before the tag landed,
    or pre-release-only versions like ``providers-celery/3.19.0rc1`` where the
    ``rc1`` exists but the final does not).

    Returns an empty set if the ``git`` command fails (e.g., outside a checkout);
    callers can decide whether to fall back to the unfiltered top entry.
    """
    try:
        result = subprocess.run(
            ["git", "tag", "--list", "providers-*"],
            capture_output=True,
            text=True,
            cwd=AIRFLOW_ROOT,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return set()
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def find_latest_released_version(
    provider_id: str,
    versions_list: list[str],
    release_tags: set[str],
) -> str | None:
    """Walk ``versions_list`` newest-first, return the first version with a real release tag.

    Returns ``None`` when no entry in ``versions_list`` has a corresponding
    ``providers-<id>/<version>`` tag, indicating the provider is unreleased
    (brand-new in-tree, no tags yet) or in an inconsistent state.
    """
    for version in versions_list:
        if f"providers-{provider_id}/{version}" in release_tags:
            return version
    return None


def main():
    """Main extraction function."""
    import argparse

    parser = argparse.ArgumentParser(description="Airflow Registry Metadata Extractor")
    parser.add_argument(
        "--provider",
        default=None,
        help="Extract only this provider ID (e.g. 'amazon'). Omit for full build.",
    )
    parser.add_argument(
        "--allow-unreleased",
        action="store_true",
        help=(
            "Include providers and versions that don't have a matching "
            "providers-<id>/<ver> git tag. Use for staging builds and local dev "
            "where maintainers want to preview unreleased provider pages before "
            "the tag lands. Default is to filter unreleased entries so live "
            "builds don't ship phantom pointers."
        ),
    )
    args = parser.parse_args()

    print("Airflow Registry Metadata Extractor")
    print("=" * 50)
    if args.provider:
        requested_providers = set(args.provider.split())
        print(f"Incremental mode: extracting provider(s) {requested_providers}")
    else:
        requested_providers = None
    if args.allow_unreleased:
        print("Unreleased providers: INCLUDED (--allow-unreleased)")

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_providers: list[Provider] = []
    all_provider_yamls: dict[str, dict] = {}

    # First pass: Load all provider.yaml files (including nested ones like dbt/cloud, microsoft/azure)
    provider_yaml_paths: dict[str, Path] = {}
    for yaml_path in PROVIDERS_DIR.rglob("provider.yaml"):
        # Calculate provider_id from path relative to PROVIDERS_DIR
        # e.g., providers/amazon/provider.yaml -> amazon
        # e.g., providers/microsoft/azure/provider.yaml -> microsoft-azure
        relative_path = yaml_path.relative_to(PROVIDERS_DIR)
        parts = relative_path.parts[:-1]  # Remove 'provider.yaml'
        provider_id = "-".join(parts)

        try:
            provider_yaml = parse_provider_yaml(yaml_path)
            all_provider_yamls[provider_id] = provider_yaml
            provider_yaml_paths[provider_id] = yaml_path.parent
        except Exception as e:
            print(f"  Error parsing {yaml_path}: {e}")

    print(f"Found {len(all_provider_yamls)} providers with provider.yaml")

    # Filter to requested providers if --provider was given
    if requested_providers:
        filtered_ids = {pid for pid in all_provider_yamls if pid in requested_providers}
        missing = requested_providers - filtered_ids
        if missing:
            print(f"  Warning: provider(s) not found: {missing}")
        # Keep only the requested providers for extraction
        extraction_ids = filtered_ids
    else:
        extraction_ids = set(all_provider_yamls.keys())

    # Load all release tags once. Used below to filter `provider.yaml`'s
    # `versions:` to only entries that have a real `providers-<id>/<ver>`
    # git tag, avoiding phantom-version leaks (next-release bumps prepended
    # to `versions:` before the tag lands, RC-only releases, brand-new
    # providers with no tags yet).
    #
    # Skipped entirely when --allow-unreleased is set: staging builds and
    # local dev want to preview unreleased provider pages so maintainers
    # can verify them before tagging.
    if args.allow_unreleased:
        release_tags: set[str] = set()
    else:
        release_tags = load_release_tags()
        if not release_tags:
            print(
                "  Warning: no providers-* git tags found; "
                "phantom version filter is disabled (falling back to versions[0]). "
                "If this is a CI run, ensure the checkout step uses fetch-tags: true."
            )
    skipped_unreleased: list[str] = []

    # Second pass: Extract full metadata (only for providers in extraction_ids)
    for provider_id in extraction_ids:
        provider_yaml = all_provider_yamls[provider_id]
        provider_path = provider_yaml_paths[provider_id]

        package_name = provider_yaml.get("package-name", f"apache-airflow-providers-{provider_id}")
        name = provider_yaml.get("name", provider_id.replace("-", " ").title())
        description = provider_yaml.get("description", "")

        # Clean up RST formatting in description
        # Convert RST links like `Text <url>`__ to just "Text"
        description = re.sub(r"`([^<]+)\s*<[^>]+>`__", r"\1", description)
        # Remove any remaining backticks
        description = re.sub(r"`", "", description)
        # Remove bullet points (- at start of lines)
        description = re.sub(r"\n\s*-\s*", ", ", description)
        # Clean up extra whitespace and normalize
        description = re.sub(r"\s+", " ", description).strip()
        # Remove trailing commas
        description = re.sub(r",\s*$", "", description)
        # Fix extra spaces before punctuation
        description = re.sub(r"\s+([),.])", r"\1", description)
        # Remove "including:" followed by just commas
        description = re.sub(r"including:\s*,", "including", description)
        # Fix "(including X )" -> "(including X)"
        description = re.sub(r"\s+\)", ")", description)
        # Remove empty parentheses
        description = re.sub(r"\(\s*\)", "", description)
        # Truncate long descriptions
        if len(description) > 200:
            description = description[:197] + "..."

        # Get versions, filtering to entries that have a real release tag.
        # Provider release prep prepends the next version to `versions:` BEFORE
        # the tag lands, and pre-release-only versions match `versions:` but
        # have no final tag. Without filtering, `version` (the latest pointer)
        # AND the `versions` list both leak phantoms downstream -- the latter
        # is consumed by extract_versions.py's backfill, which would try to
        # `git show` from a non-existent tag.
        raw_versions = provider_yaml.get("versions", [])
        if release_tags:
            versions = [v for v in raw_versions if f"providers-{provider_id}/{v}" in release_tags]
            version = find_latest_released_version(provider_id, raw_versions, release_tags)
            if version is None:
                skipped_unreleased.append(provider_id)
                print(
                    f"  Skipping {provider_id}: no released version found in "
                    f"versions list {raw_versions} "
                    f"(no matching providers-{provider_id}/<ver> tag)"
                )
                continue
        else:
            # No tag information available -- fall back to old behaviour.
            versions = list(raw_versions)
            version = versions[0] if versions else "0.0.0"

        # Extract categories from integrations
        categories = extract_integrations_as_categories(provider_yaml)

        # Find logo from provider's docs/integration-logos/ directory and copy
        # to dev/registry/logos/ (mounted in breeze).  The CI workflow copies
        # these into registry/public/logos/ before the 11ty build.
        logo = None
        logo_path = None

        # Try to find the most representative logo for the provider
        # Priority: Look for "main" provider logos that match the provider name
        integration_logos_dir = provider_path / "docs" / "integration-logos"

        # Common patterns for main provider logos
        main_logo_patterns = [
            f"{name.replace(' ', '-')}.png",  # e.g., "Google-Cloud.png"
            f"{name.replace(' ', '-')}.svg",
            f"{name}.png",
            f"{provider_id.replace('-', '_')}.png",
        ]

        # Special case mappings for well-known providers
        logo_priority_map = {
            "amazon": ["AWS-Cloud-alt_light-bg@4x.png", "Amazon-Web-Services.png"],
            "google": ["Google-Cloud.png", "Google.png"],
            "microsoft-azure": ["Microsoft-Azure.png"],
            "snowflake": ["Snowflake.png"],
            "databricks": ["Databricks.png"],
        }

        # Write logos to dev/registry/logos/ — this directory is mounted in
        # breeze (unlike registry/public/) so copies survive the container.
        # Also copy to registry/public/logos/ for local dev convenience.
        # Directories are created lazily (only when a logo is found) to avoid
        # empty dirs that trip up glob-based cp in the CI workflow.
        logos_dest_dir = SCRIPT_DIR / "logos"
        registry_logos_dir = SCRIPT_DIR.parent.parent / "registry" / "public" / "logos"

        if integration_logos_dir.exists():
            logos_dest_dir.mkdir(parents=True, exist_ok=True)

            # First, check for priority logos for known providers
            if provider_id in logo_priority_map:
                for priority_logo in logo_priority_map[provider_id]:
                    potential_logo = integration_logos_dir / priority_logo
                    if potential_logo.exists():
                        logo_dest = logos_dest_dir / f"{provider_id}-{potential_logo.name}"
                        shutil.copy2(potential_logo, logo_dest)
                        logo = f"/logos/{provider_id}-{potential_logo.name}"
                        break

            # If no priority logo found, try general patterns
            if not logo:
                for pattern in main_logo_patterns:
                    potential_logo = integration_logos_dir / pattern
                    if potential_logo.exists():
                        logo_dest = logos_dest_dir / f"{provider_id}-{potential_logo.name}"
                        shutil.copy2(potential_logo, logo_dest)
                        logo = f"/logos/{provider_id}-{potential_logo.name}"
                        break

            # Still no logo? Use the one from provider.yaml integrations
            if not logo:
                for integration in provider_yaml.get("integrations", []):
                    if integration.get("logo"):
                        logo_path = integration["logo"]
                        logo_filename = logo_path.split("/")[-1]
                        logo_source = integration_logos_dir / logo_filename
                        if logo_source.exists():
                            logo_dest = logos_dest_dir / f"{provider_id}-{logo_filename}"
                            shutil.copy2(logo_source, logo_dest)
                            logo = f"/logos/{provider_id}-{logo_filename}"
                            break

            # Last resort: use the first available logo
            if not logo:
                logos = list(integration_logos_dir.glob("*.png")) + list(integration_logos_dir.glob("*.svg"))
                if logos:
                    logo_source = logos[0]
                    logo_dest = logos_dest_dir / f"{provider_id}-{logo_source.name}"
                    shutil.copy2(logo_source, logo_dest)
                    logo = f"/logos/{provider_id}-{logo_source.name}"

        # Also copy to registry/public/logos/ so local `pnpm dev` works without
        # the extra CI copy step.
        if logo:
            logo_filename = logo.split("/")[-1]
            src = logos_dest_dir / logo_filename
            if src.exists():
                registry_logos_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, registry_logos_dir / logo_filename)

        # Extract connection types from provider.yaml
        # Resolve per-connection docs URLs from Sphinx inventory when available
        connection_types = []
        base_docs_url = AIRFLOW_PROVIDER_DOCS_URL.format(package_name=package_name).rstrip("/")
        conn_url_map: dict[str, str] = {}
        inv_path = fetch_provider_inventory(package_name)
        if inv_path:
            conn_url_map = read_connection_urls(inv_path)
        for conn in provider_yaml.get("connection-types", []):
            conn_type = conn.get("connection-type", "")
            hook_class = conn.get("hook-class-name", "")
            if conn_type:
                connection_types.append(
                    {
                        "conn_type": conn_type,
                        "hook_class": hook_class,
                        "docs_url": resolve_connection_docs_url(conn_type, conn_url_map, base_docs_url),
                    }
                )

        # Fetch PyPI download statistics and release dates
        pypi_downloads = fetch_pypi_downloads(package_name)
        pypi_dates = fetch_pypi_dates(package_name)

        # Parse pyproject.toml for requires-python and dependencies
        pyproject_path = provider_path / "pyproject.toml"
        pyproject_data = parse_pyproject_toml(pyproject_path)

        # Airflow version compatibility (from pyproject.toml dependencies)
        airflow_versions = determine_airflow_versions(pyproject_data["dependencies"])

        provider_source_path = provider_path.relative_to(PROVIDERS_DIR).as_posix()
        provider = Provider(
            id=provider_id,
            name=name,
            package_name=package_name,
            description=description,
            lifecycle=provider_yaml.get("lifecycle", "production"),
            logo=logo,
            version=version,
            versions=versions,
            airflow_versions=airflow_versions,
            pypi_downloads=pypi_downloads,
            categories=[asdict(c) for c in categories],
            connection_types=connection_types,
            requires_python=pyproject_data["requires_python"],
            dependencies=pyproject_data["dependencies"],
            optional_extras=pyproject_data.get("optional_extras", {}),
            docs_url=AIRFLOW_PROVIDER_DOCS_URL.format(package_name=package_name),
            source_url=AIRFLOW_PROVIDER_SOURCE_URL.format(
                provider_id=provider_id, version=version, provider_path=provider_source_path
            ),
            pypi_url=PYPI_PACKAGE_URL.format(package_name=package_name),
            first_released=pypi_dates["first_released"],
            last_updated=pypi_dates["last_updated"],
        )

        all_providers.append(provider)
        print(f"  {provider_id}: {len(categories)} categories")

    if skipped_unreleased:
        print(
            f"\nSkipped {len(skipped_unreleased)} unreleased provider(s) "
            f"(no matching git tag): {sorted(skipped_unreleased)}"
        )

    # Find related providers
    for provider in all_providers:
        provider.related_providers = find_related_providers(provider.id, all_provider_yamls)

    # Convert to JSON-serializable format
    new_providers = [asdict(p) for p in all_providers]

    # In incremental mode, merge new providers into existing providers.json
    # so parallel runs for different providers don't clobber each other.
    if requested_providers:
        new_by_id = {p["id"]: p for p in new_providers}
        for out_dir in [SCRIPT_DIR, OUTPUT_DIR]:
            existing_path = out_dir / "providers.json"
            if existing_path.exists():
                try:
                    existing = json.loads(existing_path.read_text())
                    merged = [new_by_id.pop(p["id"], p) for p in existing["providers"]]
                    merged.extend(new_by_id.values())
                    new_providers = merged
                    print(
                        f"Merged {len(all_providers)} updated + {len(merged) - len(all_providers)} existing providers"
                    )
                except (json.JSONDecodeError, KeyError):
                    pass
                break

    new_providers.sort(key=lambda p: p["name"].lower())
    providers_json = validate_providers_catalog({"providers": new_providers})

    # Write output files to all output directories.
    # Inside breeze, registry/ is not mounted so OUTPUT_DIR writes are lost.
    # SCRIPT_DIR (dev/registry/) is always mounted, so the other extraction
    # scripts can pick up providers.json from there.
    output_dirs = [OUTPUT_DIR, SCRIPT_DIR]

    for out_dir in output_dirs:
        if not out_dir.parent.exists():
            continue
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(out_dir / "providers.json", "w") as f:
            json.dump(providers_json, f, indent=2)
        print(f"\nWrote {len(new_providers)} providers to {out_dir}")

    print("\nDone!")


if __name__ == "__main__":
    main()
