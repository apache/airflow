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
Pick the lowest released versions of the providers a provider depends on.

``uv sync --resolution lowest-direct`` lowers a provider's external dependencies, but other
providers are workspace members, so they are always installed from the checkout. That means
the lower bound a provider declares on e.g. ``common.compat`` is never tested.

This script runs after that sync. For every provider the tested provider requires in
``[project].dependencies`` it prints a ``name==version`` pin: the lowest final release on
PyPI that satisfies every installed distribution requiring that provider. The caller
installs those pins, letting the installer adjust what they need (an old ``cncf.kubernetes``
needs an old ``kubernetes`` client), and then checks the environment is consistent.

Lowering one provider changes what it requires from the others (an older ``common.sql``
asks for a different ``common.compat``), so the requirements of every pinned release are
taken from PyPI and the pins are recomputed until they stop changing.

A dependency keeps its workspace version when:

* its line carries the ``# use next version`` comment (it needs an unreleased version),
* it declares no lower bound (the lowest release would be arbitrarily old),
* no final release on PyPI satisfies the requirements (e.g. only a release candidate exists).

It runs in the environment ``uv sync`` just produced, so it only uses the standard library
and ``packaging``/``tomli``, which ``pytest`` always brings in.
"""

from __future__ import annotations

import http.client
import json
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

AIRFLOW_ROOT_PATH = Path(__file__).resolve().parents[2]
PROVIDER_PREFIX = "apache-airflow-providers-"
LOWER_BOUND_OPERATORS = {">=", ">", "==", "===", "~="}
USE_NEXT_VERSION_PATTERN = re.compile(r"#\s*use next version", re.IGNORECASE)
REQUIREMENT_NAME_PATTERN = re.compile(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
PYPI_JSON_URL = "https://pypi.org/pypi/{name}/json"
PYPI_RELEASE_JSON_URL = "https://pypi.org/pypi/{name}/{version}/json"
PYPI_ATTEMPTS = 5
PYPI_RETRY_DELAY_SECONDS = 5


@dataclass(frozen=True)
class Decision:
    name: str
    pin: str | None
    reason: str


def _log(message: str) -> None:
    print(message, file=sys.stderr)


def get_cross_provider_requirements(pyproject_text: str) -> list[tuple[Requirement, bool]]:
    """Return ``(requirement, uses_next_version)`` for providers in ``[project].dependencies``."""
    data = tomllib.loads(pyproject_text)
    dependencies = data.get("project", {}).get("dependencies", [])
    lines = pyproject_text.splitlines()
    result = []
    for dependency in dependencies:
        requirement = Requirement(dependency)
        if not canonicalize_name(requirement.name).startswith(PROVIDER_PREFIX):
            continue
        if requirement.marker and not requirement.marker.evaluate():
            continue
        uses_next_version = any(
            f'"{dependency}"' in line and USE_NEXT_VERSION_PATTERN.search(line) for line in lines
        )
        result.append((requirement, uses_next_version))
    return result


def has_lower_bound(specifier: SpecifierSet) -> bool:
    return any(spec.operator in LOWER_BOUND_OPERATORS for spec in specifier)


def combined_specifier(
    name: str, installed_requirements: dict[str, list[str]]
) -> tuple[SpecifierSet, list[str]]:
    """
    Intersect what every installed distribution requires from ``name``.

    ``installed_requirements`` maps an installed distribution to its ``Requires-Dist`` entries.
    Entries behind an extra (``extra == "..."``) are skipped: the providers pulled in through
    extras are installed distributions themselves, with their own base requirements listed.

    Also returns ``distribution (requirement)`` for each requirer, so the log can show which one
    sets the floor when it is higher than the tested provider's own lower bound.
    """
    specifier = SpecifierSet()
    requirers = []
    wanted = canonicalize_name(name)
    for distribution, requires in sorted(installed_requirements.items()):
        for entry in requires:
            try:
                requirement = Requirement(entry)
            except InvalidRequirement:
                # Some published distributions carry metadata packaging rejects, for example
                # azure-kusto-data 4.1.0 lists "azure-core (>=1.11.0<2)". Only say so when the
                # entry is about the provider being lowered, the others are irrelevant here.
                match = REQUIREMENT_NAME_PATTERN.match(entry)
                if match and canonicalize_name(match.group(1)) == wanted:
                    _log(f"Ignoring unparsable requirement {entry!r} of {distribution}")
                continue
            if canonicalize_name(requirement.name) != wanted:
                continue
            if requirement.marker and not requirement.marker.evaluate({"extra": ""}):
                continue
            specifier &= requirement.specifier
            if requirement.specifier:
                requirers.append(f"{distribution} ({requirement.specifier})")
    return specifier, requirers


def lowest_final_release(specifier: SpecifierSet, releases: dict[str, list[dict]]) -> Version | None:
    """Return the lowest non-yanked final release with files that satisfies ``specifier``."""
    candidates = []
    for raw_version, files in releases.items():
        try:
            version = Version(raw_version)
        except InvalidVersion:
            continue
        if version.is_prerelease or version.is_devrelease:
            continue
        if not files or all(file.get("yanked") for file in files):
            continue
        if specifier.contains(version, prereleases=False):
            candidates.append(version)
    return min(candidates) if candidates else None


def decide(
    pyproject_text: str,
    installed_requirements: dict[str, list[str]],
    fetch_releases,
) -> list[Decision]:
    """Decide once, against ``installed_requirements`` as given."""
    decisions = []
    for requirement, uses_next_version in get_cross_provider_requirements(pyproject_text):
        name = canonicalize_name(requirement.name)
        if uses_next_version:
            decisions.append(Decision(name, None, "marked '# use next version'"))
            continue
        if not has_lower_bound(requirement.specifier):
            decisions.append(Decision(name, None, "no lower bound declared"))
            continue
        installed_specifier, requirers = combined_specifier(name, installed_requirements)
        specifier = installed_specifier & requirement.specifier
        required_by = f"; required by: {', '.join(requirers)}" if requirers else ""
        version = lowest_final_release(specifier, fetch_releases(name))
        if version is None:
            decisions.append(
                Decision(name, None, f"no final release on PyPI satisfies '{specifier}'{required_by}")
            )
            continue
        decisions.append(
            Decision(name, f"{name}=={version}", f"lowest final release for '{specifier}'{required_by}")
        )
    return decisions


MAX_ITERATIONS = 10


def decide_until_stable(
    pyproject_text: str,
    installed_requirements: dict[str, list[str]],
    fetch_releases,
    fetch_requires_dist,
) -> list[Decision]:
    """
    Repeat :func:`decide`, replacing each pinned provider's requirements by those of the pinned release.

    The installed (workspace) version of a provider can require more, or less, than the release
    it is lowered to, so the pins are only final once a round no longer changes them.
    """
    requirements = dict(installed_requirements)
    previous_pins: list[str | None] | None = None
    for _ in range(MAX_ITERATIONS):
        decisions = decide(pyproject_text, requirements, fetch_releases)
        pins = [decision.pin for decision in decisions]
        if pins == previous_pins:
            return decisions
        previous_pins = pins
        requirements = dict(installed_requirements)
        for decision in decisions:
            if decision.pin:
                version = decision.pin.split("==", 1)[1]
                requirements[decision.name] = fetch_requires_dist(decision.name, version)
    raise RuntimeError(f"Cross-provider pins did not settle after {MAX_ITERATIONS} rounds: {previous_pins}")


def fetch_json(url: str, sleep=time.sleep) -> dict | None:
    """Fetch JSON from PyPI, retrying transient failures. Return ``None`` if PyPI has no such page."""
    failure: Exception | None = None
    for attempt in range(1, PYPI_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            failure = error
        # A truncated response raises http.client.IncompleteRead, which is not an OSError.
        except (OSError, ValueError, http.client.HTTPException) as error:
            failure = error
        if attempt < PYPI_ATTEMPTS:
            _log(f"Fetching {url} failed ({failure}), retrying")
            sleep(PYPI_RETRY_DELAY_SECONDS * attempt)
    raise RuntimeError(f"Could not fetch {url} after {PYPI_ATTEMPTS} attempts: {failure}") from failure


def fetch_pypi_releases(name: str) -> dict[str, list[dict]]:
    # A provider that was never released (e.g. a brand new one) has no page, so nothing can be pinned.
    data = fetch_json(PYPI_JSON_URL.format(name=name))
    return data["releases"] if data else {}


def fetch_pypi_requires_dist(name: str, version: str) -> list[str]:
    data = fetch_json(PYPI_RELEASE_JSON_URL.format(name=name, version=version))
    if data is None:
        raise RuntimeError(f"PyPI has no metadata for {name}=={version}")
    return data["info"].get("requires_dist") or []


def get_installed_requirements() -> dict[str, list[str]]:
    return {
        canonicalize_name(distribution.metadata["Name"]): distribution.requires or []
        for distribution in metadata.distributions()
        # A leftover, broken dist-info directory can have no name; it cannot require anything.
        if distribution.metadata["Name"]
    }


def main(provider_id: str) -> int:
    pyproject = AIRFLOW_ROOT_PATH / "providers" / provider_id.replace(".", "/") / "pyproject.toml"
    decisions = decide_until_stable(
        pyproject.read_text(), get_installed_requirements(), fetch_pypi_releases, fetch_pypi_requires_dist
    )
    for decision in decisions:
        action = f"install {decision.pin}" if decision.pin else "keep workspace version"
        _log(f"{decision.name}: {action} ({decision.reason})")
        if decision.pin:
            print(decision.pin)
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        _log(f"Usage: {sys.argv[0]} <provider_id>")
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
