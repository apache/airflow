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
Find the first PyPI release with target wheels for every direct third-party requirement.

``uv --resolution lowest-direct`` installs each direct requirement (override requirements included) at its
declared floor, while transitive packages resolve to their newest compatible release. A floor without target
wheels therefore turns into a source build only for direct requirements, so this scan reads the declared
requirements of the given ``pyproject.toml`` files instead of a lock.

Wheel presence is not runtime support: a release can ship a pure-Python wheel that fails on the new
interpreter. Confirm every proposed floor by installing and importing it on the target Python.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
import tomllib
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from packaging.requirements import Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

_AUDIT_SPEC = importlib.util.spec_from_file_location(
    "audit_wheels", Path(__file__).with_name("audit_wheels.py")
)
audit_wheels = importlib.util.module_from_spec(_AUDIT_SPEC)
_AUDIT_SPEC.loader.exec_module(audit_wheels)

WORKSPACE_PREFIX = "apache-airflow"
LOWER_BOUND_OPERATORS = {">=", ">", "==", "~=", "==="}
LOOKUP_ERRORS = audit_wheels.LOOKUP_ERRORS
LOOKUP_ATTEMPTS = 2


def build_environment(python_version: str, machine: str) -> dict[str, str]:
    version = Version(python_version)
    return {
        "python_version": f"{version.major}.{version.minor}",
        "python_full_version": python_version,
        "implementation_name": "cpython",
        "platform_python_implementation": "CPython",
        "sys_platform": "linux",
        "platform_system": "Linux",
        "os_name": "posix",
        "platform_machine": machine,
        "extra": "",
    }


def iter_declared_requirements(pyproject: dict) -> Iterator[str]:
    """Yield requirements that lowest-direct treats as direct; constraints are deliberately not included."""
    project = pyproject.get("project", {})
    yield from project.get("dependencies", [])
    for dependencies in project.get("optional-dependencies", {}).values():
        yield from dependencies
    for dependencies in pyproject.get("dependency-groups", {}).values():
        yield from (dependency for dependency in dependencies if isinstance(dependency, str))
    yield from pyproject.get("tool", {}).get("uv", {}).get("override-dependencies", [])


def collect_requirements(paths: list[Path], python_version: str) -> dict[str, list[tuple[Requirement, str]]]:
    """Group the third-party requirements that apply to the target interpreter by package name."""
    environments = [
        build_environment(python_version, machine) for machine in audit_wheels.ARCHITECTURES.values()
    ]
    collected: dict[str, list[tuple[Requirement, str]]] = {}
    for path in paths:
        pyproject = tomllib.loads(path.read_text(encoding="utf-8"))
        for line in iter_declared_requirements(pyproject):
            requirement = Requirement(line)
            name = canonicalize_name(requirement.name)
            if name.startswith(WORKSPACE_PREFIX) or requirement.url:
                continue
            if requirement.marker and not any(requirement.marker.evaluate(env) for env in environments):
                continue
            collected.setdefault(name, []).append((requirement, path.as_posix()))
    return collected


def has_target_wheels(files: list[dict], python_version: str) -> bool:
    """
    Require a compatible wheel for every supported architecture.

    A release that ships interpreter-specific wheels is a compiled project, so its ``none-any`` wheel is a
    pure-Python fallback without the native extension (``lazy-object-proxy`` 1.11 is one) and does not count.
    """
    wheels = [file["filename"] for file in files if file.get("packagetype") == "bdist_wheel"]
    if any(re.search(r"-cp3\d+t?-", wheel) for wheel in wheels):
        wheels = [wheel for wheel in wheels if not wheel.endswith("-none-any.whl")]
    return all(
        any(audit_wheels.is_wheel_compatible(wheel, python_version, architecture) for wheel in wheels)
        for architecture in audit_wheels.ARCHITECTURES.values()
    )


def allows_python(requires_python: str | None, python_version: str) -> bool:
    try:
        return SpecifierSet(requires_python or "").contains(python_version, prereleases=True)
    except InvalidSpecifier:
        # Some old releases carry malformed metadata such as ">=3.4.*"; installers skip them as well.
        return False


def list_candidates(
    data: dict, python_version: str, *, allow_prereleases: bool
) -> list[tuple[Version, list[dict]]]:
    """Return installable releases, oldest first, without yanked files or Requires-Python misses."""
    candidates = []
    for raw_version, files in data.get("releases", {}).items():
        try:
            version = Version(raw_version)
        except InvalidVersion:
            continue
        if version.is_prerelease and not allow_prereleases:
            continue
        installable = [
            file
            for file in files
            if not file.get("yanked") and allows_python(file.get("requires_python"), python_version)
        ]
        if installable:
            candidates.append((version, installable))
    return sorted(candidates, key=lambda candidate: candidate[0])


def scan_package(
    name: str, requirements: list[tuple[Requirement, str]], python_version: str, data: dict
) -> dict:
    specifier = SpecifierSet(
        ",".join(str(requirement.specifier) for requirement, _ in requirements if str(requirement.specifier))
    )
    # Like uv, consider pre-releases only when a declared specifier names one.
    candidates = list_candidates(data, python_version, allow_prereleases=bool(specifier.prereleases))
    selectable = [(version, files) for version, files in candidates if specifier.contains(version)]
    record = {
        "name": name,
        "declared_in": sorted({source for _, source in requirements}),
        "specifier": str(specifier),
        "unbounded": not any(spec.operator in LOWER_BOUND_OPERATORS for spec in specifier),
    }
    if not selectable:
        return record | {"status": "unsatisfiable"}
    lowest_version, lowest_files = selectable[0]
    record["lowest_selected"] = str(lowest_version)
    if has_target_wheels(lowest_files, python_version):
        return record | {"status": "ok"}
    classification = classify_sdist(lowest_files)
    if classification and classification["disposition"] == "pure-python-sdist":
        return record | {"status": "pure-python-sdist"}
    record |= (
        {"compiled_source_evidence": classification["compiled_source_evidence"]} if classification else {}
    )
    for version, files in selectable[1:]:
        if has_target_wheels(files, python_version):
            uploaded = min(
                file["upload_time_iso_8601"]
                for file in files
                if file.get("packagetype") == "bdist_wheel" and "upload_time_iso_8601" in file
            )
            major_minor = build_environment(python_version, "x86_64")["python_version"]
            return record | {
                "status": "raise",
                "first_wheel_version": str(version),
                "first_wheel_uploaded": uploaded,
                "proposal": f'{name}>={version}; python_version >= "{major_minor}"',
            }
    outside = next(
        (
            str(version)
            for version, files in candidates
            if version > lowest_version
            and version not in specifier
            and has_target_wheels(files, python_version)
        ),
        None,
    )
    if outside:
        return record | {"status": "blocked-by-cap", "first_wheel_version": outside}
    return record | {"status": "source-only"}


def classify_sdist(files: list[dict]) -> dict | None:
    """Tell compiled sources from pure Python at the floor, so pure-Python packages need no raised floor."""
    sdists = [file for file in files if file.get("packagetype") == "sdist"]
    if not sdists:
        return None
    return audit_wheels.classify_sdist(
        audit_wheels.fetch_bytes(sdists[0]["url"]), sdists[0]["filename"], pure_python_fallback=False
    )


def scan_item(name: str, requirements: list[tuple[Requirement, str]], python_version: str) -> dict:
    for attempt in range(LOOKUP_ATTEMPTS):
        try:
            return scan_package(name, requirements, python_version, audit_wheels.fetch_release(name))
        except LOOKUP_ERRORS as error:
            if attempt == LOOKUP_ATTEMPTS - 1:
                return {"name": name, "status": "lookup-error", "lookup_error": str(error)}


def render_markdown(records: list[dict]) -> str:
    rows = [
        "| Package | Declared floor | Lowest selected | Status | Proposal | First wheel uploaded |",
        "|---|---|---|---|---|---|",
    ]
    for record in records:
        if record["status"] == "ok":
            continue
        rows.append(
            f"| {record['name']} | {record.get('specifier', '')} | {record.get('lowest_selected', '')} | "
            f"{record['status']}{' (no lower bound)' if record.get('unbounded') else ''} | "
            f"{record.get('proposal', record.get('first_wheel_version', ''))} | "
            f"{record.get('first_wheel_uploaded', '')} |"
        )
    return "\n".join(rows) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-version", required=True, help="Exact target version, e.g. 3.15.0")
    parser.add_argument(
        "--pyproject", required=True, nargs="+", type=Path, help="pyproject.toml files to scan"
    )
    parser.add_argument("--output", required=True, type=Path, help="JSON records")
    parser.add_argument("--markdown", type=Path, help="Optional table of the packages that need action")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"3\.\d+\.\d+", args.python_version):
        parser.error("--python-version must be a final Python 3 patch version, e.g. 3.15.0")
    declared = collect_requirements(args.pyproject, args.python_version)
    with ThreadPoolExecutor(max_workers=12) as pool:
        records = list(
            pool.map(lambda item: scan_item(item[0], item[1], args.python_version), sorted(declared.items()))
        )
    args.output.write_text(
        json.dumps({"python": args.python_version, "packages": records}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if args.markdown:
        args.markdown.write_text(render_markdown(records), encoding="utf-8")
    return int(any(record["status"] == "lookup-error" for record in records))


if __name__ == "__main__":
    sys.exit(main())
