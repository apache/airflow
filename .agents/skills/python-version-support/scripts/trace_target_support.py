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
Trace every locked third-party package, direct or transitive, that the target interpreter would install.

The dependency graph comes from a ``uv.lock``: edges whose markers fail for the target are dropped, and each
package keeps its shortest chain from a first-party member plus every first-party member that reaches it.
Each locked release and the newest PyPI release are audited for target wheels and for a
``Programming Language :: Python :: 3.N`` classifier.

Classifiers are a soft signal: many projects omit them or add them late, and a new classifier can be purely
cosmetic. They never affect the exit status. Wheel presence is not runtime support either; install and import
every release you adopt on the target interpreter.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
import tomllib
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import unquote, urlparse

from packaging.markers import Marker
from packaging.utils import canonicalize_name
from packaging.version import Version


def load_sibling(name: str):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(f"{name}.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


audit_wheels = load_sibling("audit_wheels")
scan_wheel_floors = load_sibling("scan_wheel_floors")

FIRST_PARTY_SOURCES = {"editable", "virtual", "directory", "workspace"}
PLATFORMS = {"amd64": "x86_64", "arm64": "aarch64"}


def get_source_kind(package: dict) -> str:
    return next(iter(package.get("source", {})), "")


def is_first_party(package: dict) -> bool:
    return get_source_kind(package) in FIRST_PARTY_SOURCES


def build_key(package: dict) -> tuple[str, str]:
    """Identify a lock entry; workspace members such as ``apache-airflow-task-sdk`` may have no version."""
    return canonicalize_name(package["name"]), package.get("version", "")


def is_marker_satisfied(marker: str | None, environment: dict[str, str]) -> bool:
    return marker is None or Marker(marker).evaluate(environment)


def find_target(dependency: dict, candidates: list[dict], environment: dict[str, str]) -> dict | None:
    """
    Pick the lock entry a dependency edge points at.

    uv names the version or source only when a package has several entries, so fall back to the entry
    whose ``resolution-markers`` match the target environment.
    """
    matching = [
        package
        for package in candidates
        if dependency.get("version") in (None, package.get("version"))
        and dependency.get("source") in (None, package.get("source"))
    ]
    if len(matching) > 1:
        matching = [
            package
            for package in matching
            if not package.get("resolution-markers")
            or any(is_marker_satisfied(marker, environment) for marker in package["resolution-markers"])
        ]
    return matching[0] if matching else None


def iter_edges(package: dict, extras: set[str], *, is_root: bool) -> list[dict]:
    """
    Collect a walked member's extras and groups, but only the requested extras of anything it reaches.

    CI installs every member with its own groups, so a nested member's groups are attributed to that member's
    own walk rather than to every member that depends on it.
    """
    edges = list(package.get("dependencies", []))
    optional = package.get("optional-dependencies", {})
    if is_root:
        selected_extras = optional.values()
        edges.extend(edge for group in package.get("dev-dependencies", {}).values() for edge in group)
    else:
        selected_extras = (optional[extra] for extra in sorted(extras) if extra in optional)
    for group in selected_extras:
        edges.extend(group)
    return edges


def walk_lock(lock: dict, environment: dict[str, str], excluded: set[str]) -> dict[tuple[str, str], dict]:
    """
    Return the reachable third-party packages with their shortest chain and the members that need them.

    Each first-party member is walked separately: ``declared_by`` lists members that require the package
    themselves and so own any floor change, while ``required_by`` lists every member that reaches it.
    """
    by_name: dict[str, list[dict]] = {}
    for package in lock["package"]:
        by_name.setdefault(canonicalize_name(package["name"]), []).append(package)
    roots = sorted(
        (
            package
            for package in lock["package"]
            if is_first_party(package) and canonicalize_name(package["name"]) not in excluded
        ),
        key=build_key,
    )
    reached: dict[tuple[str, str], dict] = {}
    for root in roots:
        root_name = canonicalize_name(root["name"])
        seen: dict[tuple[str, str], set[str]] = {build_key(root): set()}
        queue = deque([(root, (root_name,), set())])
        while queue:
            package, chain, extras = queue.popleft()
            for edge in iter_edges(package, extras, is_root=package is root):
                name = canonicalize_name(edge["name"])
                if name in excluded or not is_marker_satisfied(edge.get("marker"), environment):
                    continue
                target = find_target(edge, by_name.get(name, []), environment)
                if target is None:
                    continue
                key = build_key(target)
                requested = set(edge.get("extra", []))
                if key in seen and requested <= seen[key]:
                    continue
                seen[key] = seen.get(key, set()) | requested
                target_chain = (*chain, name)
                queue.append((target, target_chain, seen[key]))
                if is_first_party(target):
                    continue
                record = reached.setdefault(
                    key,
                    {
                        "name": name,
                        "version": key[1],
                        "source": target.get("source", {}),
                        "chain": list(target_chain),
                        "declared_by": set(),
                        "required_by": set(),
                    },
                )
                if len(target_chain) < len(record["chain"]):
                    record["chain"] = list(target_chain)
                record["required_by"].add(root_name)
                if package is root:
                    record["declared_by"].add(root_name)
    for record in reached.values():
        record["relation"] = "direct" if record["declared_by"] else "transitive"
        record["declared_by"] = sorted(record["declared_by"])
        record["required_by"] = sorted(record["required_by"])
    return reached


def get_declared_support(info: dict, python_version: str) -> str:
    """Read the classifier signal: ``declared``, ``not-declared`` (other minors listed) or ``unknown``."""
    major, minor = Version(python_version).release[:2]
    minors = {
        match.group(1)
        for classifier in info.get("classifiers") or []
        if (match := re.fullmatch(r"Programming Language :: Python :: (\d+\.\d+)", classifier))
    }
    if f"{major}.{minor}" in minors:
        return "declared"
    return "not-declared" if minors else "unknown"


def get_upload_date(data: dict) -> str:
    uploads = [
        artifact["upload_time_iso_8601"]
        for artifact in data.get("urls", [])
        if "upload_time_iso_8601" in artifact
    ]
    return min(uploads)[:10] if uploads else ""


def audit_version(data: dict, python_version: str, architecture: str, fallback: bool) -> dict:
    return audit_wheels.audit_release(data, python_version, architecture, pure_python_fallback=fallback) | {
        "support": get_declared_support(data["info"], python_version),
        "uploaded": get_upload_date(data),
    }


def audit_record(record: dict, platform: str, python_version: str, fallbacks: set[str]) -> dict:
    architecture = PLATFORMS[platform]
    fallback = record["name"] in fallbacks
    result = {key: value for key, value in record.items() if key != "source"} | {"platform": platform}
    source = record["source"]
    try:
        if "url" in source:
            filename = unquote(urlparse(source["url"]).path.rsplit("/", 1)[-1])
            compatible = audit_wheels.is_wheel_compatible(filename, python_version, architecture)
            result["selected"] = {"status": "wheel-url" if compatible else "source-url-review"}
        elif "registry" in source:
            result["selected"] = audit_version(
                audit_wheels.fetch_release(record["name"], record["version"]),
                python_version,
                architecture,
                fallback,
            )
        else:
            result["selected"] = {"status": "source-review", "source": source}
            return result
        result["latest"] = audit_version(
            audit_wheels.fetch_release(record["name"]), python_version, architecture, fallback
        )
    except audit_wheels.LOOKUP_ERRORS as error:
        result["lookup_error"] = str(error)
    return result


def is_missing_wheel(record: dict) -> bool:
    return record.get("selected", {}).get("status") not in ("wheel", "wheel-url")


def is_gap(record: dict) -> bool:
    """
    Flag lookup errors, locked releases without a target wheel, and newer releases that declare the target.

    A missing classifier alone is not a gap: most projects add a new minor's classifier late or never.
    """
    selected, latest = record.get("selected", {}), record.get("latest", {})
    return (
        "lookup_error" in record
        or is_missing_wheel(record)
        or (
            selected.get("support") != "declared"
            and latest.get("support") == "declared"
            and latest.get("version") != selected.get("version")
        )
    )


def build_disposition(record: dict, cutoff: date | None) -> str:
    latest = record.get("latest")
    if "lookup_error" in record:
        return "lookup error"
    if not is_gap(record):
        return "ok"
    if latest is None:
        return "review source"
    held = cutoff is not None and latest["uploaded"] and date.fromisoformat(latest["uploaded"]) > cutoff
    suffix = " (inside cooldown)" if held else ""
    if not is_missing_wheel(record):
        # The locked wheel may already work on the target; only a target failure justifies an upgrade.
        return f"newer release declares target ({latest['version']}){suffix}"
    if record["selected"].get("disposition") in ("pure-python-sdist", "pure-python-fallback"):
        return "builds without a compiler"
    if latest["status"] != "wheel":
        return "not yet available"
    return f"upgrade to {latest['version']}{suffix}"


def describe_release(audit: dict) -> str:
    """Show an sdist's classification instead of the bare ``source-only`` status."""
    status = audit.get("disposition", audit.get("status", ""))
    return f"{status}, {audit['support']}" if "support" in audit else status


def render_markdown(records: list[dict], cutoff: date | None, *, include_ready: bool = False) -> str:
    rows = [
        "| Package | Locked | Relation | Platforms | Selected | Newest | Disposition | Declared by | Members reaching it | Chain |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    merged: dict[tuple, list[str]] = {}
    for record in records:
        if not include_ready and not is_gap(record):
            continue
        selected, latest = record.get("selected", {}), record.get("latest", {})
        row = (
            record["name"],
            record["version"],
            record["relation"],
            describe_release(selected),
            f"{latest.get('version', '')} {describe_release(latest)} {latest.get('uploaded', '')}".strip(),
            build_disposition(record, cutoff),
            ", ".join(record["declared_by"]),
            str(len(record["required_by"])),
            " → ".join(record["chain"]),
        )
        merged.setdefault(row, []).append(record["platform"])
    for row, platforms in merged.items():
        name, version, relation, *rest = row
        rows.append("| " + " | ".join([name, version, relation, ", ".join(platforms), *rest]) + " |")
    return "\n".join(rows) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", required=True, type=Path, help="uv.lock to trace")
    parser.add_argument("--python-version", required=True, help="Exact target version, e.g. 3.15.0")
    parser.add_argument(
        "--exclude-member", action="append", default=[], help="First-party member excluded on the target"
    )
    parser.add_argument(
        "--package",
        action="append",
        default=[],
        help="Audit only these packages, e.g. to see what they unblock",
    )
    parser.add_argument(
        "--pure-python-fallback", action="append", default=[], help="Confirmed fallback package"
    )
    parser.add_argument("--cooldown-days", type=int, help="Root exclude-newer window, e.g. 4")
    parser.add_argument("--as-of", type=date.fromisoformat, help="Reference date for --cooldown-days")
    parser.add_argument("--output", required=True, type=Path, help="JSON records")
    parser.add_argument("--markdown", type=Path, help="Optional table of packages that need attention")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"3\.\d+\.\d+", args.python_version):
        parser.error("--python-version must be a final Python 3 patch version, e.g. 3.15.0")
    if (args.cooldown_days is None) != (args.as_of is None):
        parser.error("--cooldown-days and --as-of go together")
    cutoff = args.as_of - timedelta(days=args.cooldown_days) if args.as_of else None
    lock = tomllib.loads(args.lock.read_text(encoding="utf-8"))
    excluded = {canonicalize_name(name) for name in args.exclude_member}
    selected_names = {canonicalize_name(name) for name in args.package}
    fallbacks = {canonicalize_name(name) for name in args.pure_python_fallback}
    jobs = []
    for platform, machine in PLATFORMS.items():
        environment = scan_wheel_floors.build_environment(args.python_version, machine)
        for _, record in sorted(walk_lock(lock, environment, excluded).items()):
            if not selected_names or record["name"] in selected_names:
                jobs.append((record, platform))
    missing = selected_names - {record["name"] for record, _ in jobs}
    with ThreadPoolExecutor(max_workers=12) as pool:
        records = list(
            pool.map(lambda job: audit_record(job[0], job[1], args.python_version, fallbacks), jobs)
        )
    args.output.write_text(
        json.dumps(
            {
                "python": args.python_version,
                "lock": args.lock.as_posix(),
                "unreached": sorted(missing),
                "packages": records,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    if args.markdown:
        args.markdown.write_text(
            render_markdown(records, cutoff, include_ready=bool(selected_names)), encoding="utf-8"
        )
    return int(any("lookup_error" in record for record in records))


if __name__ == "__main__":
    sys.exit(main())
