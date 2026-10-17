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
Audit target-specific third-party pins and the newest PyPI releases for Linux wheels.

Compiled sources are evidence for review, not proof that compilation is mandatory.
Use --pure-python-fallback only after confirming the release's build supports that fallback.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tarfile
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from functools import cache
from io import BytesIO
from pathlib import Path
from urllib.parse import unquote, urlparse

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename
from packaging.version import Version

ARCHITECTURES = {"amd64": "x86_64", "arm64": "aarch64"}
COMPILED_SUFFIXES = (".c", ".cc", ".cpp", ".cxx", ".pyx", ".rs", ".f", ".f90")
COMPILED_NAMES = {"Cargo.toml", "CMakeLists.txt", "meson.build", "build.rs"}
LOOKUP_ERRORS = (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile)


def read_requirements(path: Path) -> dict[str, tuple[str, str | None]]:
    pins = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        requirement = Requirement(line)
        name = canonicalize_name(requirement.name)
        specifiers = list(requirement.specifier)
        if requirement.marker:
            raise ValueError(f"Apply target markers before auditing: {line!r}")
        if requirement.url:
            pin = ("", requirement.url)
        elif len(specifiers) == 1 and specifiers[0].operator == "==" and "*" not in specifiers[0].version:
            pin = (specifiers[0].version, None)
        else:
            raise ValueError(f"Expected an exact version or URL pin: {line!r}")
        if name in pins and pins[name] != pin:
            raise ValueError(f"Conflicting pins for {name} in {path}")
        pins[name] = pin
    if not pins:
        raise ValueError(f"No third-party pins in {path}")
    return pins


def is_wheel_compatible(filename: str, python_version: str, architecture: str) -> bool:
    try:
        _, _, _, tags = parse_wheel_filename(filename)
    except InvalidWheelFilename:
        return False
    major, minor = Version(python_version).release[:2]
    for tag in tags:
        abi3 = re.fullmatch(r"cp3(\d+)", tag.interpreter)
        python_ok = (
            tag.interpreter == "py3"
            or tag.interpreter == f"cp{major}{minor}"
            and tag.abi == f"cp{major}{minor}"
            or tag.abi == "abi3"
            and abi3 is not None
            and int(abi3.group(1)) <= minor
        )
        platform_ok = tag.platform == "any" or (
            tag.platform.startswith(("manylinux", "musllinux")) and tag.platform.endswith(f"_{architecture}")
        )
        if python_ok and platform_ok:
            return True
    return False


@cache
def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read()


def fetch_release(name: str, version: str = "") -> dict:
    suffix = f"/{version}" if version else ""
    return json.loads(fetch_bytes(f"https://pypi.org/pypi/{name}{suffix}/json"))


def classify_sdist(payload: bytes, filename: str, *, pure_python_fallback: bool) -> dict:
    if filename.endswith(".zip"):
        with zipfile.ZipFile(BytesIO(payload)) as archive:
            names = archive.namelist()
    else:
        with tarfile.open(fileobj=BytesIO(payload)) as archive:
            names = archive.getnames()
    evidence = sorted(
        name for name in names if name.endswith(COMPILED_SUFFIXES) or Path(name).name in COMPILED_NAMES
    )
    disposition = "pure-python-sdist"
    if evidence:
        disposition = "pure-python-fallback" if pure_python_fallback else "compiled-sources-review"
    return {"disposition": disposition, "compiled_source_evidence": evidence[:5]}


def audit_release(data: dict, python_version: str, architecture: str, *, pure_python_fallback: bool) -> dict:
    result = {
        "version": data["info"]["version"],
        "requires_python": data["info"].get("requires_python", ""),
    }
    files = [artifact for artifact in data.get("urls", []) if not artifact.get("yanked")]
    available = [
        artifact
        for artifact in files
        if SpecifierSet(artifact.get("requires_python") or result["requires_python"] or "").contains(
            python_version, prereleases=True
        )
    ]
    for artifact in available:
        if artifact["packagetype"] == "bdist_wheel" and is_wheel_compatible(
            artifact["filename"], python_version, architecture
        ):
            return result | {"status": "wheel", "file": artifact["filename"], "url": artifact["url"]}
    sdists = [artifact for artifact in available if artifact["packagetype"] == "sdist"]
    if sdists:
        artifact = sdists[0]
        classification = classify_sdist(
            fetch_bytes(artifact["url"]), artifact["filename"], pure_python_fallback=pure_python_fallback
        )
        return result | {"status": "source-only", "file": artifact["filename"], **classification}
    return result | {"status": "unavailable"}


def audit_requirement(
    name: str, pin: tuple[str, str | None], platform: str, python_version: str, fallbacks: set[str]
) -> dict:
    version, url = pin
    result = {"name": name, "platform": platform, "pin": version or url}
    try:
        if url:
            filename = unquote(urlparse(url).path.rsplit("/", 1)[-1])
            result["selected"] = {
                "status": "wheel-url"
                if is_wheel_compatible(filename, python_version, ARCHITECTURES[platform])
                else "source-url-review",
                "url": url,
            }
        else:
            result["selected"] = audit_release(
                fetch_release(name, version),
                python_version,
                ARCHITECTURES[platform],
                pure_python_fallback=name in fallbacks,
            )
        latest = fetch_release(name)
        result["latest"] = audit_release(
            latest, python_version, ARCHITECTURES[platform], pure_python_fallback=name in fallbacks
        )
    except LOOKUP_ERRORS as error:
        result["lookup_error"] = str(error)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-version", required=True, help="Exact target version, e.g. 3.15.0")
    parser.add_argument("--amd64", required=True, type=Path, help="Resolved third-party pins for AMD64")
    parser.add_argument("--arm64", required=True, type=Path, help="Resolved third-party pins for ARM64")
    parser.add_argument(
        "--pure-python-fallback", action="append", default=[], help="Confirmed fallback package"
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"3\.\d+\.\d+", args.python_version):
        parser.error("--python-version must be a final Python 3 patch version, e.g. 3.15.0")
    fallbacks = {canonicalize_name(name) for name in args.pure_python_fallback}
    jobs = [
        (name, pin, platform, args.python_version, fallbacks)
        for platform in ARCHITECTURES
        for name, pin in sorted(read_requirements(getattr(args, platform)).items())
    ]
    with ThreadPoolExecutor(max_workers=12) as pool:
        records = list(pool.map(lambda job: audit_requirement(*job), jobs))
    args.output.write_text(
        json.dumps(
            {"python": args.python_version, "audit_interpreter": sys.version.split()[0], "packages": records},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return int(any("lookup_error" in record for record in records))


if __name__ == "__main__":
    sys.exit(main())
