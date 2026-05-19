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
"""Interpret a provider's pre_extras_install.yaml manifest.

This is the only code that runs when a provider is registered in
PROVIDERS_NEEDING_PRE_EXTRAS_INSTALL inside scripts/docker/entrypoint_ci.sh.
The manifest is data, not code: providers can declare pinned-checksum
downloads, archive extractions under /opt or /tmp, and env-var exports, but
nothing else. Maintainers reviewing a provider's manifest only need to verify
that the URL and sha256 belong to a trusted upstream.

See contributing-docs/12_provider_distributions.rst for the format.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import shlex
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import NoReturn

import yaml

PROVIDERS_ROOT = Path("/opt/airflow/providers")
ALLOWED_EXTRACT_PREFIXES = ("/opt/", "/tmp/")
ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ALLOWED_TOP_LEVEL_KEYS = {"downloads", "env"}
ALLOWED_DOWNLOAD_KEYS = {"url", "sha256", "extract_to"}


def fail(msg: str) -> NoReturn:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def validate_manifest(manifest: object, provider_id: str) -> dict:
    if not isinstance(manifest, dict):
        fail(f"manifest for {provider_id} must be a mapping")
    extra = set(manifest) - ALLOWED_TOP_LEVEL_KEYS
    if extra:
        fail(f"manifest for {provider_id} has unknown top-level keys: {sorted(extra)}")
    downloads = manifest.get("downloads", [])
    if not isinstance(downloads, list):
        fail("'downloads' must be a list")
    for i, entry in enumerate(downloads):
        if not isinstance(entry, dict):
            fail(f"downloads[{i}] must be a mapping")
        unknown = set(entry) - ALLOWED_DOWNLOAD_KEYS
        if unknown:
            fail(f"downloads[{i}] has unknown keys: {sorted(unknown)}")
        missing = ALLOWED_DOWNLOAD_KEYS - set(entry)
        if missing:
            fail(f"downloads[{i}] is missing required keys: {sorted(missing)}")
        url = entry["url"]
        if not isinstance(url, str) or not url.startswith("https://"):
            fail(f"downloads[{i}].url must be an https:// string (got {url!r})")
        sha256 = entry["sha256"]
        if not isinstance(sha256, str) or not SHA256_RE.match(sha256):
            fail(f"downloads[{i}].sha256 must be 64 lowercase hex chars")
        extract_to = entry["extract_to"]
        if not isinstance(extract_to, str) or not any(
            extract_to.startswith(prefix) for prefix in ALLOWED_EXTRACT_PREFIXES
        ):
            fail(
                f"downloads[{i}].extract_to must start with one of {ALLOWED_EXTRACT_PREFIXES} "
                f"(got {extract_to!r})"
            )
        if ".." in Path(extract_to).parts:
            fail(f"downloads[{i}].extract_to cannot contain '..'")
    env = manifest.get("env", {})
    if not isinstance(env, dict):
        fail("'env' must be a mapping")
    for name, value in env.items():
        if not isinstance(name, str) or not ENV_NAME_RE.match(name):
            fail(f"env name {name!r} must match {ENV_NAME_RE.pattern}")
        if not isinstance(value, str):
            fail(f"env value for {name} must be a string (got {type(value).__name__})")
    return manifest


def download_with_checksum(url: str, expected_sha256: str, dest: Path) -> None:
    print(f"Downloading {url}")
    digest = hashlib.sha256()
    with urllib.request.urlopen(url) as response, dest.open("wb") as out:
        while True:
            chunk = response.read(64 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            out.write(chunk)
    got = digest.hexdigest()
    if got != expected_sha256:
        fail(f"sha256 mismatch for {url}: expected {expected_sha256}, got {got}")


def safe_extract(archive: Path, target: Path) -> None:
    target = target.resolve()
    target.mkdir(parents=True, exist_ok=True)
    name = archive.name.lower()
    if name.endswith((".tar.gz", ".tgz", ".tar")):
        with tarfile.open(archive) as tf:
            for member in tf.getmembers():
                member_path = (target / member.name).resolve()
                if member_path != target and target not in member_path.parents:
                    fail(f"archive entry escapes target: {member.name}")
            tf.extractall(target)
    elif name.endswith(".zip"):
        with zipfile.ZipFile(archive) as zf:
            for member_name in zf.namelist():
                member_path = (target / member_name).resolve()
                if member_path != target and target not in member_path.parents:
                    fail(f"archive entry escapes target: {member_name}")
            zf.extractall(target)
    else:
        fail(f"unsupported archive extension: {archive.name}")


def manifest_path_for(provider_id: str) -> Path:
    if not re.match(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$", provider_id):
        fail(f"invalid provider id: {provider_id!r}")
    return PROVIDERS_ROOT / provider_id.replace(".", "/") / "scripts/pre_extras_install.yaml"


def emit_env_file(env: dict, env_file: Path) -> None:
    lines = [f"export {name}={shlex.quote(value)}" for name, value in env.items()]
    env_file.write_text("\n".join(lines) + ("\n" if lines else ""))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("provider_id", help="Dotted provider id (e.g. ibm.mq)")
    parser.add_argument(
        "--emit-env-to",
        required=True,
        type=Path,
        help="Path to write the export statements for env vars defined by the manifest",
    )
    args = parser.parse_args()

    manifest_file = manifest_path_for(args.provider_id)
    if not manifest_file.is_file():
        fail(f"manifest not found: {manifest_file}")
    with manifest_file.open() as fh:
        manifest = validate_manifest(yaml.safe_load(fh), args.provider_id)

    with tempfile.TemporaryDirectory(
        prefix=f"pre_extras_install_{args.provider_id.replace('.', '_')}_"
    ) as tmpdir:
        tmp = Path(tmpdir)
        for index, entry in enumerate(manifest.get("downloads", [])):
            archive_name = Path(entry["url"]).name or f"download_{index}"
            archive = tmp / f"{index}_{archive_name}"
            download_with_checksum(entry["url"], entry["sha256"], archive)
            safe_extract(archive, Path(entry["extract_to"]))
            archive.unlink(missing_ok=True)

    emit_env_file(manifest.get("env", {}), args.emit_env_to)


if __name__ == "__main__":
    main()
