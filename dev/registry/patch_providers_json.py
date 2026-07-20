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
"""Patch providers.json to include backfilled versions in `provider.versions`.

Backfill writes per-version `metadata.json` to disk for the targeted version,
but the `Download data files from S3 for build` workflow step then overwrites
`registry/src/_data/providers.json` with the cached S3 copy. That cached copy
predates the backfilled version (that's why we're backfilling). Without
patching, `providerVersions.js`'s ``provider.versions ∩ availableSet`` filter
drops the on-disk metadata and the Eleventy build emits no page.

This script runs after the S3 download, in the per-matrix backfill job, and
appends the backfilled version(s) into the targeted ``provider.versions``
array. It also defensively re-includes ``provider.version`` (the "latest"
field) -- otherwise patching into a previously-empty ``versions: []`` could
leave the latest field excluded once the array becomes the authoritative
filter.

Backfill-only tool. ``registry-build.yml`` (the full-build workflow) doesn't
need this -- ``extract_metadata.py`` regenerates ``providers.json`` from
scratch there.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from packaging.version import InvalidVersion, Version


def patch(providers_json_path: Path, provider_id: str, versions: list[str]) -> int:
    """Append `versions` into the targeted provider's `versions[]`. Returns exit code."""
    data = json.loads(providers_json_path.read_text())
    target = next((p for p in data["providers"] if p["id"] == provider_id), None)
    if target is None:
        print(
            f"ERROR: provider '{provider_id}' not found in {providers_json_path}. "
            f"This script is for backfilling versions of providers already in the "
            f"registry; for new providers run a full registry-build.yml dispatch."
        )
        return 1

    existing = list(target.get("versions") or [])
    # Always keep `provider.version` (latest) in `versions[]`. Without this,
    # patching into a previously-empty list could leave the latest field out
    # of providerVersions.js's authoritative filter.
    latest = target.get("version")
    if latest and latest not in existing:
        existing.append(latest)

    added: list[str] = []
    for v in versions:
        if v not in existing:
            existing.append(v)
            added.append(v)

    try:
        existing.sort(key=Version, reverse=True)
    except InvalidVersion:
        existing.sort(reverse=True)

    target["versions"] = existing
    providers_json_path.write_text(json.dumps(data, indent=2) + "\n")
    print(f"Patched {provider_id}: added {added or []}; versions list now has {len(existing)} entries")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    parser.add_argument(
        "--providers-json",
        required=True,
        type=Path,
        help="Path to providers.json to modify in place.",
    )
    parser.add_argument(
        "--provider",
        required=True,
        help="Provider ID (e.g. 'amazon', 'common-compat').",
    )
    parser.add_argument(
        "--version",
        required=True,
        action="append",
        help="Version(s) to add (repeat the flag for multiple).",
    )
    args = parser.parse_args()
    return patch(args.providers_json, args.provider, args.version)


if __name__ == "__main__":
    sys.exit(main())
