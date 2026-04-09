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
"""Merge incremental provider data into an existing registry dataset.

During incremental builds, extract_metadata.py produces JSON containing
only the updated provider(s).  This script merges those partial results
into the full dataset previously downloaded from S3, so that the Eleventy
build still sees all providers.

Usage:
    python dev/registry/merge_registry_data.py \
        --existing-providers /tmp/existing-registry/providers.json \
        --existing-modules /tmp/existing-registry/modules.json \
        --new-providers dev/registry/providers.json \
        --new-modules dev/registry/modules.json \
        --output dev/registry/
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from registry_contract_models import validate_modules_catalog, validate_providers_catalog


def merge(
    existing_providers_path: Path,
    existing_modules_path: Path,
    new_providers_path: Path,
    new_modules_path: Path,
    output_dir: Path,
) -> None:
    """Replace entries for updated providers and write merged JSON files."""
    existing_providers: list[dict] = []
    if existing_providers_path.exists():
        existing_providers = json.loads(existing_providers_path.read_text())["providers"]
    new_providers = json.loads(new_providers_path.read_text())["providers"]

    existing_modules: list[dict] = []
    if existing_modules_path.exists():
        existing_modules = json.loads(existing_modules_path.read_text())["modules"]
    new_modules: list[dict] = []
    if new_modules_path.exists():
        new_modules = json.loads(new_modules_path.read_text())["modules"]

    # IDs being replaced
    new_ids = {p["id"] for p in new_providers}

    # Merge providers: keep existing (except those being replaced), add new
    merged_providers = [p for p in existing_providers if p["id"] not in new_ids]
    merged_providers.extend(new_providers)
    merged_providers.sort(key=lambda p: p["name"].lower())

    # Merge modules: keep existing (except for replaced providers), add new
    merged_modules = [m for m in existing_modules if m["provider_id"] not in new_ids]
    merged_modules.extend(new_modules)

    # Sort modules by provider's last_updated date (newest first)
    date_by_provider = {p["id"]: p.get("last_updated", "") for p in merged_providers}
    merged_modules.sort(key=lambda m: date_by_provider.get(m["provider_id"], ""), reverse=True)

    providers_payload = validate_providers_catalog({"providers": merged_providers})
    modules_payload = validate_modules_catalog({"modules": merged_modules})

    # Write merged output
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "providers.json").write_text(json.dumps(providers_payload, indent=2) + "\n")
    (output_dir / "modules.json").write_text(json.dumps(modules_payload, indent=2) + "\n")

    print(f"Merged {len(new_ids)} updated provider(s) into {len(merged_providers)} total providers")
    print(f"Total modules: {len(merged_modules)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge incremental registry data into existing dataset")
    parser.add_argument("--existing-providers", required=True, type=Path)
    parser.add_argument("--existing-modules", required=True, type=Path)
    parser.add_argument("--new-providers", required=True, type=Path)
    parser.add_argument("--new-modules", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    merge(args.existing_providers, args.existing_modules, args.new_providers, args.new_modules, args.output)


if __name__ == "__main__":
    main()
