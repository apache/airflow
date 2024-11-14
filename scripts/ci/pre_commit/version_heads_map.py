#!/usr/bin/env python

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
from __future__ import annotations

import os
import sys
from pathlib import Path

import re2

PROJECT_SOURCE_ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent

DB_FILE = PROJECT_SOURCE_ROOT_DIR / "airflow" / "utils" / "db.py"
MIGRATION_PATH = PROJECT_SOURCE_ROOT_DIR / "airflow" / "migrations" / "versions"

PROVIDERS_SRC = PROJECT_SOURCE_ROOT_DIR / "providers" / "src"
FAB_DB_FILE = PROVIDERS_SRC / "airflow" / "providers" / "fab" / "auth_manager" / "models" / "db.py"
FAB_MIGRATION_PATH = PROVIDERS_SRC / "airflow" / "providers" / "fab" / "migrations" / "versions"

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is importable


def revision_heads_map(migration_path):
    rh_map = {}
    pattern = r'revision = "[a-fA-F0-9]+"'
    version_pattern = None
    if migration_path == MIGRATION_PATH:
        version_pattern = r'airflow_version = "\d+\.\d+\.\d+"'
    elif migration_path == FAB_MIGRATION_PATH:
        version_pattern = r'fab_version = "\d+\.\d+\.\d+"'
    filenames = os.listdir(migration_path)

    def sorting_key(filen):
        prefix = filen.split("_")[0]
        return int(prefix) if prefix.isdigit() else 0

    sorted_filenames = sorted(filenames, key=sorting_key)

    for filename in sorted_filenames:
        if not filename.endswith(".py") or filename == "__init__.py":
            continue
        with open(os.path.join(migration_path, filename)) as file:
            content = file.read()
            revision_match = re2.search(pattern, content)
            _version_match = re2.search(version_pattern, content)
            if revision_match and _version_match:
                revision = revision_match.group(0).split('"')[1]
                version = _version_match.group(0).split('"')[1]
                rh_map[version] = revision
    return rh_map


if __name__ == "__main__":
    paths = [(DB_FILE, MIGRATION_PATH), (FAB_DB_FILE, FAB_MIGRATION_PATH)]
    for dbfile, mpath in paths:
        with open(dbfile) as file:
            content = file.read()

        pattern = r"_REVISION_HEADS_MAP:\s*dict\[\s*str\s*,\s*str\s*\]\s*=\s*\{[^}]*\}"
        match = re2.search(pattern, content)
        if not match:
            print(
                f"_REVISION_HEADS_MAP not found in {dbfile}. If this has been removed intentionally, "
                "please update scripts/ci/pre_commit/version_heads_map.py"
            )
            sys.exit(1)

        existing_revision_heads_map = match.group(0)
        rh_map = revision_heads_map(mpath)
        updated_revision_heads_map = "_REVISION_HEADS_MAP: dict[str, str] = {\n"
        for k, v in rh_map.items():
            updated_revision_heads_map += f'    "{k}": "{v}",\n'
        updated_revision_heads_map += "}"
        if updated_revision_heads_map == "_REVISION_HEADS_MAP: dict[str, str] = {\n}":
            updated_revision_heads_map = "_REVISION_HEADS_MAP: dict[str, str] = {}"
        if existing_revision_heads_map != updated_revision_heads_map:
            new_content = content.replace(existing_revision_heads_map, updated_revision_heads_map)

            with open(dbfile, "w") as file:
                file.write(new_content)
            print(f"_REVISION_HEADS_MAP updated in {dbfile}. Please commit the changes.")
            sys.exit(1)
