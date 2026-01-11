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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "packaging>=25",
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_CORE_SOURCES_PATH, AIRFLOW_PROVIDERS_ROOT_PATH, console

DB_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "utils" / "db.py"
MIGRATION_PATH = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "migrations" / "versions"

FAB_PROVIDER_SRC_PATH = AIRFLOW_PROVIDERS_ROOT_PATH / "fab" / "src"
FAB_DB_FILE = FAB_PROVIDER_SRC_PATH / "airflow" / "providers" / "fab" / "auth_manager" / "models" / "db.py"
FAB_MIGRATION_PATH = FAB_PROVIDER_SRC_PATH / "airflow" / "providers" / "fab" / "migrations" / "versions"


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
            revision_match = re.search(pattern, content)
            _version_match = re.search(version_pattern, content)
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
        match = re.search(pattern, content)
        if not match:
            console.print(
                f"[red]_REVISION_HEADS_MAP not found in {dbfile}. If this has been removed intentionally, "
                "please update scripts/ci/prek/version_heads_map.py"
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
            console.print(f"[yellow]_REVISION_HEADS_MAP updated in {dbfile}. Please commit the changes.")
            sys.exit(1)
