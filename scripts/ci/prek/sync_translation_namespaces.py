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
"""Sync the translation namespace file list in SKILL.md with the English locale directory."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_ROOT_PATH, insert_documentation

EN_LOCALE_DIR = (
    AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "ui" / "public" / "i18n" / "locales" / "en"
)
SKILL_FILE = AIRFLOW_ROOT_PATH / ".github" / "skills" / "airflow-translations" / "SKILL.md"

START_MARKER = "<!-- START namespace-files, please keep comment here to allow auto update -->"
END_MARKER = "<!-- END namespace-files, please keep comment here to allow auto update -->"

if __name__ == "__main__":
    json_files = sorted(p.name for p in EN_LOCALE_DIR.glob("*.json"))
    if not json_files:
        print(f"No JSON files found in {EN_LOCALE_DIR}")
        sys.exit(1)

    formatted = ", ".join(f"`{f}`" for f in json_files) + "\n"
    insert_documentation(
        file_path=SKILL_FILE,
        content=[formatted],
        header=START_MARKER,
        footer=END_MARKER,
        extra_information="translation namespace file list",
    )
