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

import sys
from hashlib import md5
from pathlib import Path
from time import time

import yaml

sys.path.insert(
    0, str(Path(__file__).parent.resolve())
)  # make sure common_precommit_utils is importable

from common_precommit_utils import AIRFLOW_SOURCES_ROOT_PATH

CHART_DIR = AIRFLOW_SOURCES_ROOT_PATH / "chart"

AIRFLOW_RELEASE_NOTES_FILE_PATH = AIRFLOW_SOURCES_ROOT_PATH / "RELEASE_NOTES.rst"
AIRFLOW_REPRODUCIBLE_BUILD_FILE = (
    AIRFLOW_SOURCES_ROOT_PATH / "airflow" / "reproducible_build.yaml"
)

CHART_RELEASE_NOTES_FILE_PATH = CHART_DIR / "RELEASE_NOTES.rst"
CHART_REPRODUCIBLE_BUILD_FILE = CHART_DIR / "reproducible_build.yaml"


def write_reproducible_build_file(
    release_notes_path: Path, reproducible_build_path: Path
):
    hash_md5 = md5()
    hash_md5.update(release_notes_path.read_bytes())
    release_notes_hash = hash_md5.hexdigest()
    reproducible_build_text = reproducible_build_path.read_text()
    reproducible_build = yaml.safe_load(reproducible_build_text)
    old_hash = reproducible_build["release-notes-hash"]
    if release_notes_hash != old_hash:
        # Replace the hash in the file
        reproducible_build["release-notes-hash"] = release_notes_hash
        reproducible_build["source-date-epoch"] = int(time())
    reproducible_build_path.write_text(yaml.dump(reproducible_build))


if __name__ == "__main__":
    write_reproducible_build_file(
        release_notes_path=AIRFLOW_RELEASE_NOTES_FILE_PATH,
        reproducible_build_path=AIRFLOW_REPRODUCIBLE_BUILD_FILE,
    )
    write_reproducible_build_file(
        release_notes_path=CHART_RELEASE_NOTES_FILE_PATH,
        reproducible_build_path=CHART_REPRODUCIBLE_BUILD_FILE,
    )
