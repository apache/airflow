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

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is importable

from common_precommit_utils import AIRFLOW_SOURCES_ROOT_PATH

RELEASE_NOTES_FILE_PATH = AIRFLOW_SOURCES_ROOT_PATH / "RELEASE_NOTES.rst"
REPRODUCIBLE_BUILD_FILE = AIRFLOW_SOURCES_ROOT_PATH / "airflow" / "reproducible_build.yaml"

if __name__ == "__main__":
    hash_md5 = md5()
    hash_md5.update(RELEASE_NOTES_FILE_PATH.read_bytes())
    release_notes_hash = hash_md5.hexdigest()
    reproducible_build_text = REPRODUCIBLE_BUILD_FILE.read_text()
    reproducible_build = yaml.safe_load(reproducible_build_text)
    old_hash = reproducible_build["release-notes-hash"]
    if release_notes_hash != old_hash:
        # Replace the hash in the file
        reproducible_build["release-notes-hash"] = release_notes_hash
        reproducible_build["source-date-epoch"] = int(time())
    REPRODUCIBLE_BUILD_FILE.write_text(yaml.dump(reproducible_build))
