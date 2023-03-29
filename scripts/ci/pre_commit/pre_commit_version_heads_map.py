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

import ast
import sys
from pathlib import Path

from packaging.version import Version

PROJECT_SOURCE_ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent

DB_FILE = PROJECT_SOURCE_ROOT_DIR / "airflow" / "utils" / "db.py"

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is importable

from common_precommit_utils import read_airflow_version  # noqa: E402


def read_revision_heads_map():
    revision_heads_map_ast_obj = ast.parse(open(DB_FILE).read())

    revision_heads_map_ast = [
        a
        for a in revision_heads_map_ast_obj.body
        if isinstance(a, ast.Assign) and a.targets[0].id == "REVISION_HEADS_MAP"
    ][0]

    revision_heads_map = ast.literal_eval(revision_heads_map_ast.value)

    return revision_heads_map.keys()


if __name__ == "__main__":
    airflow_version = Version(read_airflow_version())
    if airflow_version.is_devrelease or "b" in (airflow_version.pre or ()):
        exit(0)
    versions = read_revision_heads_map()
    if airflow_version.base_version not in versions:
        print("Current airflow version is not in the REVISION_HEADS_MAP")
        print("Current airflow version:", airflow_version)
        print("Please add the version to the REVISION_HEADS_MAP at:", DB_FILE)
        sys.exit(3)
