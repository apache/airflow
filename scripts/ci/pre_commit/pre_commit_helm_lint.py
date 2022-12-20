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

AIRFLOW_SOURCES_DIR = Path(__file__).parents[3].resolve()


sys.path.insert(0, os.fspath(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
sys.path.insert(0, os.fspath(AIRFLOW_SOURCES_DIR))  # make sure setup is imported from Airflow
sys.path.insert(
    0, os.fspath(AIRFLOW_SOURCES_DIR / "dev" / "breeze" / "src")
)  # make sure setup is imported from Airflow

if __name__ == "__main__":
    from airflow_breeze.utils.kubernetes_utils import HELM_BIN_PATH, make_sure_kubernetes_tools_are_installed
    from airflow_breeze.utils.run_utils import run_command

    make_sure_kubernetes_tools_are_installed()

    result = run_command(
        [os.fspath(HELM_BIN_PATH), "lint", ".", "-f", "values.yaml"],
        check=False,
        cwd=AIRFLOW_SOURCES_DIR / "chart",
    )
    sys.exit(result.returncode)
