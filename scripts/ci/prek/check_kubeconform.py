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
#   "requests>=2.31.0",
#   "rich>=13.6.0",
# ]
# ///

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_ROOT_PATH, console, initialize_breeze_prek

initialize_breeze_prek(__name__, __file__)

res_setup = subprocess.run(["breeze", "k8s", "setup-env"], check=True)
if res_setup.returncode != 0:
    console.print("[red]\nError while setting up k8s environment.")
    sys.exit(res_setup.returncode)

HELM_BIN_PATH = AIRFLOW_ROOT_PATH / ".venv" / "bin" / "helm"

ps = subprocess.Popen(
    [os.fspath(HELM_BIN_PATH), "template", ".", "-f", "values.yaml"],
    cwd=AIRFLOW_ROOT_PATH / "chart",
    stdout=subprocess.PIPE,
)
result = subprocess.run(
    ["docker", "run", "--rm", "-i", "ghcr.io/yannh/kubeconform:latest-alpine", "--strict"],
    stdin=ps.stdout,
    check=False,
)
if result.returncode != 0:
    console.print("[red]\nError while running kubeconform.")
    sys.exit(result.returncode)
