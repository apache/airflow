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
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import (
    initialize_breeze_prek,
    run_command_via_breeze_shell,
    validate_cmd_result,
)

initialize_breeze_prek(__name__, __file__)

cmd_result = run_command_via_breeze_shell(
    ["python3", "/opt/airflow/scripts/in_container/run_prepare_er_diagram.py"],
    backend="postgres",
    project_name="prek",
    skip_environment_initialization=False,
    warn_image_upgrade_needed=True,
    extra_env={
        "DB_RESET": "true",
    },
)

validate_cmd_result(cmd_result)
