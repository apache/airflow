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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_precommit_utils import (
    initialize_breeze_precommit,
    run_command_via_breeze_shell,
    validate_cmd_result,
)

initialize_breeze_precommit(__name__, __file__)
result = run_command_via_breeze_shell(
    [
        "bash",
        "-c",
        "airflow db migrate --to-revision heads && cd airflow-core/src/airflow && alembic check",
    ],
    backend="sqlite",
    warn_image_upgrade_needed=True,
)

validate_cmd_result(result, include_ci_env_check=True)
