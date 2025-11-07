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

import pytest


@pytest.fixture(autouse=True)
def skip_if_env_var_not_set() -> None:
    """Skip system tests if required environment variables are not set."""
    import os

    required_env_vars = ("SYSTEM_TESTS_ENV_ID",)
    greatexpectations_env_vars = (
        "GX_CLOUD_ACCESS_TOKEN",
        "GX_CLOUD_ORGANIZATION_ID",
        "GX_CLOUD_WORKSPACE_ID",
    )

    for env in required_env_vars:
        if env not in os.environ:
            pytest.skip(f"Missing required environment variable {env}")

    for env in greatexpectations_env_vars:
        if env not in os.environ:
            pytest.skip(f"Missing required Great Expectations environment variable {env}")
