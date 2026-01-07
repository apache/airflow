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

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    pytest.skip("AWS auth manager is only compatible with Airflow >= 3.0.0", allow_module_level=True)

from airflow.providers.amazon.aws.cli.definition import AWS_AUTH_MANAGER_COMMANDS


class TestAwsCliDefinition:
    def test_aws_auth_manager_cli_commands(self):
        assert len(AWS_AUTH_MANAGER_COMMANDS) == 2
