#
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

from importlib import reload
from unittest import mock

import pytest

from airflow.cli.commands.standalone_command import StandaloneCommand
from airflow.executors import executor_loader
from airflow.executors.executor_constants import (
    CELERY_EXECUTOR,
    KUBERNETES_EXECUTOR,
    LOCAL_EXECUTOR,
)


class TestStandaloneCommand:
    @pytest.mark.parametrize(
        "conf_executor_name",
        [LOCAL_EXECUTOR, CELERY_EXECUTOR, KUBERNETES_EXECUTOR],
    )
    def test_calculate_env(self, conf_executor_name):
        """Should always force a local executor compatible with the db."""
        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW__CORE__EXECUTOR": conf_executor_name,
            },
        ):
            reload(executor_loader)
            env = StandaloneCommand().calculate_env()
            # all non local executors will fall back to localesecutor
            assert env["AIRFLOW__CORE__EXECUTOR"] == LOCAL_EXECUTOR
