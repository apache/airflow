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

from unittest.mock import MagicMock, patch

import pytest
from kylinpy.exceptions import KylinCubeError

from airflow.exceptions import AirflowException
from airflow.providers.apache.kylin.hooks.kylin import KylinHook

pytestmark = pytest.mark.db_test


class TestKylinHook:
    def setup_method(self) -> None:
        self.hook = KylinHook(kylin_conn_id="kylin_default", project="learn_kylin")

    @patch("kylinpy.Kylin.get_job")
    def test_get_job_status(self, mock_job):
        job = MagicMock()
        job.status = "ERROR"
        mock_job.return_value = job
        assert self.hook.get_job_status("123") == "ERROR"

    @patch("kylinpy.Kylin.get_datasource")
    def test_cube_run(self, cube_source):
        class MockCubeSource:
            def invoke_command(self, command, **kwargs):
                invoke_command_list = [
                    "fullbuild",
                    "build",
                    "merge",
                    "refresh",
                    "delete",
                    "build_streaming",
                    "merge_streaming",
                    "refresh_streaming",
                    "disable",
                    "enable",
                    "purge",
                    "clone",
                    "drop",
                ]
                if command in invoke_command_list:
                    return {"code": "000", "data": {}}
                else:
                    raise KylinCubeError(f"Unsupported invoke command for datasource: {command}")

        cube_source.return_value = MockCubeSource()
        response_data = {"code": "000", "data": {}}
        assert self.hook.cube_run("kylin_sales_cube", "build") == response_data
        assert self.hook.cube_run("kylin_sales_cube", "refresh") == response_data
        assert self.hook.cube_run("kylin_sales_cube", "merge") == response_data
        assert self.hook.cube_run("kylin_sales_cube", "build_streaming") == response_data
        with pytest.raises(AirflowException):
            self.hook.cube_run(
                "kylin_sales_cube",
                "build123",
            )
