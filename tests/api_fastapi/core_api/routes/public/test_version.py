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

from unittest import mock

import pytest

from tests_common.test_utils.db import clear_db_jobs

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class TestVersionEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_jobs()

    def teardown_method(self):
        clear_db_jobs()


class TestGetVersion(TestVersionEndpoint):
    @mock.patch(
        "airflow.api_fastapi.core_api.routes.public.version.airflow.__version__",
        "MOCK_VERSION",
    )
    @mock.patch(
        "airflow.api_fastapi.core_api.routes.public.version.get_airflow_git_version",
        return_value="GIT_COMMIT",
    )
    def test_airflow_version_info(self, mock_get_airflow_get_commit, client):
        response = client().get("/public/version")

        assert 200 == response.status_code
        assert {"git_version": "GIT_COMMIT", "version": "MOCK_VERSION"} == response.json()
        mock_get_airflow_get_commit.assert_called_once_with()
