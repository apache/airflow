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

from unittest import mock

import pytest

from airflow.providers.alibaba.cloud.links.maxcompute import MaxComputeLogViewLink

MAXCOMPUTE_LINK_MODULE = "airflow.providers.alibaba.cloud.links.maxcompute.{}"
MOCK_TASK_ID = "run_sql"
MOCK_SQL = "SELECT 1"
MOCK_INSTANCE_ID = "mock_instance_id"


class TestMaxComputeLogViewLink:
    @pytest.mark.parametrize(
        ("xcom_value", "expected_link"),
        [
            pytest.param("http://mock_url.com", "http://mock_url.com", id="has-log-link"),
            pytest.param(None, "", id="no-log-link"),
        ],
    )
    @mock.patch(MAXCOMPUTE_LINK_MODULE.format("XCom"))
    def test_get_link(self, mock_xcom, xcom_value, expected_link):
        mock_xcom.get_value.return_value = xcom_value

        link = MaxComputeLogViewLink().get_link(
            operator=mock.MagicMock(),
            ti_key=mock.MagicMock(),
        )

        assert link == expected_link

    def test_persist(self):
        mock_task_instance = mock.MagicMock()
        mock_context = {"task_instance": mock_task_instance}
        mock_url = "mock_url"

        MaxComputeLogViewLink.persist(
            context=mock_context,
            log_view_url=mock_url,
        )

        mock_task_instance.xcom_push.assert_called_once_with(
            key=MaxComputeLogViewLink.key,
            value=mock_url,
        )
