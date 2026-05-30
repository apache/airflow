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

from airflow.providers.airbyte.links.airbyte_connection import AirbyteConnectionLink
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult


TEST_CONNECTION_ID = "some-very-long-uuid"
TEST_WORKSPACE_ID = "another-long-uuid"
TEST_AIRBYTE_CLOUD_HOST = "https://cloud.airbyte.com"
TEST_AIRBYTE_OSS_HOST = "https://example.com"

EXPECTED_CLOUD_CONNECTION_LINK_FORMAT = (
    TEST_AIRBYTE_CLOUD_HOST + "/workspaces/{workspace_id}/connections/{connection_id}"
)
EXPECTED_CLOUD_CONNECTION_LINK = EXPECTED_CLOUD_CONNECTION_LINK_FORMAT.format(
    workspace_id=TEST_WORKSPACE_ID, connection_id=TEST_CONNECTION_ID
)

EXPECTED_OSS_CONNECTION_LINK_FORMAT = TEST_AIRBYTE_OSS_HOST + "/connections/{connection_id}"
EXPECTED_OSS_CONNECTION_LINK = EXPECTED_OSS_CONNECTION_LINK_FORMAT.format(connection_id=TEST_CONNECTION_ID)


class TestAirbyteConnectionLink:
    airbyte_conn_id = "test_airbyte_conn_id"
    connection_id = "test_airbyte_connection"
    job_id = 1
    wait_seconds = 0
    timeout = 360

    @pytest.mark.db_test
    def test_get_link_airbyte_managed(
        self, create_task_instance_of_operator, dag_maker, session, mock_supervisor_comms
    ):
        link = AirbyteConnectionLink()

        ti = create_task_instance_of_operator(
            AirbyteTriggerSyncOperator,
            dag_id="test_airbyte_link_dag",
            task_id="test_airbyte_link_task",
            airbyte_conn_id=self.airbyte_conn_id,
            workspace_id=TEST_WORKSPACE_ID,
            connection_id=TEST_CONNECTION_ID,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )

        session.add(ti)
        session.commit()

        task = dag_maker.dag.get_task(ti.task_id)

        link.persist(context={"ti": ti, "task": task})

        if mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "connection_id": task.connection_id,
                    "workspace_id": task.workspace_id,
                    "server_url": "https://api.airbyte.com/v1/another-long-uuid",
                },
            )

        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)

        assert actual_url == EXPECTED_CLOUD_CONNECTION_LINK

    @pytest.mark.db_test
    def test_get_link_no_workspace(
        self, create_task_instance_of_operator, dag_maker, session, mock_supervisor_comms
    ):
        link = AirbyteConnectionLink()

        ti = create_task_instance_of_operator(
            AirbyteTriggerSyncOperator,
            dag_id="test_airbyte_link_dag",
            task_id="test_airbyte_link_task",
            airbyte_conn_id=self.airbyte_conn_id,
            workspace_id=None,
            connection_id=TEST_CONNECTION_ID,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )

        session.add(ti)
        session.commit()

        task = dag_maker.dag.get_task(ti.task_id)

        link.persist(context={"ti": ti, "task": task})

        if mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "connection_id": task.connection_id,
                    "workspace_id": task.workspace_id,
                    "server_url": "https://example.com/api/public/v1",
                },
            )

        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)

        assert actual_url == EXPECTED_OSS_CONNECTION_LINK
