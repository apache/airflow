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

from airflow.providers.alibaba.cloud.operators.maxcompute import MaxComputeSQLOperator

MAXCOMPUTE_OPERATOR_MODULE = "airflow.providers.alibaba.cloud.operators.maxcompute.{}"
MOCK_TASK_ID = "run_sql"
MOCK_SQL = "SELECT 1"
MOCK_INSTANCE_ID = "mock_instance_id"


class TestMaxComputeSQLOperator:
    @mock.patch(MAXCOMPUTE_OPERATOR_MODULE.format("MaxComputeLogViewLink"))
    @mock.patch(MAXCOMPUTE_OPERATOR_MODULE.format("MaxComputeHook"))
    def test_execute(self, mock_hook, mock_log_view_link):
        instance_mock = mock.MagicMock()
        instance_mock.id = MOCK_INSTANCE_ID
        instance_mock.get_logview_address.return_value = "http://mock_logview_address"

        mock_hook.return_value.run_sql.return_value = instance_mock

        op = MaxComputeSQLOperator(
            task_id=MOCK_TASK_ID,
            sql=MOCK_SQL,
        )

        instance_id = op.execute(context=mock.MagicMock())

        assert instance_id == instance_mock.id

        mock_hook.return_value.run_sql.assert_called_once_with(
            project=op.project,
            sql=op.sql,
            endpoint=op.endpoint,
            priority=op.priority,
            running_cluster=op.running_cluster,
            hints=op.hints,
            aliases=op.aliases,
            default_schema=op.default_schema,
            quota_name=op.quota_name,
        )

        mock_log_view_link.persist.assert_called_once_with(
            context=mock.ANY,
            log_view_url=instance_mock.get_logview_address.return_value,
        )

    @mock.patch(MAXCOMPUTE_OPERATOR_MODULE.format("MaxComputeHook"))
    def test_on_kill(self, mock_hook):
        instance_mock = mock.MagicMock()
        instance_mock.id = MOCK_INSTANCE_ID

        mock_hook.return_value.run_sql.return_value = instance_mock

        op = MaxComputeSQLOperator(
            task_id=MOCK_TASK_ID,
            sql=MOCK_SQL,
            cancel_on_kill=False,
        )
        op.execute(context=mock.MagicMock())

        op.on_kill()
        mock_hook.return_value.cancel_job.assert_not_called()

        op.cancel_on_kill = True
        op.on_kill()
        mock_hook.return_value.stop_instance.assert_called_once_with(
            instance_id=instance_mock.id, project=op.project, endpoint=op.endpoint
        )
