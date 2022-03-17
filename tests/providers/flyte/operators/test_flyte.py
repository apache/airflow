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

import unittest
from datetime import timedelta
from unittest import mock

from airflow.models import Connection
from airflow.providers.flyte.operators.flyte import AirflowFlyteOperator


class TestAirflowFlyteOperator(unittest.TestCase):

    task_id = "test_flyte_operator"
    flyte_conn_id = "flyte_default"
    conn_type = "flyte"
    host = "localhost"
    port = "30081"
    project = "flytesnacks"
    domain = "development"
    launchplan_name = "core.basic.hello_world.my_wf"
    raw_data_prefix = "s3://flyte-demo/raw_data"
    assumable_iam_role = "arn:aws:iam::123456789012:role/example-role"
    kubernetes_service_account = "default"
    version = "v1"
    inputs = {"name": "hello world"}
    timeout = timedelta(seconds=3600)
    execution_name = "f6e973ed8ca08481292a"

    @classmethod
    def get_connection(cls):
        return Connection(
            conn_id=cls.flyte_conn_id,
            conn_type=cls.conn_type,
            host=cls.host,
            port=cls.port,
            extra={"project": cls.project, "domain": cls.domain},
        )

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.trigger_execution")
    @mock.patch(
        "airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.wait_for_execution",
        return_value=None,
    )
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    def test_execute(self, mock_get_connection, mock_wait_for_execution, mock_trigger_execution):
        mock_get_connection.return_value = self.get_connection()
        mock_trigger_execution.return_value = self.execution_name

        operator = AirflowFlyteOperator(
            task_id=self.task_id,
            flyte_conn_id=self.flyte_conn_id,
            project=self.project,
            domain=self.domain,
            launchplan_name=self.launchplan_name,
            raw_data_prefix=self.raw_data_prefix,
            assumable_iam_role=self.assumable_iam_role,
            kubernetes_service_account=self.kubernetes_service_account,
            version=self.version,
            inputs=self.inputs,
            timeout=self.timeout,
        )
        result = operator.execute({})

        mock_get_connection.assert_called_once_with(self.flyte_conn_id)
        mock_trigger_execution.assert_called_once_with(
            launchplan_name=self.launchplan_name,
            task_name=None,
            max_parallelism=None,
            raw_data_prefix=self.raw_data_prefix,
            assumable_iam_role=self.assumable_iam_role,
            kubernetes_service_account=self.kubernetes_service_account,
            version=self.version,
            inputs=self.inputs,
        )
        mock_wait_for_execution.assert_called_once_with(
            execution_name=result, timeout=self.timeout, poll_interval=timedelta(seconds=30)
        )

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.trigger_execution")
    @mock.patch(
        "airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.wait_for_execution",
        return_value=None,
    )
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.terminate")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    def test_on_kill_success(
        self, mock_get_connection, mock_terminate, mock_wait_for_execution, mock_trigger_execution
    ):
        mock_trigger_execution.return_value = self.execution_name
        mock_get_connection.return_value = self.get_connection()

        operator = AirflowFlyteOperator(
            task_id=self.task_id,
            flyte_conn_id=self.flyte_conn_id,
            project=self.project,
            domain=self.domain,
            launchplan_name=self.launchplan_name,
            inputs=self.inputs,
            timeout=self.timeout,
        )
        result = operator.execute({})
        operator.on_kill()

        mock_get_connection.has_calls([mock.call(self.flyte_conn_id)] * 2)
        mock_wait_for_execution.assert_called_once_with(
            execution_name=result, timeout=self.timeout, poll_interval=timedelta(seconds=30)
        )
        mock_terminate.assert_called_once_with(execution_name=result, cause="Killed by Airflow")

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.terminate")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    def test_on_kill_noop(self, mock_get_connection, mock_terminate):
        mock_get_connection.return_value = self.get_connection()

        operator = AirflowFlyteOperator(
            task_id=self.task_id,
            flyte_conn_id=self.flyte_conn_id,
            project=self.project,
            domain=self.domain,
            launchplan_name=self.launchplan_name,
            inputs=self.inputs,
        )
        operator.on_kill()

        assert not mock_get_connection.called
        assert not mock_terminate.called
