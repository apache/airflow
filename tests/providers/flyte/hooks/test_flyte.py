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

import pytest
from flytekit.configuration import Config, PlatformConfig
from flytekit.exceptions.user import FlyteEntityNotExistException, FlyteValueException
from flytekit.models.core import execution as core_execution_models
from flytekit.remote import FlyteRemote

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.flyte.hooks.flyte import AirflowFlyteHook


class TestAirflowFlyteHook(unittest.TestCase):

    flyte_conn_id = "flyte_default"
    execution_name = "flyte20220330t133856"
    conn_type = "flyte"
    host = "localhost"
    port = "30081"
    extra = {"project": "flytesnacks", "domain": "development"}
    launchplan_name = "core.basic.hello_world.my_wf"
    task_name = "core.basic.hello_world.say_hello"
    raw_data_prefix = "s3://flyte-demo/raw_data"
    assumable_iam_role = "arn:aws:iam::123456789012:role/example-role"
    kubernetes_service_account = "default"
    version = "v1"
    inputs = {"name": "hello world"}
    timeout = timedelta(seconds=3600)

    @classmethod
    def get_mock_connection(cls):
        return Connection(
            conn_id=cls.flyte_conn_id, conn_type=cls.conn_type, host=cls.host, port=cls.port, extra=cls.extra
        )

    @classmethod
    def create_remote(cls):
        return FlyteRemote(
            config=Config(
                platform=PlatformConfig(endpoint=":".join([cls.host, cls.port]), insecure=True),
            )
        )

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    def test_trigger_execution_success(self, mock_create_flyte_remote, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote
        mock_remote.fetch_launch_plan = mock.MagicMock()

        mock_remote.execute = mock.MagicMock()

        test_hook.trigger_execution(
            launchplan_name=self.launchplan_name,
            raw_data_prefix=self.raw_data_prefix,
            assumable_iam_role=self.assumable_iam_role,
            kubernetes_service_account=self.kubernetes_service_account,
            version=self.version,
            inputs=self.inputs,
            execution_name=self.execution_name,
        )
        mock_create_flyte_remote.assert_called_once()

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    def test_trigger_task_execution_success(self, mock_create_flyte_remote, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote
        mock_remote.fetch_task = mock.MagicMock()

        mock_remote.execute = mock.MagicMock()

        test_hook.trigger_execution(
            task_name=self.task_name,
            raw_data_prefix=self.raw_data_prefix,
            assumable_iam_role=self.assumable_iam_role,
            kubernetes_service_account=self.kubernetes_service_account,
            version=self.version,
            inputs=self.inputs,
            execution_name=self.execution_name,
        )
        mock_create_flyte_remote.assert_called_once()

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    def test_trigger_execution_failed_to_fetch(self, mock_create_flyte_remote, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote
        mock_remote.fetch_launch_plan = mock.MagicMock(side_effect=FlyteEntityNotExistException)

        with pytest.raises(AirflowException):
            test_hook.trigger_execution(
                launchplan_name=self.launchplan_name,
                raw_data_prefix=self.raw_data_prefix,
                assumable_iam_role=self.assumable_iam_role,
                kubernetes_service_account=self.kubernetes_service_account,
                version=self.version,
                inputs=self.inputs,
                execution_name=self.execution_name,
            )
        mock_create_flyte_remote.assert_called_once()

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    def test_trigger_execution_failed_to_trigger(self, mock_create_flyte_remote, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote
        mock_remote.fetch_launch_plan = mock.MagicMock()
        mock_remote.execute = mock.MagicMock(side_effect=FlyteValueException)

        with pytest.raises(AirflowException):
            test_hook.trigger_execution(
                launchplan_name=self.launchplan_name,
                raw_data_prefix=self.raw_data_prefix,
                assumable_iam_role=self.assumable_iam_role,
                kubernetes_service_account=self.kubernetes_service_account,
                version=self.version,
                inputs=self.inputs,
                execution_name=self.execution_name,
            )
        mock_create_flyte_remote.assert_called_once()

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.execution_id")
    def test_wait_for_execution_succeeded(
        self, mock_execution_id, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote

        execution_id = mock.MagicMock()
        mock_execution_id.return_value = execution_id

        mock_get_execution = mock.MagicMock()
        mock_remote.client.get_execution = mock_get_execution
        mock_phase = mock.PropertyMock(side_effect=[test_hook.SUCCEEDED])
        type(mock_get_execution().closure).phase = mock_phase

        test_hook.wait_for_execution(
            execution_name=self.execution_name, timeout=self.timeout, poll_interval=timedelta(seconds=3)
        )
        mock_create_flyte_remote.assert_called_once()
        mock_execution_id.assert_called_once_with(self.execution_name)
        assert mock_phase.call_count == 1

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.execution_id")
    def test_wait_for_execution_failed(
        self, mock_execution_id, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote

        execution_id = mock.MagicMock()
        mock_execution_id.return_value = execution_id

        mock_get_execution = mock.MagicMock()
        mock_remote.client.get_execution = mock_get_execution
        mock_phase = mock.PropertyMock(
            side_effect=[core_execution_models.WorkflowExecutionPhase.RUNNING, test_hook.FAILED]
        )
        type(mock_get_execution().closure).phase = mock_phase

        with pytest.raises(AirflowException):
            test_hook.wait_for_execution(
                execution_name=self.execution_name, timeout=self.timeout, poll_interval=timedelta(seconds=3)
            )

        mock_create_flyte_remote.assert_called_once()
        mock_execution_id.assert_has_calls([mock.call(self.execution_name)] * 2)
        assert mock_phase.call_count == 2

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.execution_id")
    def test_wait_for_execution_queued_succeeded(
        self, mock_execution_id, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote

        execution_id = mock.MagicMock()
        mock_execution_id.return_value = execution_id

        mock_get_execution = mock.MagicMock()
        mock_remote.client.get_execution = mock_get_execution
        mock_phase = mock.PropertyMock(
            side_effect=[core_execution_models.WorkflowExecutionPhase.QUEUED, test_hook.SUCCEEDED]
        )
        type(mock_get_execution().closure).phase = mock_phase

        test_hook.wait_for_execution(
            execution_name=self.execution_name, timeout=self.timeout, poll_interval=timedelta(seconds=3)
        )

        mock_create_flyte_remote.assert_called_once()
        mock_execution_id.assert_has_calls([mock.call(self.execution_name)] * 2)
        assert mock_phase.call_count == 2

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.execution_id")
    def test_wait_for_execution_timedout(
        self, mock_execution_id, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote

        execution_id = mock.MagicMock()
        mock_execution_id.return_value = execution_id

        mock_get_execution = mock.MagicMock()
        mock_remote.client.get_execution = mock_get_execution
        mock_phase = mock.PropertyMock(
            side_effect=[
                core_execution_models.WorkflowExecutionPhase.QUEUED,
                core_execution_models.WorkflowExecutionPhase.RUNNING,
                test_hook.TIMED_OUT,
            ]
        )
        type(mock_get_execution().closure).phase = mock_phase

        with pytest.raises(AirflowException):
            test_hook.wait_for_execution(
                execution_name=self.execution_name, timeout=self.timeout, poll_interval=timedelta(seconds=3)
            )

        mock_create_flyte_remote.assert_called_once()
        mock_execution_id.assert_has_calls([mock.call(self.execution_name)] * 3)
        assert mock_phase.call_count == 3

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.execution_id")
    def test_wait_for_execution_timeout(
        self, mock_execution_id, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote

        execution_id = mock.MagicMock()
        mock_execution_id.return_value = execution_id

        mock_get_execution = mock.MagicMock()
        mock_remote.client.get_execution = mock_get_execution
        mock_phase = mock.PropertyMock(
            side_effect=[
                core_execution_models.WorkflowExecutionPhase.QUEUED,
                core_execution_models.WorkflowExecutionPhase.RUNNING,
                core_execution_models.WorkflowExecutionPhase.RUNNING,
            ]
        )
        type(mock_get_execution().closure).phase = mock_phase

        with pytest.raises(AirflowException):
            test_hook.wait_for_execution(
                execution_name=self.execution_name,
                timeout=timedelta(seconds=1),
                poll_interval=timedelta(seconds=3),
            )

        mock_create_flyte_remote.assert_called_once()
        mock_execution_id.assert_called()

    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.get_connection")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.create_flyte_remote")
    @mock.patch("airflow.providers.flyte.hooks.flyte.AirflowFlyteHook.execution_id")
    def test_wait_for_execution_aborted(
        self, mock_execution_id, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = AirflowFlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote

        execution_id = mock.MagicMock()
        mock_execution_id.return_value = execution_id

        mock_get_execution = mock.MagicMock()
        mock_remote.client.get_execution = mock_get_execution
        mock_phase = mock.PropertyMock(
            side_effect=[
                core_execution_models.WorkflowExecutionPhase.RUNNING,
                core_execution_models.WorkflowExecutionPhase.ABORTED,
            ]
        )
        type(mock_get_execution().closure).phase = mock_phase

        with pytest.raises(AirflowException):
            test_hook.wait_for_execution(
                execution_name=self.execution_name,
                timeout=self.timeout,
                poll_interval=timedelta(seconds=3),
            )

        mock_create_flyte_remote.assert_called_once()
        mock_execution_id.assert_has_calls([mock.call(self.execution_name)] * 2)
        assert mock_phase.call_count == 2
