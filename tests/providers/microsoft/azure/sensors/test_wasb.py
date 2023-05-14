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

import datetime
from unittest import mock

import pendulum
import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import Connection
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.microsoft.azure.sensors.wasb import (
    WasbBlobSensor,
    WasbPrefixSensor,
)
from airflow.providers.microsoft.azure.triggers.wasb import WasbBlobSensorTrigger, WasbPrefixSensorTrigger
from airflow.utils import timezone
from airflow.utils.types import DagRunType

TEST_DATA_STORAGE_BLOB_NAME = "test_blob_providers.txt"
TEST_DATA_STORAGE_CONTAINER_NAME = "test-container-providers"
TEST_DATA_STORAGE_BLOB_PREFIX = TEST_DATA_STORAGE_BLOB_NAME[:10]


class TestWasbBlobSensor:
    _config = {
        "container_name": "container",
        "blob_name": "blob",
        "wasb_conn_id": "conn_id",
        "timeout": 100,
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    def test_init(self):
        sensor = WasbBlobSensor(task_id="wasb_sensor_1", dag=self.dag, **self._config)
        assert sensor.container_name == self._config["container_name"]
        assert sensor.blob_name == self._config["blob_name"]
        assert sensor.wasb_conn_id == self._config["wasb_conn_id"]
        assert sensor.check_options == {}
        assert sensor.timeout == self._config["timeout"]

        sensor = WasbBlobSensor(
            task_id="wasb_sensor_2", dag=self.dag, check_options={"timeout": 2}, **self._config
        )
        assert sensor.check_options == {"timeout": 2}

    @mock.patch("airflow.providers.microsoft.azure.sensors.wasb.WasbHook", autospec=True)
    def test_poke(self, mock_hook):
        mock_instance = mock_hook.return_value
        sensor = WasbBlobSensor(
            task_id="wasb_sensor", dag=self.dag, check_options={"timeout": 2}, **self._config
        )
        sensor.poke(None)
        mock_instance.check_for_blob.assert_called_once_with("container", "blob", timeout=2)


class TestWasbBlobAsyncSensor:
    def get_dag_run(self, dag_id: str = "test_dag_id", run_id: str = "test_dag_id") -> DagRun:
        dag_run = DagRun(
            dag_id=dag_id, run_type="manual", execution_date=timezone.datetime(2022, 1, 1), run_id=run_id
        )
        return dag_run

    def get_task_instance(self, task: BaseOperator) -> TaskInstance:
        return TaskInstance(task, timezone.datetime(2022, 1, 1))

    def get_conn(self) -> Connection:
        return Connection(
            conn_id="test_conn",
            extra={},
        )

    def create_context(self, task, dag=None):
        if dag is None:
            dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("UTC")
        execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        dag_run = DagRun(
            dag_id=dag.dag_id,
            execution_date=execution_date,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
        )

        task_instance = TaskInstance(task=task)
        task_instance.dag_run = dag_run
        task_instance.xcom_push = mock.Mock()
        return {
            "dag": dag,
            "ts": execution_date.isoformat(),
            "task": task,
            "ti": task_instance,
            "task_instance": task_instance,
            "run_id": dag_run.run_id,
            "dag_run": dag_run,
            "execution_date": execution_date,
            "data_interval_end": execution_date,
            "logical_date": execution_date,
        }

    SENSOR = WasbBlobSensor(
        task_id="wasb_blob_async_sensor",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
        timeout=5,
        deferrable=True,
    )

    def test_wasb_blob_sensor_async(self):
        """Assert execute method defer for wasb blob sensor"""

        with pytest.raises(TaskDeferred) as exc:
            self.SENSOR.execute(self.create_context(self.SENSOR))
        assert isinstance(exc.value.trigger, WasbBlobSensorTrigger), "Trigger is not a WasbBlobSensorTrigger"
        assert exc.value.timeout == datetime.timedelta(seconds=5)

    @pytest.mark.parametrize(
        "event",
        [None, {"status": "success", "message": "Job completed"}],
    )
    def test_wasb_blob_sensor_execute_complete_success(self, event):
        """Assert execute_complete log success message when trigger fire with target status."""

        if not event:
            with pytest.raises(AirflowException) as exception_info:
                self.SENSOR.execute_complete(context=None, event=None)
            assert exception_info.value.args[0] == "Did not receive valid event from the triggerer"
        else:
            with mock.patch.object(self.SENSOR.log, "info") as mock_log_info:
                self.SENSOR.execute_complete(context={}, event=event)
            mock_log_info.assert_called_with(event["message"])

    def test_wasb_blob_sensor_execute_complete_failure(self):
        """Assert execute_complete method raises an exception when the triggerer fires an error event."""

        with pytest.raises(AirflowException):
            self.SENSOR.execute_complete(context={}, event={"status": "error", "message": ""})


class TestWasbPrefixSensor:
    _config = {
        "container_name": "container",
        "prefix": "prefix",
        "wasb_conn_id": "conn_id",
        "timeout": 100,
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    def test_init(self):
        sensor = WasbPrefixSensor(task_id="wasb_sensor_1", dag=self.dag, **self._config)
        assert sensor.container_name == self._config["container_name"]
        assert sensor.prefix == self._config["prefix"]
        assert sensor.wasb_conn_id == self._config["wasb_conn_id"]
        assert sensor.check_options == {}
        assert sensor.timeout == self._config["timeout"]

        sensor = WasbPrefixSensor(
            task_id="wasb_sensor_2", dag=self.dag, check_options={"timeout": 2}, **self._config
        )
        assert sensor.check_options == {"timeout": 2}

    @mock.patch("airflow.providers.microsoft.azure.sensors.wasb.WasbHook", autospec=True)
    def test_poke(self, mock_hook):
        mock_instance = mock_hook.return_value
        sensor = WasbPrefixSensor(
            task_id="wasb_sensor", dag=self.dag, check_options={"timeout": 2}, **self._config
        )
        sensor.poke(None)
        mock_instance.check_for_prefix.assert_called_once_with("container", "prefix", timeout=2)


class TestWasbPrefixAsyncSensor:
    def get_dag_run(self, dag_id: str = "test_dag_id", run_id: str = "test_dag_id") -> DagRun:
        dag_run = DagRun(
            dag_id=dag_id, run_type="manual", execution_date=timezone.datetime(2022, 1, 1), run_id=run_id
        )
        return dag_run

    def get_task_instance(self, task: BaseOperator) -> TaskInstance:
        return TaskInstance(task, timezone.datetime(2022, 1, 1))

    def get_conn(self) -> Connection:
        return Connection(
            conn_id="test_conn",
            extra={},
        )

    def create_context(self, task, dag=None):
        if dag is None:
            dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("UTC")
        execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        dag_run = DagRun(
            dag_id=dag.dag_id,
            execution_date=execution_date,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
        )

        task_instance = TaskInstance(task=task)
        task_instance.dag_run = dag_run
        task_instance.xcom_push = mock.Mock()
        return {
            "dag": dag,
            "ts": execution_date.isoformat(),
            "task": task,
            "ti": task_instance,
            "task_instance": task_instance,
            "run_id": dag_run.run_id,
            "dag_run": dag_run,
            "execution_date": execution_date,
            "data_interval_end": execution_date,
            "logical_date": execution_date,
        }

    SENSOR = WasbPrefixSensor(
        task_id="wasb_prefix_sensor_async",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
        deferrable=True,
    )

    def test_wasb_prefix_sensor_async(self):
        """Assert execute method defer for wasb prefix sensor"""

        with pytest.raises(TaskDeferred) as exc:
            self.SENSOR.execute(self.create_context(self.SENSOR))
        assert isinstance(
            exc.value.trigger, WasbPrefixSensorTrigger
        ), "Trigger is not a WasbPrefixSensorTrigger"

    @pytest.mark.parametrize(
        "event",
        [None, {"status": "success", "message": "Job completed"}],
    )
    def test_wasb_prefix_sensor_execute_complete_success(self, event):
        """Assert execute_complete log success message when trigger fire with target status."""

        if not event:
            with pytest.raises(AirflowException) as exception_info:
                self.SENSOR.execute_complete(context=None, event=None)
            assert exception_info.value.args[0] == "Did not receive valid event from the triggerer"
        else:
            with mock.patch.object(self.SENSOR.log, "info") as mock_log_info:
                self.SENSOR.execute_complete(context={}, event=event)
            mock_log_info.assert_called_with(event["message"])

    def test_wasb_prefix_sensor_execute_complete_failure(self):
        """Assert execute_complete method raises an exception when the triggerer fires an error event."""

        with pytest.raises(AirflowException):
            self.SENSOR.execute_complete(context={}, event={"status": "error", "message": ""})
