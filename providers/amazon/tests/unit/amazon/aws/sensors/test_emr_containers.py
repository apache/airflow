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

from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook
from airflow.providers.amazon.aws.sensors.emr import EmrContainerSensor
from airflow.providers.amazon.aws.triggers.emr import EmrContainerTrigger
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred


class TestEmrContainerSensor:
    def setup_method(self):
        self.sensor = EmrContainerSensor(
            task_id="test_emrcontainer_sensor",
            virtual_cluster_id="vzwemreks",
            job_id="job1234",
            poll_interval=5,
            max_retries=1,
            aws_conn_id="aws_default",
        )
        # We're mocking all actual AWS calls and don't need a connection. This
        # avoids an Airflow warning about connection cannot be found.
        self.sensor.hook.get_connection = lambda _: None

    @mock.patch.object(EmrContainerHook, "check_query_status", side_effect=("PENDING",))
    def test_poke_pending(self, mock_check_query_status):
        assert not self.sensor.poke(None)

    @mock.patch.object(EmrContainerHook, "check_query_status", side_effect=("SUBMITTED",))
    def test_poke_submitted(self, mock_check_query_status):
        assert not self.sensor.poke(None)

    @mock.patch.object(EmrContainerHook, "check_query_status", side_effect=("RUNNING",))
    def test_poke_running(self, mock_check_query_status):
        assert not self.sensor.poke(None)

    @mock.patch.object(EmrContainerHook, "check_query_status", side_effect=("COMPLETED",))
    def test_poke_completed(self, mock_check_query_status):
        assert self.sensor.poke(None)

    @mock.patch.object(EmrContainerHook, "check_query_status", side_effect=("FAILED",))
    def test_poke_failed(self, mock_check_query_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert "EMR Containers sensor failed" in str(ctx.value)
        assert "FAILED" in str(ctx.value)

    @mock.patch.object(EmrContainerHook, "check_query_status", side_effect=("CANCELLED",))
    def test_poke_cancelled(self, mock_check_query_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert "EMR Containers sensor failed" in str(ctx.value)
        assert "CANCELLED" in str(ctx.value)

    @mock.patch.object(EmrContainerHook, "check_query_status", side_effect=("CANCEL_PENDING",))
    def test_poke_cancel_pending(self, mock_check_query_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert "EMR Containers sensor failed" in str(ctx.value)
        assert "CANCEL_PENDING" in str(ctx.value)

    @mock.patch("airflow.providers.amazon.aws.sensors.emr.EmrContainerSensor.poke")
    def test_sensor_defer(self, mock_poke):
        self.sensor.deferrable = True
        mock_poke.return_value = False
        with pytest.raises(TaskDeferred) as e:
            self.sensor.execute(context=None)
        assert isinstance(e.value.trigger, EmrContainerTrigger), (
            f"{e.value.trigger} is not a EmrContainerTrigger"
        )

    @mock.patch("airflow.providers.amazon.aws.sensors.emr.EmrContainerSensor.poke")
    def test_sensor_defer_with_timeout(self, mock_poke):
        self.sensor.deferrable = True
        mock_poke.return_value = False
        self.sensor.max_retries = 1000

        with pytest.raises(TaskDeferred) as e:
            self.sensor.execute(context=None)

        trigger = e.value.trigger
        assert isinstance(trigger, EmrContainerTrigger), f"{trigger} is not a EmrContainerTrigger"
        assert trigger.waiter_delay == self.sensor.poll_interval
        assert trigger.attempts == self.sensor.max_retries
