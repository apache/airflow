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

import unittest
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.sensors.dms import DmsTaskCompletedSensor


class TestDmsTaskCompletedSensor(unittest.TestCase):
    def setUp(self):
        self.sensor = DmsTaskCompletedSensor(
            task_id="test_dms_sensor",
            aws_conn_id="aws_default",
            replication_task_arn="task_arn",
        )

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("stopped",))
    def test_poke_stopped(self, mock_get_task_status):
        assert self.sensor.poke(None)

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("running",))
    def test_poke_running(self, mock_get_task_status):
        assert not self.sensor.poke(None)

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("starting",))
    def test_poke_starting(self, mock_get_task_status):
        assert not self.sensor.poke(None)

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("ready",))
    def test_poke_ready(self, mock_get_task_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert "Unexpected status: ready" in str(ctx.value)

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("creating",))
    def test_poke_creating(self, mock_get_task_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert "Unexpected status: creating" in str(ctx.value)

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("failed",))
    def test_poke_failed(self, mock_get_task_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert "Unexpected status: failed" in str(ctx.value)

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("deleting",))
    def test_poke_deleting(self, mock_get_task_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert "Unexpected status: deleting" in str(ctx.value)
