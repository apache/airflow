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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.sensors.dms import DmsTaskCompletedSensor


class TestDmsTaskCompletedSensor:
    def setup_method(self):
        self.sensor = DmsTaskCompletedSensor(
            task_id="test_dms_sensor",
            aws_conn_id="aws_default",
            replication_task_arn="task_arn",
        )

    @pytest.mark.parametrize("task_status", ["stopped"])
    def test_poke_true_on_status(self, task_status):
        with mock.patch.object(DmsHook, "get_task_status", side_effect=[task_status]):
            assert self.sensor.poke({})

    @pytest.mark.parametrize("task_status", ["running", "starting"])
    def test_poke_false_on_status(self, task_status):
        with mock.patch.object(DmsHook, "get_task_status", side_effect=[task_status]):
            assert not self.sensor.poke({})

    @pytest.mark.parametrize("task_status", ["ready", "creating", "failed", "deleting"])
    def test_poke_raise_unexpected_status_on_status(self, task_status):
        with mock.patch.object(DmsHook, "get_task_status", side_effect=[task_status]):
            with pytest.raises(AirflowException, match=rf"Unexpected status: {task_status}"):
                self.sensor.poke({})
