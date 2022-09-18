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

import unittest
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook
from airflow.providers.amazon.aws.sensors.emr import EmrContainerSensor


class TestEmrContainerSensor(unittest.TestCase):
    def setUp(self):
        self.sensor = EmrContainerSensor(
            task_id='test_emrcontainer_sensor',
            virtual_cluster_id='vzwemreks',
            job_id='job1234',
            poll_interval=5,
            max_retries=1,
            aws_conn_id='aws_default',
        )

    @mock.patch.object(EmrContainerHook, 'check_query_status', side_effect=("PENDING",))
    def test_poke_pending(self, mock_check_query_status):
        assert not self.sensor.poke(None)

    @mock.patch.object(EmrContainerHook, 'check_query_status', side_effect=("SUBMITTED",))
    def test_poke_submitted(self, mock_check_query_status):
        assert not self.sensor.poke(None)

    @mock.patch.object(EmrContainerHook, 'check_query_status', side_effect=("RUNNING",))
    def test_poke_running(self, mock_check_query_status):
        assert not self.sensor.poke(None)

    @mock.patch.object(EmrContainerHook, 'check_query_status', side_effect=("COMPLETED",))
    def test_poke_completed(self, mock_check_query_status):
        assert self.sensor.poke(None)

    @mock.patch.object(EmrContainerHook, 'check_query_status', side_effect=("FAILED",))
    def test_poke_failed(self, mock_check_query_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert 'EMR Containers sensor failed' in str(ctx.value)

    @mock.patch.object(EmrContainerHook, 'check_query_status', side_effect=("CANCELLED",))
    def test_poke_cancelled(self, mock_check_query_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert 'EMR Containers sensor failed' in str(ctx.value)

    @mock.patch.object(EmrContainerHook, 'check_query_status', side_effect=("CANCEL_PENDING",))
    def test_poke_cancel_pending(self, mock_check_query_status):
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)
        assert 'EMR Containers sensor failed' in str(ctx.value)
