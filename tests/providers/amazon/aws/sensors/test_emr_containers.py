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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook
from airflow.providers.amazon.aws.sensors.emr import EmrContainerSensor


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

    @pytest.mark.parametrize("query_status", ["COMPLETED"])
    def test_poke_true_on_query_status(self, query_status):
        with mock.patch.object(EmrContainerHook, "check_query_status", side_effect=[query_status]):
            assert self.sensor.poke({})

    @pytest.mark.parametrize("query_status", ["PENDING", "SUBMITTED", "RUNNING"])
    def test_poke_false_on_query_status(self, query_status):
        with mock.patch.object(EmrContainerHook, "check_query_status", side_effect=[query_status]):
            assert not self.sensor.poke({})

    @pytest.mark.parametrize("query_status", ["FAILED", "CANCELLED", "CANCEL_PENDING"])
    def test_poke_raise_on_query_status(self, query_status):
        with mock.patch.object(EmrContainerHook, "check_query_status", side_effect=[query_status]):
            with pytest.raises(AirflowException, match=r"EMR Containers sensor failed"):
                assert not self.sensor.poke({})
