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
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor


class TestAthenaSensor:
    def setup_method(self):
        self.sensor = AthenaSensor(
            task_id="test_athena_sensor",
            query_execution_id="abc",
            sleep_time=5,
            max_retries=1,
            aws_conn_id="aws_default",
        )

    @pytest.mark.parametrize("poll_query_status", ["SUCCEEDED"])
    def test_poke_true_on_status(self, poll_query_status):
        with mock.patch.object(AthenaHook, "poll_query_status", side_effect=[poll_query_status]):
            assert self.sensor.poke({})

    @pytest.mark.parametrize("poll_query_status", ["RUNNING", "QUEUED"])
    def test_poke_false_on_status(self, poll_query_status):
        with mock.patch.object(AthenaHook, "poll_query_status", side_effect=[poll_query_status]):
            assert not self.sensor.poke({})

    @pytest.mark.parametrize("poll_query_status", ["FAILED", "CANCELLED"])
    def test_poke_raise_on_status(self, poll_query_status):
        with mock.patch.object(AthenaHook, "poll_query_status", side_effect=[poll_query_status]):
            with pytest.raises(AirflowException, match=r"Athena sensor failed"):
                self.sensor.poke({})
