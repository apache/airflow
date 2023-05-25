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

from airflow.providers.google.cloud.hooks.dataprep import JobGroupStatuses
from airflow.providers.google.cloud.sensors.dataprep import DataprepJobGroupIsFinishedSensor

JOB_GROUP_ID = 1312


class TestDataprepJobGroupIsFinishedSensor:
    @mock.patch("airflow.providers.google.cloud.sensors.dataprep.GoogleDataprepHook")
    def test_passing_arguments_to_hook(self, hook_mock):
        sensor = DataprepJobGroupIsFinishedSensor(
            task_id="check_job_group_finished",
            job_group_id=JOB_GROUP_ID,
        )

        hook_mock.return_value.get_job_group_status.return_value = JobGroupStatuses.COMPLETE
        is_job_group_finished = sensor.poke(context=mock.MagicMock())

        assert is_job_group_finished

        hook_mock.assert_called_once_with(
            dataprep_conn_id="dataprep_default",
        )
        hook_mock.return_value.get_job_group_status.assert_called_once_with(
            job_group_id=JOB_GROUP_ID,
        )
