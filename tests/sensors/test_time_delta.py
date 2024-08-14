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

from datetime import timedelta
from unittest import mock

import pendulum
import pytest

from airflow.models import DagBag
from airflow.models.dag import DAG
from airflow.sensors.time_delta import TimeDeltaSensor, TimeDeltaSensorAsync
from airflow.utils.timezone import datetime

pytestmark = pytest.mark.db_test


DEFAULT_DATE = datetime(2015, 1, 1)
DEV_NULL = "/dev/null"
TEST_DAG_ID = "unit_tests"


class TestTimedeltaSensor:
    def setup_method(self):
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        self.args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, schedule=timedelta(days=1), default_args=self.args)

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_timedelta_sensor(self):
        op = TimeDeltaSensor(task_id="timedelta_sensor_check", delta=timedelta(seconds=2), dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)


class TestTimeDeltaSensorAsync:
    def setup_method(self):
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        self.args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, schedule=timedelta(days=1), default_args=self.args)

    @pytest.mark.parametrize(
        "should_defer",
        [False, True],
    )
    @mock.patch("airflow.models.baseoperator.BaseOperator.defer")
    def test_timedelta_sensor(self, defer_mock, should_defer):
        delta = timedelta(hours=1)
        op = TimeDeltaSensorAsync(task_id="timedelta_sensor_check", delta=delta, dag=self.dag)
        if should_defer:
            data_interval_end = pendulum.now("UTC").add(hours=1)
        else:
            data_interval_end = pendulum.now("UTC").replace(microsecond=0, second=0, minute=0).add(hours=-1)
        op.execute({"data_interval_end": data_interval_end})
        if should_defer:
            defer_mock.assert_called_once()
        else:
            defer_mock.assert_not_called()
