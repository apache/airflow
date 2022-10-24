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
from unittest.mock import patch

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.apache.livy.hooks.livy import BatchState, LivyHook
from airflow.providers.apache.livy.sensors.livy import LivySensor
from airflow.utils import db, timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestLivySensor(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", default_args=args)
        db.merge_conn(Connection(conn_id="livyunittest", conn_type="livy", host="http://localhost:8998"))

    @patch("airflow.providers.apache.livy.hooks.livy.LivyHook.get_batch_state")
    def test_poke(self, mock_state):
        sensor = LivySensor(
            livy_conn_id="livyunittest", task_id="livy_sensor_test", dag=self.dag, batch_id=100
        )

        for state in BatchState:
            with self.subTest(state.value):
                mock_state.return_value = state
                assert sensor.poke({}) == (state in LivyHook.TERMINAL_STATES)
