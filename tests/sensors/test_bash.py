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
import unittest

import pytest

from airflow.exceptions import AirflowSensorTimeout
from airflow.models.dag import DAG
from airflow.sensors.bash import BashSensor


class TestBashSensor(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': datetime.datetime(2017, 1, 1)}
        dag = DAG('test_dag_id', default_args=args)
        self.dag = dag

    def test_true_condition(self):
        op = BashSensor(
            task_id='test_true_condition',
            bash_command='freturn() { return "$1"; }; freturn 0',
            output_encoding='utf-8',
            poke_interval=1,
            timeout=2,
            dag=self.dag,
        )
        op.execute(None)

    def test_false_condition(self):
        op = BashSensor(
            task_id='test_false_condition',
            bash_command='freturn() { return "$1"; }; freturn 1',
            output_encoding='utf-8',
            poke_interval=1,
            timeout=2,
            dag=self.dag,
        )
        with pytest.raises(AirflowSensorTimeout):
            op.execute(None)
