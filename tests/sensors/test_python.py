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

from collections import namedtuple
from datetime import date

import pytest

from airflow.exceptions import AirflowSensorTimeout
from airflow.sensors.python import PythonSensor
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.operators.test_python import Call, assert_calls_equal, build_recording_function

DEFAULT_DATE = datetime(2015, 1, 1)


class TestPythonSensor:
    def test_python_sensor_true(self, dag_maker):
        with dag_maker():
            op = PythonSensor(task_id='python_sensor_check_true', python_callable=lambda: True)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_python_sensor_false(self, dag_maker):
        with dag_maker():
            op = PythonSensor(
                task_id='python_sensor_check_false',
                timeout=0.01,
                poke_interval=0.01,
                python_callable=lambda: False,
            )
        with pytest.raises(AirflowSensorTimeout):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_python_sensor_raise(self, dag_maker):
        with dag_maker():
            op = PythonSensor(task_id='python_sensor_check_raise', python_callable=lambda: 1 / 0)
        with pytest.raises(ZeroDivisionError):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_python_callable_arguments_are_templatized(self, dag_maker):
        """Test PythonSensor op_args are templatized"""
        recorded_calls = []

        # Create a named tuple and ensure it is still preserved
        # after the rendering is done
        Named = namedtuple('Named', ['var1', 'var2'])
        named_tuple = Named('{{ ds }}', 'unchanged')

        with dag_maker() as dag:
            task = PythonSensor(
                task_id='python_sensor',
                timeout=0.01,
                poke_interval=0.3,
                # a Mock instance cannot be used as a callable function or test fails with a
                # TypeError: Object of type Mock is not JSON serializable
                python_callable=build_recording_function(recorded_calls),
                op_args=[4, date(2019, 1, 1), "dag {{dag.dag_id}} ran on {{ds}}.", named_tuple],
            )

        dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        with pytest.raises(AirflowSensorTimeout):
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ds_templated = DEFAULT_DATE.date().isoformat()
        assert_calls_equal(
            recorded_calls[0],
            Call(
                4,
                date(2019, 1, 1),
                f"dag {dag.dag_id} ran on {ds_templated}.",
                Named(ds_templated, 'unchanged'),
            ),
        )

    def test_python_callable_keyword_arguments_are_templatized(self, dag_maker):
        """Test PythonSensor op_kwargs are templatized"""
        recorded_calls = []

        with dag_maker() as dag:
            task = PythonSensor(
                task_id='python_sensor',
                timeout=0.01,
                poke_interval=0.01,
                # a Mock instance cannot be used as a callable function or test fails with a
                # TypeError: Object of type Mock is not JSON serializable
                python_callable=build_recording_function(recorded_calls),
                op_kwargs={
                    'an_int': 4,
                    'a_date': date(2019, 1, 1),
                    'a_templated_string': "dag {{dag.dag_id}} ran on {{ds}}.",
                },
            )

        dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        with pytest.raises(AirflowSensorTimeout):
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        assert_calls_equal(
            recorded_calls[0],
            Call(
                an_int=4,
                a_date=date(2019, 1, 1),
                a_templated_string=f"dag {dag.dag_id} ran on {DEFAULT_DATE.date().isoformat()}.",
            ),
        )
