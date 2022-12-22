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
from tests.operators.test_python import BasePythonTest


class TestPythonSensor(BasePythonTest):
    opcls = PythonSensor

    def test_python_sensor_true(self):
        self.run_as_task(fn=lambda: True)

    def test_python_sensor_false(self):
        with pytest.raises(AirflowSensorTimeout):
            self.run_as_task(lambda: False, timeout=0.01, poke_interval=0.01)

    def test_python_sensor_raise(self):
        with pytest.raises(ZeroDivisionError):
            self.run_as_task(lambda: 1 / 0)

    def test_python_callable_arguments_are_templatized(self):
        """Test PythonSensor op_args are templatized"""
        # Create a named tuple and ensure it is still preserved
        # after the rendering is done
        Named = namedtuple("Named", ["var1", "var2"])
        named_tuple = Named("{{ ds }}", "unchanged")

        task = self.render_templates(
            lambda: 0,
            op_args=[4, date(2019, 1, 1), "dag {{dag.dag_id}} ran on {{ds}}.", named_tuple],
        )
        rendered_op_args = task.op_args
        assert len(rendered_op_args) == 4
        assert rendered_op_args[0] == 4
        assert rendered_op_args[1] == date(2019, 1, 1)
        assert rendered_op_args[2] == f"dag {self.dag_id} ran on {self.ds_templated}."
        assert rendered_op_args[3] == Named(self.ds_templated, "unchanged")

    def test_python_callable_keyword_arguments_are_templatized(self):
        """Test PythonSensor op_kwargs are templatized"""
        task = self.render_templates(
            lambda: 0,
            op_kwargs={
                "an_int": 4,
                "a_date": date(2019, 1, 1),
                "a_templated_string": "dag {{dag.dag_id}} ran on {{ds}}.",
            },
        )
        rendered_op_kwargs = task.op_kwargs
        assert rendered_op_kwargs["an_int"] == 4
        assert rendered_op_kwargs["a_date"] == date(2019, 1, 1)
        assert rendered_op_kwargs["a_templated_string"] == f"dag {self.dag_id} ran on {self.ds_templated}."
