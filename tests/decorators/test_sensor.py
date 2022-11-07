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

import pytest

from airflow.decorators import task
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import XCom
from airflow.sensors.base import PokeReturnValue
from airflow.utils.state import State


class TestSensorDecorator:
    def test_sensor_fails_on_none_python_callable(self, dag_maker):
        not_callable = {}
        with pytest.raises(TypeError):
            task.sensor(not_callable)

    def test_basic_sensor_success(self, dag_maker):
        sensor_xcom_value = "xcom_value"

        @task.sensor
        def sensor_f():
            return PokeReturnValue(is_done=True, xcom_value=sensor_xcom_value)

        @task
        def dummy_f():
            pass

        with dag_maker():
            sf = sensor_f()
            df = dummy_f()
            sf >> df

        dr = dag_maker.create_dagrun()
        sf.operator.run(start_date=dr.execution_date, end_date=dr.execution_date, ignore_ti_state=True)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == "sensor_f":
                assert ti.state == State.SUCCESS
            if ti.task_id == "dummy_f":
                assert ti.state == State.NONE
        actual_xcom_value = XCom.get_one(
            key="return_value", task_id="sensor_f", dag_id=dr.dag_id, run_id=dr.run_id
        )
        assert actual_xcom_value == sensor_xcom_value

    def test_basic_sensor_failure(self, dag_maker):
        @task.sensor(timeout=0)
        def sensor_f():
            return PokeReturnValue(is_done=False, xcom_value="xcom_value")

        @task
        def dummy_f():
            pass

        with dag_maker():
            sf = sensor_f()
            df = dummy_f()
            sf >> df

        dr = dag_maker.create_dagrun()
        with pytest.raises(AirflowSensorTimeout):
            sf.operator.run(start_date=dr.execution_date, end_date=dr.execution_date, ignore_ti_state=True)

        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == "sensor_f":
                assert ti.state == State.FAILED
            if ti.task_id == "dummy_f":
                assert ti.state == State.NONE

    def test_basic_sensor_soft_fail(self, dag_maker):
        @task.sensor(timeout=0, soft_fail=True)
        def sensor_f():
            return PokeReturnValue(is_done=False, xcom_value="xcom_value")

        @task
        def dummy_f():
            pass

        with dag_maker():
            sf = sensor_f()
            df = dummy_f()
            sf >> df

        dr = dag_maker.create_dagrun()
        sf.operator.run(start_date=dr.execution_date, end_date=dr.execution_date, ignore_ti_state=True)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == "sensor_f":
                assert ti.state == State.SKIPPED
            if ti.task_id == "dummy_f":
                assert ti.state == State.NONE

    def test_basic_sensor_get_upstream_output(self, dag_maker):
        ret_val = 100
        sensor_xcom_value = "xcom_value"

        @task
        def upstream_f() -> int:
            return ret_val

        @task.sensor
        def sensor_f(n: int):
            assert n == ret_val
            return PokeReturnValue(is_done=True, xcom_value=sensor_xcom_value)

        with dag_maker():
            uf = upstream_f()
            sf = sensor_f(uf)

        dr = dag_maker.create_dagrun()
        uf.operator.run(start_date=dr.execution_date, end_date=dr.execution_date, ignore_ti_state=True)
        sf.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        tis = dr.get_task_instances()
        assert len(tis) == 2
        for ti in tis:
            if ti.task_id == "sensor_f":
                assert ti.state == State.SUCCESS
            if ti.task_id == "dummy_f":
                assert ti.state == State.SUCCESS
        actual_xcom_value = XCom.get_one(
            key="return_value", task_id="sensor_f", dag_id=dr.dag_id, run_id=dr.run_id
        )
        assert actual_xcom_value == sensor_xcom_value
