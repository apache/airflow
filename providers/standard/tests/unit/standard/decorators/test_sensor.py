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

from airflow.providers.common.compat.sdk import AirflowSensorTimeout
from airflow.sensors.base import PokeReturnValue
from airflow.utils.state import State

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]

pytestmark = pytest.mark.db_test


@pytest.mark.skipif(
    not AIRFLOW_V_3_0_PLUS,
    reason="Decorators were part of core not providers, so this test doesnt make sense for < AF3.",
)
class TestSensorDecorator:
    def test_sensor_fails_on_none_python_callable(self):
        not_callable = {}
        with pytest.raises(TypeError):
            task.sensor(not_callable)

    def test_basic_sensor_success(self, run_task):
        sensor_xcom_value = "xcom_value"

        @task.sensor
        def sensor_f():
            return PokeReturnValue(is_done=True, xcom_value=sensor_xcom_value)

        sf = sensor_f()
        run_task(sf.operator)

        assert run_task.state == State.SUCCESS
        assert run_task.xcom.get("return_value") == sensor_xcom_value

    def test_basic_sensor_success_returns_bool(self, run_task):
        @task.sensor
        def sensor_f():
            return True

        sf = sensor_f()
        run_task(sf.operator)

        assert run_task.state == State.SUCCESS

    def test_basic_sensor_failure(self, run_task):
        @task.sensor(timeout=0)
        def sensor_f():
            return PokeReturnValue(is_done=False, xcom_value="xcom_value")

        sf = sensor_f()
        run_task(sf.operator)

        assert run_task.state == State.FAILED
        assert isinstance(run_task.error, AirflowSensorTimeout)

    def test_basic_sensor_failure_returns_bool(self, run_task):
        @task.sensor(timeout=0)
        def sensor_f():
            return PokeReturnValue(is_done=False, xcom_value="xcom_value")

        sf = sensor_f()
        run_task(sf.operator)

        assert run_task.state == State.FAILED
        assert isinstance(run_task.error, AirflowSensorTimeout)

    def test_basic_sensor_soft_fail(self, run_task):
        @task.sensor(timeout=0, soft_fail=True)
        def sensor_f():
            return PokeReturnValue(is_done=False, xcom_value="xcom_value")

        sf = sensor_f()
        run_task(sf.operator)

        assert run_task.state == State.SKIPPED

    def test_basic_sensor_soft_fail_returns_bool(self, run_task):
        @task.sensor(timeout=0, soft_fail=True)
        def sensor_f():
            return False

        sf = sensor_f()
        run_task(sf.operator)

        assert run_task.state == State.SKIPPED

    def test_basic_sensor_get_upstream_output(self, dag_maker, run_task):
        ret_val = 100
        sensor_xcom_value = "xcom_value"

        @task
        def upstream_f() -> int:
            return ret_val

        @task.sensor
        def sensor_f(n: int):
            assert n == ret_val
            return PokeReturnValue(is_done=True, xcom_value=sensor_xcom_value)

        with dag_maker(serialized=True):
            uf = upstream_f()
            sf = sensor_f(uf)

        dr = dag_maker.create_dagrun()
        run_task(uf.operator, dag_id=dr.dag_id, run_id=dr.run_id)
        # Patch the return value for upstream_f
        assert run_task.xcom.get("return_value", task_id="upstream_f") == ret_val
        assert run_task.state == State.SUCCESS

        run_task(sf.operator, dag_id=dr.dag_id, run_id=dr.run_id)
        assert run_task.state == State.SUCCESS
        assert run_task.xcom.get("return_value", task_id="sensor_f") == sensor_xcom_value
