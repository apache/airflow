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
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
TEST_DAG_ID = 'unit_test_dag'


@pytest.fixture
def setup_dag():
    args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
    yield DAG(TEST_DAG_ID, default_args=args)


class TestHttpSensor:
    def test_poke_exception(self, httpx_mock):
        """
        Exception occurs in poke function should not be ignored.
        """

        def resp_check(_):
            raise AirflowException('AirflowException raised here!')

        httpx_mock.add_response(status_code=200)
        task = HttpSensor(
            task_id='http_sensor_poke_exception',
            http_conn_id='http_default',
            endpoint='',
            request_params={},
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
        )
        with pytest.raises(AirflowException, match='AirflowException raised here!'):
            task.execute(context={})

    def test_poke_continues_for_http_500_with_extra_options_check_response_false(self, httpx_mock, setup_dag):
        def resp_check(_):
            return False

        httpx_mock.add_response(status_code=500, data="Internal Server Error")

        task = HttpSensor(
            dag=setup_dag,
            task_id='http_sensor_poke_for_code_500',
            http_conn_id='http_default',
            endpoint='',
            request_params={},
            method='HEAD',
            response_check=resp_check,
            extra_options={'check_response': False},
            timeout=5,
            poke_interval=1,
        )

        with pytest.raises(AirflowSensorTimeout, match='Snap. Time is OUT. DAG id: unit_test_dag'):
            task.execute(context={})

    def test_head_method(self, httpx_mock, setup_dag):
        def resp_check(_):
            return True

        httpx_mock.add_response(status_code=200, url='https://www.httpbin.org', method='HEAD')
        task = HttpSensor(
            dag=setup_dag,
            task_id='http_sensor_head_method',
            http_conn_id='http_default',
            endpoint='',
            request_params={},
            method='HEAD',
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
        )

        task.execute(context={})

    def test_poke_context(self, httpx_mock, setup_dag):
        httpx_mock.add_response(status_code=200, url='https://www.httpbin.org')

        def resp_check(_, execution_date):
            if execution_date == DEFAULT_DATE:
                return True
            raise AirflowException('AirflowException raised here!')

        task = HttpSensor(
            task_id='http_sensor_poke_exception',
            http_conn_id='http_default',
            endpoint='',
            request_params={},
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
            dag=setup_dag,
        )

        task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        task.execute(task_instance.get_template_context())

    def test_logging_head_error_request(self, httpx_mock, setup_dag):
        def resp_check(_):
            return True

        httpx_mock.add_response(status_code=404, data="This endpoint doesn't exist")
        task = HttpSensor(
            dag=setup_dag,
            task_id='http_sensor_head_method',
            http_conn_id='http_default',
            endpoint='',
            request_params={},
            method='HEAD',
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
        )

        with mock.patch.object(task.hook.log, 'error') as mock_errors:
            with pytest.raises(AirflowSensorTimeout):
                task.execute(context={})

            assert mock_errors.called
            calls = [
                mock.call('HTTP error: %s', 'Not Found'),
                mock.call("This endpoint doesn't exist"),
                mock.call('HTTP error: %s', 'Not Found'),
                mock.call("This endpoint doesn't exist"),
                mock.call('HTTP error: %s', 'Not Found'),
                mock.call("This endpoint doesn't exist"),
                mock.call('HTTP error: %s', 'Not Found'),
                mock.call("This endpoint doesn't exist"),
                mock.call('HTTP error: %s', 'Not Found'),
                mock.call("This endpoint doesn't exist"),
                mock.call('HTTP error: %s', 'Not Found'),
                mock.call("This endpoint doesn't exist"),
            ]
            mock_errors.assert_has_calls(calls)


@pytest.fixture
def setup_op_dag():
    args = {'owner': 'airflow', 'start_date': DEFAULT_DATE_ISO}
    yield DAG(TEST_DAG_ID, default_args=args)


class TestHttpOpSensor:
    def test_get(self, httpx_mock, setup_op_dag):
        httpx_mock.add_response(status_code=200)
        op = SimpleHttpOperator(
            task_id='get_op',
            method='GET',
            endpoint='/search',
            data={"client": "ubuntu", "q": "airflow"},
            headers={},
            dag=setup_op_dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_get_response_check(self, httpx_mock, setup_op_dag):
        httpx_mock.add_response(status_code=200, data="apache/airflow")
        op = SimpleHttpOperator(
            task_id='get_op',
            method='GET',
            endpoint='/search',
            response_check=lambda response: ("apache/airflow" in response.text),
            headers={},
            dag=setup_op_dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_sensor(self, httpx_mock, setup_op_dag):
        httpx_mock.add_response(
            status_code=200,
            url="https://www.httpbin.org//search?client=ubuntu&q=airflow&date="
            + DEFAULT_DATE.strftime('%Y-%m-%d'),
        )
        sensor = HttpSensor(
            task_id='http_sensor_check',
            http_conn_id='http_default',
            endpoint='/search',
            request_params={"client": "ubuntu", "q": "airflow", 'date': '{{ds}}'},
            headers={},
            poke_interval=5,
            timeout=15,
            dag=setup_op_dag,
        )
        sensor.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
