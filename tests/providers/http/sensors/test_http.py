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
from unittest.mock import patch

import pytest
import requests

from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException
from airflow.models.dag import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.timezone import datetime

pytestmark = pytest.mark.db_test


DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
TEST_DAG_ID = "unit_test_dag"


class TestHttpSensor:
    @patch("airflow.providers.http.hooks.http.requests.Session.send")
    def test_poke_exception(self, mock_session_send, create_task_of_operator):
        """
        Exception occurs in poke function should not be ignored.
        """
        response = requests.Response()
        response.status_code = 200
        mock_session_send.return_value = response

        def resp_check(_):
            raise AirflowException("AirflowException raised here!")

        task = create_task_of_operator(
            HttpSensor,
            dag_id="http_sensor_poke_exception",
            task_id="http_sensor_poke_exception",
            http_conn_id="http_default",
            endpoint="",
            request_params={},
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
        )
        with pytest.raises(AirflowException, match="AirflowException raised here!"):
            task.execute(context={})

    @patch("airflow.providers.http.hooks.http.requests.Session.send")
    def test_poke_exception_with_soft_fail(self, mock_session_send, create_task_of_operator):
        """
        Exception occurs in poke function should be skipped if soft_fail is True.
        """
        response = requests.Response()
        response.status_code = 200
        mock_session_send.return_value = response

        def resp_check(_):
            raise AirflowException("AirflowException raised here!")

        task = create_task_of_operator(
            HttpSensor,
            dag_id="http_sensor_poke_exception",
            task_id="http_sensor_poke_exception",
            http_conn_id="http_default",
            endpoint="",
            request_params={},
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
            soft_fail=True,
        )
        with pytest.raises(AirflowSkipException):
            task.execute(context={})

    @patch("airflow.providers.http.hooks.http.requests.Session.send")
    def test_poke_continues_for_http_500_with_extra_options_check_response_false(
        self,
        mock_session_send,
        create_task_of_operator,
    ):
        def resp_check(_):
            return False

        response = requests.Response()
        response.status_code = 500
        response.reason = "Internal Server Error"
        response._content = b"Internal Server Error"
        mock_session_send.return_value = response

        task = create_task_of_operator(
            HttpSensor,
            dag_id="http_sensor_poke_for_code_500",
            task_id="http_sensor_poke_for_code_500",
            http_conn_id="http_default",
            endpoint="",
            request_params={},
            method="HEAD",
            response_check=resp_check,
            extra_options={"check_response": False},
            timeout=5,
            poke_interval=1,
        )

        with pytest.raises(AirflowSensorTimeout):
            task.execute(context={})

    @patch("airflow.providers.http.hooks.http.requests.Session.send")
    def test_head_method(self, mock_session_send, create_task_of_operator):
        def resp_check(_):
            return True

        task = create_task_of_operator(
            HttpSensor,
            dag_id="http_sensor_head_method",
            task_id="http_sensor_head_method",
            http_conn_id="http_default",
            endpoint="",
            request_params={},
            method="HEAD",
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
        )

        task.execute(context={})

        received_request = mock_session_send.call_args.args[0]

        prep_request = requests.Request("HEAD", "https://www.httpbin.org", {}).prepare()

        assert prep_request.url == received_request.url
        assert prep_request.method, received_request.method

    @patch("airflow.providers.http.hooks.http.requests.Session.send")
    def test_poke_context(self, mock_session_send, create_task_instance_of_operator):
        response = requests.Response()
        response.status_code = 200
        mock_session_send.return_value = response

        def resp_check(_, logical_date):
            if logical_date == DEFAULT_DATE:
                return True
            raise AirflowException("AirflowException raised here!")

        task_instance = create_task_instance_of_operator(
            HttpSensor,
            dag_id="http_sensor_poke_exception",
            execution_date=DEFAULT_DATE,
            task_id="http_sensor_poke_exception",
            http_conn_id="http_default",
            endpoint="",
            request_params={},
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
        )

        task_instance.task.execute(task_instance.get_template_context())

    @patch("airflow.providers.http.hooks.http.requests.Session.send")
    def test_logging_head_error_request(self, mock_session_send, create_task_of_operator):
        def resp_check(_):
            return True

        response = requests.Response()
        response.status_code = 404
        response.reason = "Not Found"
        response._content = b"This endpoint doesn't exist"
        mock_session_send.return_value = response

        task = create_task_of_operator(
            HttpSensor,
            dag_id="http_sensor_head_error",
            task_id="http_sensor_head_error",
            http_conn_id="http_default",
            endpoint="",
            request_params={},
            method="HEAD",
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
        )

        with mock.patch("airflow.providers.http.hooks.http.HttpHook.log") as mock_log:
            with pytest.raises(AirflowSensorTimeout):
                task.execute(None)

            assert mock_log.error.called
            calls = [
                mock.call("HTTP error: %s", "Not Found"),
                mock.call("This endpoint doesn't exist"),
                mock.call("HTTP error: %s", "Not Found"),
                mock.call("This endpoint doesn't exist"),
            ]
            mock_log.error.assert_has_calls(calls)

    @patch("airflow.providers.http.hooks.http.requests.Session.send")
    def test_response_error_codes_allowlist(self, mock_session_send, create_task_of_operator):
        allowed_error_response_gen = iter(
            [
                (503, "Service Unavailable"),
                (503, "Service Unavailable"),
                (503, "Service Unavailable"),
                (404, "Not Found"),
                (499, "Allowed Non-standard Error Code"),
            ]
        )

        def mocking_allowed_error_responses(*_, **__):
            try:
                error_code, error_reason = next(allowed_error_response_gen)
            except StopIteration:
                return mock.DEFAULT

            error_response = requests.Response()
            error_response.status_code = error_code
            error_response.reason = error_reason

            return error_response

        def resp_check(_):
            return True

        final_response = requests.Response()
        final_response.status_code = 500
        final_response.reason = "Internal Server Error"

        mock_session_send.side_effect = mocking_allowed_error_responses
        mock_session_send.return_value = final_response

        task = create_task_of_operator(
            HttpSensor,
            dag_id="http_sensor_response_error_codes_allowlist",
            task_id="http_sensor_response_error_codes_allowlist",
            response_error_codes_allowlist=["404", "499", "503"],
            http_conn_id="http_default",
            endpoint="",
            request_params={},
            method="GET",
            response_check=resp_check,
            timeout=5,
            poke_interval=1,
        )
        with pytest.raises(AirflowException, match="500:Internal Server Error"):
            task.execute(context={})


class FakeSession:
    def __init__(self):
        self.response = requests.Response()
        self.response.status_code = 200
        self.response._content = "apache/airflow".encode("ascii", "ignore")

    def send(self, *args, **kwargs):
        return self.response

    def prepare_request(self, request):
        if "date" in request.params:
            self.response._content += ("/" + request.params["date"]).encode("ascii", "ignore")
        return self.response

    def merge_environment_settings(self, _url, **kwargs):
        return kwargs

    def mount(self, prefix, adapter):
        pass


class TestHttpOpSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE_ISO}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    @mock.patch("requests.Session", FakeSession)
    def test_get(self):
        op = HttpOperator(
            task_id="get_op",
            method="GET",
            endpoint="/search",
            data={"client": "ubuntu", "q": "airflow"},
            headers={},
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch("requests.Session", FakeSession)
    def test_get_response_check(self):
        op = HttpOperator(
            task_id="get_op",
            method="GET",
            endpoint="/search",
            data={"client": "ubuntu", "q": "airflow"},
            response_check=lambda response: ("apache/airflow" in response.text),
            headers={},
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch("requests.Session", FakeSession)
    def test_sensor(self):
        sensor = HttpSensor(
            task_id="http_sensor_check",
            http_conn_id="http_default",
            endpoint="/search",
            request_params={"client": "ubuntu", "q": "airflow", "date": "{{ds}}"},
            headers={},
            response_check=lambda response: f"apache/airflow/{DEFAULT_DATE:%Y-%m-%d}" in response.text,
            poke_interval=5,
            timeout=15,
            dag=self.dag,
        )
        sensor.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
