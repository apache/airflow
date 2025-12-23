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
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import jenkins
import pytest
from jenkins import JenkinsException

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator

TEST_PARAMETERS = (
    pytest.param({"a_param": "blip", "another_param": "42"}, id="dict params"),
    pytest.param('{"second_param": "beep", "third_param": "153"}', id="string params"),
    pytest.param(["final_one", "bop", "real_final", "eggs"], id="list params"),
)


class TestJenkinsOperator:
    @pytest.mark.parametrize("parameters", TEST_PARAMETERS)
    def test_execute(self, parameters, mocker):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth="secret")
        jenkins_mock.get_build_info.return_value = {
            "result": "SUCCESS",
            "url": "http://aaa.fake-url.com/congratulation/its-a-job",
        }
        jenkins_mock.build_job_url.return_value = "http://www.jenkins.url/somewhere/in/the/universe"
        jenkins_mock.get_queue_item.side_effect = [
            {},
            {"executable": {"number": "1"}},
        ]

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with (
            patch.object(
                JenkinsJobTriggerOperator,
                "hook",
                new_callable=mocker.PropertyMock,
            ) as hook_mocked,
            patch(
                "airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers"
            ) as mock_make_request,
        ):
            mock_make_request.return_value = {
                "body": "",
                "headers": {"Location": "http://what-a-strange.url/queue/item/18/"},
            }
            hook_mocked.return_value = hook_mock

            operator = JenkinsJobTriggerOperator(
                dag=None,
                jenkins_connection_id="fake_jenkins_connection",
                # The hook is mocked, this connection won't be used
                task_id="operator_test",
                job_name="a_job_on_jenkins",
                parameters=parameters,
                sleep_time=1,
            )

            operator.execute(None)

            assert jenkins_mock.get_build_info.call_count == 1
            jenkins_mock.get_build_info.assert_called_once_with(name="a_job_on_jenkins", number="1")

    @pytest.mark.parametrize("parameters", TEST_PARAMETERS)
    @mock.patch.object(JenkinsJobTriggerOperator, "log")
    def test_execute_job_polling_loop(self, mock_log, parameters, mocker):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth="secret")
        jenkins_mock.get_job_info.return_value = {"nextBuildNumber": "1"}
        jenkins_mock.get_build_info.side_effect = [
            {"result": None},
            {"result": "SUCCESS", "url": "http://aaa.fake-url.com/congratulation/its-a-job"},
        ]
        jenkins_mock.build_job_url.return_value = "http://www.jenkins.url/somewhere/in/the/universe"
        jenkins_mock.get_queue_item.side_effect = [
            {},
            HTTPError(
                url="http://aaa.fake-url.com", code=500, msg="Internal Server Error", hdrs=None, fp=None
            ),
            JenkinsException("Jenkins is unavailable"),
            {"executable": {"number": "1"}},
        ]

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with (
            patch.object(
                JenkinsJobTriggerOperator,
                "hook",
                new_callable=mocker.PropertyMock,
            ) as hook_mocked,
            patch(
                "airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers"
            ) as mock_make_request,
        ):
            mock_make_request.return_value = {
                "body": "",
                "headers": {"Location": "http://what-a-strange.url/queue/item/18/"},
            }
            hook_mocked.return_value = hook_mock
            operator = JenkinsJobTriggerOperator(
                dag=None,
                task_id="operator_test",
                job_name="a_job_on_jenkins",
                jenkins_connection_id="fake_jenkins_connection",
                # The hook is mocked, this connection won't be used
                parameters=parameters,
                sleep_time=1,
            )

            operator.execute(None)
            assert jenkins_mock.get_build_info.call_count == 2
            assert jenkins_mock.get_queue_item.call_count == 4
            # on HTTPError and JenkinsException
            assert mock_log.warning.mock_calls == [
                mock.call("polling failed, retrying", exc_info=True),
                mock.call("polling failed, retrying", exc_info=True),
            ]

    @pytest.mark.parametrize("parameters", TEST_PARAMETERS)
    def test_execute_job_failure(self, parameters, mocker):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth="secret")
        jenkins_mock.get_job_info.return_value = {"nextBuildNumber": "1"}
        jenkins_mock.get_build_info.return_value = {
            "result": "FAILURE",
            "url": "http://aaa.fake-url.com/congratulation/its-a-job",
        }
        jenkins_mock.build_job_url.return_value = "http://www.jenkins.url/somewhere/in/the/universe"
        jenkins_mock.get_queue_item.side_effect = [
            {},
            {"executable": {"number": "1"}},
        ]

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with (
            patch.object(
                JenkinsJobTriggerOperator,
                "hook",
                new_callable=mocker.PropertyMock,
            ) as hook_mocked,
            patch(
                "airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers"
            ) as mock_make_request,
        ):
            mock_make_request.return_value = {
                "body": "",
                "headers": {"Location": "http://what-a-strange.url/queue/item/18/"},
            }
            hook_mocked.return_value = hook_mock
            operator = JenkinsJobTriggerOperator(
                dag=None,
                task_id="operator_test",
                job_name="a_job_on_jenkins",
                parameters=parameters,
                jenkins_connection_id="fake_jenkins_connection",
                # The hook is mocked, this connection won't be used
                sleep_time=1,
            )

            with pytest.raises(AirflowException):
                operator.execute(None)

    @pytest.mark.parametrize(
        ("state", "allowed_jenkins_states"),
        [
            (
                "SUCCESS",
                ["SUCCESS", "UNSTABLE"],
            ),
            (
                "UNSTABLE",
                ["SUCCESS", "UNSTABLE"],
            ),
            (
                "UNSTABLE",
                ["UNSTABLE"],
            ),
            (
                "SUCCESS",
                None,
            ),
        ],
    )
    def test_allowed_jenkins_states(self, state, allowed_jenkins_states, mocker):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth="secret")
        jenkins_mock.get_job_info.return_value = {"nextBuildNumber": "1"}
        jenkins_mock.get_build_info.return_value = {
            "result": state,
            "url": "http://aaa.fake-url.com/congratulation/its-a-job",
        }
        jenkins_mock.build_job_url.return_value = "http://www.jenkins.url/somewhere/in/the/universe"
        jenkins_mock.get_queue_item.side_effect = [
            {},
            {"executable": {"number": "1"}},
        ]

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with (
            patch.object(
                JenkinsJobTriggerOperator,
                "hook",
                new_callable=mocker.PropertyMock,
            ) as hook_mocked,
            patch(
                "airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers",
            ) as mock_make_request,
        ):
            mock_make_request.return_value = {
                "body": "",
                "headers": {"Location": "http://what-a-strange.url/queue/item/18/"},
            }
            hook_mocked.return_value = hook_mock
            operator = JenkinsJobTriggerOperator(
                dag=None,
                task_id="operator_test",
                job_name="a_job_on_jenkins",
                jenkins_connection_id="fake_jenkins_connection",
                allowed_jenkins_states=allowed_jenkins_states,
                # The hook is mocked, this connection won't be used
                sleep_time=1,
            )

            try:
                operator.execute(None)
            except AirflowException:
                pytest.fail(f"Job failed with state={state} while allowed states={allowed_jenkins_states}")

    @pytest.mark.parametrize(
        ("state", "allowed_jenkins_states"),
        [
            (
                "FAILURE",
                ["SUCCESS", "UNSTABLE"],
            ),
            (
                "UNSTABLE",
                ["SUCCESS"],
            ),
            (
                "SUCCESS",
                ["UNSTABLE"],
            ),
            (
                "FAILURE",
                None,
            ),
            (
                "UNSTABLE",
                None,
            ),
        ],
    )
    def test_allowed_jenkins_states_failure(self, state, allowed_jenkins_states, mocker):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth="secret")
        jenkins_mock.get_job_info.return_value = {"nextBuildNumber": "1"}
        jenkins_mock.get_build_info.return_value = {
            "result": state,
            "url": "http://aaa.fake-url.com/congratulation/its-a-job",
        }
        jenkins_mock.build_job_url.return_value = "http://www.jenkins.url/somewhere/in/the/universe"
        jenkins_mock.get_queue_item.side_effect = [
            {},
            {"executable": {"number": "1"}},
        ]

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with (
            patch.object(
                JenkinsJobTriggerOperator,
                "hook",
                new_callable=mocker.PropertyMock,
            ) as hook_mocked,
            patch(
                "airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers"
            ) as mock_make_request,
        ):
            mock_make_request.return_value = {
                "body": "",
                "headers": {"Location": "http://what-a-strange.url/queue/item/18/"},
            }
            hook_mocked.return_value = hook_mock
            operator = JenkinsJobTriggerOperator(
                dag=None,
                task_id="operator_test",
                job_name="a_job_on_jenkins",
                jenkins_connection_id="fake_jenkins_connection",
                allowed_jenkins_states=allowed_jenkins_states,
                # The hook is mocked, this connection won't be used
                sleep_time=1,
            )

            with pytest.raises(AirflowException):
                operator.execute(None)

    def test_build_job_request_settings(self):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth="secret", timeout=2)
        jenkins_mock.build_job_url.return_value = "http://apache.org"

        with patch(
            "airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers"
        ) as mock_make_request:
            operator = JenkinsJobTriggerOperator(
                dag=None,
                task_id="build_job_test",
                job_name="a_job_on_jenkins",
                jenkins_connection_id="fake_jenkins_connection",
            )
            operator.build_job(jenkins_mock)
            mock_request = mock_make_request.call_args_list[0][0][1]

        assert mock_request.method == "POST"
        assert mock_request.url == "http://apache.org"
