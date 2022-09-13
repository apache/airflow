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

import unittest
from unittest.mock import Mock, patch

import jenkins
import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator


class TestJenkinsOperator(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "dict params",
                {'a_param': 'blip', 'another_param': '42'},
            ),
            (
                "string params",
                '{"second_param": "beep", "third_param": "153"}',
            ),
            (
                "list params",
                ['final_one', 'bop', 'real_final', 'eggs'],
            ),
        ]
    )
    def test_execute(self, _, parameters):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth='secret')
        jenkins_mock.get_build_info.return_value = {
            'result': 'SUCCESS',
            'url': 'http://aaa.fake-url.com/congratulation/its-a-job',
        }
        jenkins_mock.build_job_url.return_value = 'http://www.jenkins.url/somewhere/in/the/universe'

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with patch.object(JenkinsJobTriggerOperator, "get_hook") as get_hook_mocked, patch(
            'airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers'
        ) as mock_make_request:
            mock_make_request.side_effect = [
                {'body': '', 'headers': {'Location': 'http://what-a-strange.url/18'}},
                {'body': '{"executable":{"number":"1"}}', 'headers': {}},
            ]
            get_hook_mocked.return_value = hook_mock
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
            jenkins_mock.get_build_info.assert_called_once_with(name='a_job_on_jenkins', number='1')

    @parameterized.expand(
        [
            (
                "dict params",
                {'a_param': 'blip', 'another_param': '42'},
            ),
            (
                "string params",
                '{"second_param": "beep", "third_param": "153"}',
            ),
            (
                "list params",
                ['final_one', 'bop', 'real_final', 'eggs'],
            ),
        ]
    )
    def test_execute_job_polling_loop(self, _, parameters):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth='secret')
        jenkins_mock.get_job_info.return_value = {'nextBuildNumber': '1'}
        jenkins_mock.get_build_info.side_effect = [
            {'result': None},
            {'result': 'SUCCESS', 'url': 'http://aaa.fake-url.com/congratulation/its-a-job'},
        ]
        jenkins_mock.build_job_url.return_value = 'http://www.jenkins.url/somewhere/in/the/universe'

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with patch.object(JenkinsJobTriggerOperator, "get_hook") as get_hook_mocked, patch(
            'airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers'
        ) as mock_make_request:
            mock_make_request.side_effect = [
                {'body': '', 'headers': {'Location': 'http://what-a-strange.url/18'}},
                {'body': '{"executable":{"number":"1"}}', 'headers': {}},
            ]
            get_hook_mocked.return_value = hook_mock
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

    @parameterized.expand(
        [
            (
                "dict params",
                {'a_param': 'blip', 'another_param': '42'},
            ),
            (
                "string params",
                '{"second_param": "beep", "third_param": "153"}',
            ),
            (
                "list params",
                ['final_one', 'bop', 'real_final', 'eggs'],
            ),
        ]
    )
    def test_execute_job_failure(self, _, parameters):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth='secret')
        jenkins_mock.get_job_info.return_value = {'nextBuildNumber': '1'}
        jenkins_mock.get_build_info.return_value = {
            'result': 'FAILURE',
            'url': 'http://aaa.fake-url.com/congratulation/its-a-job',
        }
        jenkins_mock.build_job_url.return_value = 'http://www.jenkins.url/somewhere/in/the/universe'

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with patch.object(JenkinsJobTriggerOperator, "get_hook") as get_hook_mocked, patch(
            'airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers'
        ) as mock_make_request:
            mock_make_request.side_effect = [
                {'body': '', 'headers': {'Location': 'http://what-a-strange.url/18'}},
                {'body': '{"executable":{"number":"1"}}', 'headers': {}},
            ]
            get_hook_mocked.return_value = hook_mock
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

    @parameterized.expand(
        [
            (
                'SUCCESS',
                ['SUCCESS', 'UNSTABLE'],
            ),
            (
                'UNSTABLE',
                ['SUCCESS', 'UNSTABLE'],
            ),
            (
                'UNSTABLE',
                ['UNSTABLE'],
            ),
            (
                'SUCCESS',
                None,
            ),
        ]
    )
    def test_allowed_jenkins_states(self, state, allowed_jenkins_states):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth='secret')
        jenkins_mock.get_job_info.return_value = {'nextBuildNumber': '1'}
        jenkins_mock.get_build_info.return_value = {
            'result': state,
            'url': 'http://aaa.fake-url.com/congratulation/its-a-job',
        }
        jenkins_mock.build_job_url.return_value = 'http://www.jenkins.url/somewhere/in/the/universe'

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with patch.object(JenkinsJobTriggerOperator, "get_hook") as get_hook_mocked, patch(
            'airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers'
        ) as mock_make_request:
            mock_make_request.side_effect = [
                {'body': '', 'headers': {'Location': 'http://what-a-strange.url/18'}},
                {'body': '{"executable":{"number":"1"}}', 'headers': {}},
            ]
            get_hook_mocked.return_value = hook_mock
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
                pytest.fail(f'Job failed with state={state} while allowed states={allowed_jenkins_states}')

    @parameterized.expand(
        [
            (
                'FAILURE',
                ['SUCCESS', 'UNSTABLE'],
            ),
            (
                'UNSTABLE',
                ['SUCCESS'],
            ),
            (
                'SUCCESS',
                ['UNSTABLE'],
            ),
            (
                'FAILURE',
                None,
            ),
            (
                'UNSTABLE',
                None,
            ),
        ]
    )
    def test_allowed_jenkins_states_failure(self, state, allowed_jenkins_states):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth='secret')
        jenkins_mock.get_job_info.return_value = {'nextBuildNumber': '1'}
        jenkins_mock.get_build_info.return_value = {
            'result': state,
            'url': 'http://aaa.fake-url.com/congratulation/its-a-job',
        }
        jenkins_mock.build_job_url.return_value = 'http://www.jenkins.url/somewhere/in/the/universe'

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with patch.object(JenkinsJobTriggerOperator, "get_hook") as get_hook_mocked, patch(
            'airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers'
        ) as mock_make_request:
            mock_make_request.side_effect = [
                {'body': '', 'headers': {'Location': 'http://what-a-strange.url/18'}},
                {'body': '{"executable":{"number":"1"}}', 'headers': {}},
            ]
            get_hook_mocked.return_value = hook_mock
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
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth='secret', timeout=2)
        jenkins_mock.build_job_url.return_value = 'http://apache.org'

        with patch(
            'airflow.providers.jenkins.operators.jenkins_job_trigger.jenkins_request_with_headers'
        ) as mock_make_request:
            operator = JenkinsJobTriggerOperator(
                dag=None,
                task_id="build_job_test",
                job_name="a_job_on_jenkins",
                jenkins_connection_id="fake_jenkins_connection",
            )
            operator.build_job(jenkins_mock)
            mock_request = mock_make_request.call_args_list[0][0][1]

        assert mock_request.method == 'POST'
        assert mock_request.url == 'http://apache.org'
