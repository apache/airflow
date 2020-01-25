# -*- coding: utf-8 -*-
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

import unittest

import jenkins
import mock

from airflow.exceptions import AirflowException
from airflow.providers.jenking.hooks.jenkins import JenkinsHook
from airflow.providers.jenking.operators.jenkins_job_trigger import JenkinsJobTriggerOperator


class TestJenkinsOperator(unittest.TestCase):
    @unittest.skipIf(mock is None, 'mock package not present')
    def test_execute(self):
        jenkins_mock = mock.Mock(spec=jenkins.Jenkins, auth='secret')
        jenkins_mock.get_build_info.return_value = \
            {'result': 'SUCCESS',
             'url': 'http://aaa.fake-url.com/congratulation/its-a-job'}
        jenkins_mock.build_job_url.return_value = \
            'http://www.jenkins.url/somewhere/in/the/universe'

        hook_mock = mock.Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        the_parameters = {'a_param': 'blip', 'another_param': '42'}

        with mock.patch.object(JenkinsJobTriggerOperator, "get_hook") as get_hook_mocked,\
            mock.patch(
                'airflow.providers.jenking.operators.jenkins_job_trigger.jenkins_request_with_headers') \
                as mock_make_request:
            mock_make_request.side_effect = \
                [{'body': '', 'headers': {'Location': 'http://what-a-strange.url/18'}},
                 {'body': '{"executable":{"number":"1"}}', 'headers': {}}]
            get_hook_mocked.return_value = hook_mock
            operator = JenkinsJobTriggerOperator(
                dag=None,
                jenkins_connection_id="fake_jenkins_connection",
                # The hook is mocked, this connection won't be used
                task_id="operator_test",
                job_name="a_job_on_jenkins",
                parameters=the_parameters,
                sleep_time=1)

            operator.execute(None)

            self.assertEqual(jenkins_mock.get_build_info.call_count, 1)
            jenkins_mock.get_build_info.assert_called_once_with(name='a_job_on_jenkins',
                                                                number='1')

    @unittest.skipIf(mock is None, 'mock package not present')
    def test_execute_job_polling_loop(self):
        jenkins_mock = mock.Mock(spec=jenkins.Jenkins, auth='secret')
        jenkins_mock.get_job_info.return_value = {'nextBuildNumber': '1'}
        jenkins_mock.get_build_info.side_effect = \
            [{'result': None},
             {'result': 'SUCCESS',
              'url': 'http://aaa.fake-url.com/congratulation/its-a-job'}]
        jenkins_mock.build_job_url.return_value = \
            'http://www.jenkins.url/somewhere/in/the/universe'

        hook_mock = mock.Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        the_parameters = {'a_param': 'blip', 'another_param': '42'}

        with mock.patch.object(JenkinsJobTriggerOperator, "get_hook") as get_hook_mocked,\
            mock.patch(
                'airflow.providers.jenking.operators.jenkins_job_trigger.jenkins_request_with_headers') \
                as mock_make_request:
            mock_make_request.side_effect = \
                [{'body': '', 'headers': {'Location': 'http://what-a-strange.url/18'}},
                 {'body': '{"executable":{"number":"1"}}', 'headers': {}}]
            get_hook_mocked.return_value = hook_mock
            operator = JenkinsJobTriggerOperator(
                dag=None,
                task_id="operator_test",
                job_name="a_job_on_jenkins",
                jenkins_connection_id="fake_jenkins_connection",
                # The hook is mocked, this connection won't be used
                parameters=the_parameters,
                sleep_time=1)

            operator.execute(None)
            self.assertEqual(jenkins_mock.get_build_info.call_count, 2)

    @unittest.skipIf(mock is None, 'mock package not present')
    def test_execute_job_failure(self):
        jenkins_mock = mock.Mock(spec=jenkins.Jenkins, auth='secret')
        jenkins_mock.get_job_info.return_value = {'nextBuildNumber': '1'}
        jenkins_mock.get_build_info.return_value = {
            'result': 'FAILURE',
            'url': 'http://aaa.fake-url.com/congratulation/its-a-job'}
        jenkins_mock.build_job_url.return_value = \
            'http://www.jenkins.url/somewhere/in/the/universe'

        hook_mock = mock.Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        the_parameters = {'a_param': 'blip', 'another_param': '42'}

        with mock.patch.object(JenkinsJobTriggerOperator, "get_hook") as get_hook_mocked,\
            mock.patch(
                'airflow.providers.jenking.operators.jenkins_job_trigger.jenkins_request_with_headers') \
                as mock_make_request:
            mock_make_request.side_effect = \
                [{'body': '', 'headers': {'Location': 'http://what-a-strange.url/18'}},
                 {'body': '{"executable":{"number":"1"}}', 'headers': {}}]
            get_hook_mocked.return_value = hook_mock
            operator = JenkinsJobTriggerOperator(
                dag=None,
                task_id="operator_test",
                job_name="a_job_on_jenkins",
                parameters=the_parameters,
                jenkins_connection_id="fake_jenkins_connection",
                # The hook is mocked, this connection won't be used
                sleep_time=1)

            self.assertRaises(AirflowException, operator.execute, None)

    @unittest.skipIf(mock is None, 'mock package not present')
    def test_build_job_request_settings(self):
        jenkins_mock = mock.Mock(spec=jenkins.Jenkins, auth='secret', timeout=2)
        jenkins_mock.build_job_url.return_value = 'http://apache.org'

        with mock.patch(
            'airflow.providers.jenking.operators.jenkins_job_trigger.jenkins_request_with_headers'
        ) as mock_make_request:
            operator = JenkinsJobTriggerOperator(
                dag=None,
                task_id="build_job_test",
                job_name="a_job_on_jenkins",
                jenkins_connection_id="fake_jenkins_connection")
            operator.build_job(jenkins_mock)
            mock_request = mock_make_request.call_args_list[0][0][1]

        self.assertEqual(mock_request.method, 'POST')
        self.assertEqual(mock_request.url, 'http://apache.org')


if __name__ == "__main__":
    unittest.main()
