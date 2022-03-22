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
from unittest.mock import Mock, patch
from xmlrpc.client import boolean

import jenkins
import pytest
from parameterized import parameterized

from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.sensors.jenkins import JenkinsBuildSensor

class TestJenkinsBuildSensor(unittest.TestCase):
    @parameterized.expand(
        [
            (1,False,),
            (None, True,),
            (3, True,),
        ]
    )
    def test_pole(self, _, build_number, build_state):
        jenkins_mock = Mock(spec=jenkins.Jenkins, auth='secret')
        jenkins_mock.get_job_info.return_value = {'lastBuild': {'number': 10}}
        jenkins_mock.get_build_info.return_value = {
            'building': build_state,
        }

        hook_mock = Mock(spec=JenkinsHook)
        hook_mock.get_jenkins_server.return_value = jenkins_mock

        with patch.object(JenkinsBuildSensor, "get_hook") as get_hook_mocked:
            get_hook_mocked.return_value = hook_mock
            sensor = JenkinsBuildSensor(
                dag=None,
                jenkins_connection_id="fake_jenkins_connection",
                # The hook is mocked, this connection won't be used
                task_id="sensor_test",
                job_name="a_job_on_jenkins",
                build_number=build_number,
                sleep_time=1,
            )

            output = sensor.poke(None)

            assert output == (not build_state)
            assert jenkins_mock.get_job_info.call_count == 0 if build_number else 1
            assert jenkins_mock.get_build_info.call_count == 1
            jenkins_mock.get_build_info.assert_called_once_with(name='a_job_on_jenkins', number=(build_number if build_number else 10))


