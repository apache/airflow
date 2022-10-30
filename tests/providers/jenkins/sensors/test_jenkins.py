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
from unittest.mock import MagicMock, patch

from parameterized import parameterized

from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.sensors.jenkins import JenkinsBuildSensor


class TestJenkinsBuildSensor(unittest.TestCase):
    @parameterized.expand(
        [
            (
                1,
                False,
            ),
            (
                None,
                True,
            ),
            (
                3,
                True,
            ),
        ]
    )
    @patch("jenkins.Jenkins")
    def test_poke(self, build_number, build_state, mock_jenkins):
        target_build_number = build_number if build_number else 10

        jenkins_mock = MagicMock()
        jenkins_mock.get_job_info.return_value = {"lastBuild": {"number": target_build_number}}
        jenkins_mock.get_build_info.return_value = {
            "building": build_state,
        }
        mock_jenkins.return_value = jenkins_mock

        with patch.object(JenkinsHook, "get_connection") as mock_get_connection:
            mock_get_connection.return_value = MagicMock()

            sensor = JenkinsBuildSensor(
                dag=None,
                jenkins_connection_id="fake_jenkins_connection",
                task_id="sensor_test",
                job_name="a_job_on_jenkins",
                build_number=target_build_number,
            )

            output = sensor.poke(None)

            assert output == (not build_state)
            assert jenkins_mock.get_job_info.call_count == 0 if build_number else 1
            jenkins_mock.get_build_info.assert_called_once_with("a_job_on_jenkins", target_build_number)
