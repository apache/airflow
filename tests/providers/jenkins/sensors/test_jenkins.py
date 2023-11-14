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

from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.sensors.jenkins import JenkinsBuildSensor


class TestJenkinsBuildSensor:
    @pytest.mark.parametrize(
        "build_number, build_state, result",
        [
            (
                None,
                True,
                "",
            ),
            (
                3,
                True,
                "",
            ),
        ],
    )
    @patch("jenkins.Jenkins")
    def test_poke_buliding(self, mock_jenkins, build_number, build_state, result):
        target_build_number = build_number or 10

        jenkins_mock = MagicMock()
        jenkins_mock.get_job_info.return_value = {"lastBuild": {"number": target_build_number}}
        jenkins_mock.get_build_info.return_value = {"building": build_state}
        mock_jenkins.return_value = jenkins_mock

        with patch.object(JenkinsHook, "get_connection") as mock_get_connection:
            mock_get_connection.return_value = MagicMock()

            sensor = JenkinsBuildSensor(
                dag=None,
                jenkins_connection_id="fake_jenkins_connection",
                task_id="sensor_test",
                job_name="a_job_on_jenkins",
                build_number=target_build_number,
                target_states=["SUCCESS"],
            )

            output = sensor.poke(None)

            assert output == (not build_state)
            assert jenkins_mock.get_job_info.call_count == 0 if build_number else 1
            jenkins_mock.get_build_info.assert_called_once_with("a_job_on_jenkins", target_build_number)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @pytest.mark.parametrize(
        "build_number, build_state, result",
        [
            (
                1,
                False,
                "SUCCESS",
            ),
            (
                2,
                False,
                "FAILED",
            ),
        ],
    )
    @patch("jenkins.Jenkins")
    def test_poke_finish_building(
        self, mock_jenkins, build_number, build_state, result, soft_fail, expected_exception
    ):
        target_build_number = build_number or 10

        jenkins_mock = MagicMock()
        jenkins_mock.get_job_info.return_value = {"lastBuild": {"number": target_build_number}}
        jenkins_mock.get_build_info.return_value = {"building": build_state, "result": result}
        mock_jenkins.return_value = jenkins_mock

        with patch.object(JenkinsHook, "get_connection") as mock_get_connection:
            mock_get_connection.return_value = MagicMock()

            sensor = JenkinsBuildSensor(
                dag=None,
                jenkins_connection_id="fake_jenkins_connection",
                task_id="sensor_test",
                job_name="a_job_on_jenkins",
                build_number=target_build_number,
                target_states=["SUCCESS"],
                soft_fail=soft_fail,
            )
            if result not in sensor.target_states:
                with pytest.raises(expected_exception):
                    sensor.poke(None)
                    assert jenkins_mock.get_build_info.call_count == 2
            else:
                output = sensor.poke(None)
                assert output == (not build_state)
                assert jenkins_mock.get_job_info.call_count == 0 if build_number else 1
                assert jenkins_mock.get_build_info.call_count == 2
