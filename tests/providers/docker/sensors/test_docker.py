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
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.providers.docker.sensors.docker import DockerSensor


@mock.patch('airflow.providers.docker.sensors.docker.get_client', autospec=True)
@mock.patch('airflow.providers.docker.sensors.docker.DockerClientHook')
class TestDockerOperator(unittest.TestCase):
    def test_sensor_ok_on_container_success(self, hook_class, get_client_mock):
        get_client_mock.return_value = mock.Mock()
        sensor = DockerSensor(image='ubuntu', task_id='unittest')
        sensor.cli = get_client_mock.return_value
        assert sensor.poke({})

    def test_sensor_ng_on_container_failure(self, hook_class, get_client_mock):
        get_client_mock.return_value = mock.Mock()
        sensor = DockerSensor(image='ubuntu', task_id='unittest')
        sensor.cli = get_client_mock.return_value
        hook = hook_class.return_value
        hook.run_image.side_effect = AirflowException()
        assert not sensor.poke({})
