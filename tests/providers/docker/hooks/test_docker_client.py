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
import logging
import unittest
from unittest import mock
from unittest.mock import call

import pytest
from docker.errors import APIError

from airflow.exceptions import AirflowException

try:
    from docker import APIClient
    from docker.types import Mount

    from airflow.providers.docker.hooks.docker_client import DockerClientHook
except ImportError:
    pass


TEMPDIR_MOCK_RETURN_VALUE = '/mkdtemp'


class TestDockerClientHook(unittest.TestCase):
    def setUp(self):
        self.tempdir_patcher = mock.patch('airflow.providers.docker.hooks.docker_client.TemporaryDirectory')
        self.tempdir_mock = self.tempdir_patcher.start()
        self.tempdir_mock.return_value.__enter__.return_value = TEMPDIR_MOCK_RETURN_VALUE

        self.client_mock = mock.Mock(spec=APIClient)
        self.client_mock.create_container.return_value = {'Id': 'some_id'}
        self.client_mock.images.return_value = []
        self.client_mock.attach.return_value = ['container log 1', 'container log 2']
        self.client_mock.pull.return_value = {"status": "pull log"}
        self.client_mock.wait.return_value = {"StatusCode": 0}
        self.client_mock.create_host_config.return_value = mock.Mock()
        self.client_class_patcher = mock.patch(
            'airflow.providers.docker.hooks.docker_client.APIClient',
            return_value=self.client_mock,
        )
        self.client_class_mock = self.client_class_patcher.start()

    def tearDown(self) -> None:
        self.tempdir_patcher.stop()
        self.client_class_patcher.stop()

    def test_execute(self):
        hook = DockerClientHook(
            cli=self.client_mock,
            command='env',
            environment={'UNIT': 'TEST'},
            private_environment={'PRIVATE': 'MESSAGE'},
            image='ubuntu:latest',
            network_mode='bridge',
            mounts=[Mount(source='/host/path', target='/container/path', type='bind')],
            entrypoint='["sh", "-c"]',
            working_dir='/container/path',
            shm_size=1000,
            host_tmp_dir='/host/airflow',
            container_name='test_container',
            tty=True,
        )
        hook.run_image()

        self.client_mock.create_container.assert_called_once_with(
            command='env',
            name='test_container',
            environment={'AIRFLOW_TMP_DIR': '/tmp/airflow', 'UNIT': 'TEST', 'PRIVATE': 'MESSAGE'},
            host_config=self.client_mock.create_host_config.return_value,
            image='ubuntu:latest',
            user=None,
            entrypoint=['sh', '-c'],
            working_dir='/container/path',
            tty=True,
        )
        self.client_mock.create_host_config.assert_called_once_with(
            mounts=[
                Mount(source='/host/path', target='/container/path', type='bind'),
                Mount(source='/mkdtemp', target='/tmp/airflow', type='bind'),
            ],
            network_mode='bridge',
            shm_size=1000,
            cpu_shares=1024,
            mem_limit=None,
            auto_remove=False,
            dns=None,
            dns_search=None,
            cap_add=None,
            extra_hosts=None,
            privileged=False,
        )
        self.tempdir_mock.assert_called_once_with(dir='/host/airflow', prefix='airflowtmp')
        self.client_mock.images.assert_called_once_with(name='ubuntu:latest')
        self.client_mock.attach.assert_called_once_with(
            container='some_id', stdout=True, stderr=True, stream=True
        )
        self.client_mock.pull.assert_called_once_with('ubuntu:latest', stream=True, decode=True)
        self.client_mock.wait.assert_called_once_with('some_id')
        assert hook.cli.pull('ubuntu:latest', stream=True, decode=True) == self.client_mock.pull.return_value

    def test_execute_no_temp_dir(self):
        hook = DockerClientHook(
            cli=self.client_mock,
            command='env',
            environment={'UNIT': 'TEST'},
            private_environment={'PRIVATE': 'MESSAGE'},
            image='ubuntu:latest',
            network_mode='bridge',
            mounts=[Mount(source='/host/path', target='/container/path', type='bind')],
            mount_tmp_dir=False,
            entrypoint='["sh", "-c"]',
            working_dir='/container/path',
            shm_size=1000,
            host_tmp_dir='/host/airflow',
            container_name='test_container',
            tty=True,
        )
        hook.run_image()

        self.client_mock.create_container.assert_called_once_with(
            command='env',
            name='test_container',
            environment={'UNIT': 'TEST', 'PRIVATE': 'MESSAGE'},
            host_config=self.client_mock.create_host_config.return_value,
            image='ubuntu:latest',
            user=None,
            entrypoint=['sh', '-c'],
            working_dir='/container/path',
            tty=True,
        )
        self.client_mock.create_host_config.assert_called_once_with(
            mounts=[
                Mount(source='/host/path', target='/container/path', type='bind'),
            ],
            network_mode='bridge',
            shm_size=1000,
            cpu_shares=1024,
            mem_limit=None,
            auto_remove=False,
            dns=None,
            dns_search=None,
            cap_add=None,
            extra_hosts=None,
            privileged=False,
        )
        self.tempdir_mock.assert_not_called()
        self.client_mock.images.assert_called_once_with(name='ubuntu:latest')
        self.client_mock.attach.assert_called_once_with(
            container='some_id', stdout=True, stderr=True, stream=True
        )
        self.client_mock.pull.assert_called_once_with('ubuntu:latest', stream=True, decode=True)
        self.client_mock.wait.assert_called_once_with('some_id')
        assert hook.cli.pull('ubuntu:latest', stream=True, decode=True) == self.client_mock.pull.return_value

    def test_execute_fallback_temp_dir(self):
        self.client_mock.create_container.side_effect = [
            APIError(message="wrong path: " + TEMPDIR_MOCK_RETURN_VALUE),
            {'Id': 'some_id'},
        ]
        hook = DockerClientHook(
            cli=self.client_mock,
            command='env',
            environment={'UNIT': 'TEST'},
            private_environment={'PRIVATE': 'MESSAGE'},
            image='ubuntu:latest',
            network_mode='bridge',
            mounts=[Mount(source='/host/path', target='/container/path', type='bind')],
            mount_tmp_dir=True,
            entrypoint='["sh", "-c"]',
            working_dir='/container/path',
            shm_size=1000,
            host_tmp_dir='/host/airflow',
            container_name='test_container',
            tty=True,
        )
        with self.assertLogs(hook.log, level=logging.WARNING) as captured:
            hook.run_image()
            assert (
                "WARNING:airflow.providers.docker.hooks.docker_client.DockerClientHook:Using remote engine "
                "or docker-in-docker and mounting temporary volume from host is not supported"
                in captured.output[0]
            )
        self.client_mock.create_container.assert_has_calls(
            [
                call(
                    command='env',
                    name='test_container',
                    environment={'AIRFLOW_TMP_DIR': '/tmp/airflow', 'UNIT': 'TEST', 'PRIVATE': 'MESSAGE'},
                    host_config=self.client_mock.create_host_config.return_value,
                    image='ubuntu:latest',
                    user=None,
                    entrypoint=['sh', '-c'],
                    working_dir='/container/path',
                    tty=True,
                ),
                call(
                    command='env',
                    name='test_container',
                    environment={'UNIT': 'TEST', 'PRIVATE': 'MESSAGE'},
                    host_config=self.client_mock.create_host_config.return_value,
                    image='ubuntu:latest',
                    user=None,
                    entrypoint=['sh', '-c'],
                    working_dir='/container/path',
                    tty=True,
                ),
            ]
        )
        self.client_mock.create_host_config.assert_has_calls(
            [
                call(
                    mounts=[
                        Mount(source='/host/path', target='/container/path', type='bind'),
                        Mount(source='/mkdtemp', target='/tmp/airflow', type='bind'),
                    ],
                    network_mode='bridge',
                    shm_size=1000,
                    cpu_shares=1024,
                    mem_limit=None,
                    auto_remove=False,
                    dns=None,
                    dns_search=None,
                    cap_add=None,
                    extra_hosts=None,
                    privileged=False,
                ),
                call(
                    mounts=[
                        Mount(source='/host/path', target='/container/path', type='bind'),
                    ],
                    network_mode='bridge',
                    shm_size=1000,
                    cpu_shares=1024,
                    mem_limit=None,
                    auto_remove=False,
                    dns=None,
                    dns_search=None,
                    cap_add=None,
                    extra_hosts=None,
                    privileged=False,
                ),
            ]
        )
        self.tempdir_mock.assert_called_once_with(dir='/host/airflow', prefix='airflowtmp')
        self.client_mock.images.assert_called_once_with(name='ubuntu:latest')
        self.client_mock.attach.assert_called_once_with(
            container='some_id', stdout=True, stderr=True, stream=True
        )
        self.client_mock.pull.assert_called_once_with('ubuntu:latest', stream=True, decode=True)
        self.client_mock.wait.assert_called_once_with('some_id')
        assert hook.cli.pull('ubuntu:latest', stream=True, decode=True) == self.client_mock.pull.return_value

    def test_private_environment_is_private(self):
        hook = DockerClientHook(
            cli=self.client_mock, private_environment={'PRIVATE': 'MESSAGE'}, image='ubuntu:latest'
        )
        assert hook._private_environment == {
            'PRIVATE': 'MESSAGE'
        }, "To keep this private, it must be an underscored attribute."

    def test_execute_container_fails(self):
        self.client_mock.wait.return_value = {"StatusCode": 1}
        hook = DockerClientHook(cli=self.client_mock, image='ubuntu')
        with pytest.raises(AirflowException):
            hook.run_image()

    def test_auto_remove_container_fails(self):
        self.client_mock.wait.return_value = {"StatusCode": 1}
        hook = DockerClientHook(cli=self.client_mock, image='ubuntu', auto_remove=True)
        hook.container = {'Id': 'some_id'}
        with pytest.raises(AirflowException):
            hook.run_image()

        self.client_mock.remove_container.assert_called_once_with('some_id')

    @staticmethod
    def test_stop():
        client_mock = mock.Mock(spec=APIClient)

        hook = DockerClientHook(cli=client_mock, image='ubuntu')
        hook.container = {'Id': 'some_id'}

        hook.stop()

        client_mock.stop.assert_called_once_with('some_id')

    def test_extra_hosts(self):
        hosts_obj = mock.Mock()
        hook = DockerClientHook(cli=self.client_mock, image='test', extra_hosts=hosts_obj)
        hook.run_image()
        self.client_mock.create_container.assert_called_once()
        assert 'host_config' in self.client_mock.create_container.call_args[1]
        assert 'extra_hosts' in self.client_mock.create_host_config.call_args[1]
        assert hosts_obj is self.client_mock.create_host_config.call_args[1]['extra_hosts']

    def test_privileged(self):
        privileged = mock.Mock()
        hook = DockerClientHook(cli=self.client_mock, image='test', privileged=privileged)
        hook.run_image()
        self.client_mock.create_container.assert_called_once()
        assert 'host_config' in self.client_mock.create_container.call_args[1]
        assert 'privileged' in self.client_mock.create_host_config.call_args[1]
        assert privileged is self.client_mock.create_host_config.call_args[1]['privileged']
