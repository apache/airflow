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

import pytest

from airflow.exceptions import AirflowException

try:
    from docker import APIClient
    from docker.types import Mount

    from airflow.providers.docker.hooks.docker_client import DockerClientHook
    from airflow.providers.docker.operators.docker import DockerOperator
except ImportError:
    pass


TEMPDIR_MOCK_RETURN_VALUE = '/mkdtemp'


class TestDockerOperator(unittest.TestCase):
    def setUp(self):

        self.hook_mock = mock.Mock(spec=DockerClientHook)
        self.hook_mock.run_image.return_value = (['container log 1', 'container log 2'], 'container log 2')
        self.hook_class_patcher = mock.patch(
            'airflow.providers.docker.operators.docker.DockerClientHook',
            return_value=self.hook_mock,
        )
        self.hook_class_mock = self.hook_class_patcher.start()

        self.client_mock = mock.Mock(spec=APIClient)
        self.client_class_patcher = mock.patch(
            'airflow.providers.docker.hooks.docker.APIClient',
            return_value=self.client_mock,
        )
        self.client_class_mock = self.client_class_patcher.start()

    def tearDown(self) -> None:
        self.hook_class_patcher.stop()
        self.client_class_patcher.stop()

    def test_execute(self):
        operator = DockerOperator(
            api_version='1.19',
            command='env',
            environment={'UNIT': 'TEST'},
            private_environment={'PRIVATE': 'MESSAGE'},
            image='ubuntu:latest',
            network_mode='bridge',
            owner='unittest',
            task_id='unittest',
            mounts=[Mount(source='/host/path', target='/container/path', type='bind')],
            entrypoint='["sh", "-c"]',
            working_dir='/container/path',
            shm_size=1000,
            host_tmp_dir='/host/airflow',
            container_name='test_container',
            tty=True,
        )
        operator.execute(None)

        self.client_class_mock.assert_called_once_with(
            base_url='unix://var/run/docker.sock', tls=None, version='1.19'
        )

        self.hook_class_mock.assert_called_once_with(
            self.client_mock,
            'ubuntu:latest',
            'env',
            'test_container',
            1.0,
            {'UNIT': 'TEST'},
            {'PRIVATE': 'MESSAGE'},
            False,
            None,
            '/host/airflow',
            'bridge',
            True,
            '/tmp/airflow',
            None,
            [{'Target': '/container/path', 'Source': '/host/path', 'Type': 'bind', 'ReadOnly': False}],
            '["sh", "-c"]',
            '/container/path',
            None,
            None,
            1000,
            True,
            False,
            None,
            None,
        )

        self.hook_mock.run_image.assert_called_once_with()

    def test_private_environment_is_private(self):
        operator = DockerOperator(
            private_environment={'PRIVATE': 'MESSAGE'}, image='ubuntu:latest', task_id='unittest'
        )
        assert operator._private_environment == {
            'PRIVATE': 'MESSAGE'
        }, "To keep this private, it must be an underscored attribute."

    @mock.patch('airflow.providers.docker.operators.docker.tls.TLSConfig')
    def test_execute_tls(self, tls_class_mock):
        tls_mock = mock.Mock()
        tls_class_mock.return_value = tls_mock

        operator = DockerOperator(
            docker_url='tcp://127.0.0.1:2376',
            image='ubuntu',
            owner='unittest',
            task_id='unittest',
            tls_client_cert='cert.pem',
            tls_ca_cert='ca.pem',
            tls_client_key='key.pem',
        )
        operator.execute(None)

        tls_class_mock.assert_called_once_with(
            assert_hostname=None,
            ca_cert='ca.pem',
            client_cert=('cert.pem', 'key.pem'),
            ssl_version=None,
            verify=True,
        )

        self.client_class_mock.assert_called_once_with(
            base_url='https://127.0.0.1:2376', tls=tls_mock, version=None
        )

    def test_execute_unicode_logs(self):
        self.hook_mock.run_image.return_value = (['unicode container log 😁'], '')

        originalRaiseExceptions = logging.raiseExceptions
        logging.raiseExceptions = True

        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')

        with mock.patch('traceback.print_exception') as print_exception_mock:
            operator.execute(None)
            logging.raiseExceptions = originalRaiseExceptions
            print_exception_mock.assert_not_called()

    def test_run_image_fails(self):
        self.hook_mock.run_image.side_effect = AirflowException()
        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')
        with pytest.raises(AirflowException):
            operator.execute(None)

    @staticmethod
    def test_on_kill():
        hook_mock = mock.Mock(spec=DockerClientHook)

        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')
        operator.cli_hook = hook_mock

        operator.on_kill()

        hook_mock.stop.assert_called_once_with()

    def test_execute_xcom_behavior(self):
        self.client_mock.pull.return_value = [b'{"status":"pull log"}']

        kwargs = {
            'api_version': '1.19',
            'command': 'env',
            'environment': {'UNIT': 'TEST'},
            'private_environment': {'PRIVATE': 'MESSAGE'},
            'image': 'ubuntu:latest',
            'network_mode': 'bridge',
            'owner': 'unittest',
            'task_id': 'unittest',
            'mounts': [Mount(source='/host/path', target='/container/path', type='bind')],
            'working_dir': '/container/path',
            'shm_size': 1000,
            'host_tmp_dir': '/host/airflow',
            'container_name': 'test_container',
            'tty': True,
        }

        xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=False)
        xcom_all_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=True)
        no_xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=False)

        xcom_push_result = xcom_push_operator.execute(None)
        xcom_all_result = xcom_all_operator.execute(None)
        no_xcom_push_result = no_xcom_push_operator.execute(None)

        assert xcom_push_result == 'container log 2'
        assert xcom_all_result == ['container log 1', 'container log 2']
        assert no_xcom_push_result is None

    def test_execute_xcom_behavior_bytes(self):
        self.client_mock.pull.return_value = [b'{"status":"pull log"}']
        self.client_mock.attach.return_value = [b'container log 1 ', b'container log 2']
        kwargs = {
            'api_version': '1.19',
            'command': 'env',
            'environment': {'UNIT': 'TEST'},
            'private_environment': {'PRIVATE': 'MESSAGE'},
            'image': 'ubuntu:latest',
            'network_mode': 'bridge',
            'owner': 'unittest',
            'task_id': 'unittest',
            'mounts': [Mount(source='/host/path', target='/container/path', type='bind')],
            'working_dir': '/container/path',
            'shm_size': 1000,
            'host_tmp_dir': '/host/airflow',
            'container_name': 'test_container',
            'tty': True,
        }

        xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=False)
        xcom_all_operator = DockerOperator(**kwargs, do_xcom_push=True, xcom_all=True)
        no_xcom_push_operator = DockerOperator(**kwargs, do_xcom_push=False)

        xcom_push_result = xcom_push_operator.execute(None)
        xcom_all_result = xcom_all_operator.execute(None)
        no_xcom_push_result = no_xcom_push_operator.execute(None)

        assert xcom_push_result == 'container log 2'
        assert xcom_all_result == ['container log 1', 'container log 2']
        assert no_xcom_push_result is None
