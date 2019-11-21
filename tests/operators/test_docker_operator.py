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

import logging
import unittest

from airflow.exceptions import AirflowException
from tests.compat import mock

try:
    from airflow.operators.docker_operator import DockerOperator
    from airflow.hooks.docker_hook import DockerHook
    from docker import APIClient
except ImportError:
    pass


class TestDockerOperator(unittest.TestCase):
    @mock.patch('airflow.utils.file.mkdtemp')
    @mock.patch('airflow.operators.docker_operator.APIClient')
    def test_execute(self, client_class_mock, mkdtemp_mock):
        host_config = mock.Mock()
        mkdtemp_mock.return_value = '/mkdtemp'

        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_container.return_value = {'Id': 'some_id'}
        client_mock.create_host_config.return_value = host_config
        client_mock.images.return_value = []
        client_mock.attach.return_value = ['container log']
        client_mock.logs.return_value = ['container log']
        client_mock.pull.return_value = [b'{"status":"pull log"}']
        client_mock.wait.return_value = {"StatusCode": 0}

        client_class_mock.return_value = client_mock

        operator = DockerOperator(api_version='1.19', command='env', environment={'UNIT': 'TEST'},
                                  image='ubuntu:latest', network_mode='bridge', owner='unittest',
                                  task_id='unittest', volumes=['/host/path:/container/path'],
                                  working_dir='/container/path', shm_size=1000,
                                  host_tmp_dir='/host/airflow', container_name='test_container',
                                  tty=True)
        operator.execute(None)

        client_class_mock.assert_called_once_with(base_url='unix://var/run/docker.sock', tls=None,
                                                  version='1.19')

        client_mock.create_container.assert_called_once_with(command='env',
                                                             name='test_container',
                                                             environment={
                                                                 'AIRFLOW_TMP_DIR': '/tmp/airflow',
                                                                 'UNIT': 'TEST'
                                                             },
                                                             host_config=host_config,
                                                             image='ubuntu:latest',
                                                             user=None,
                                                             working_dir='/container/path',
                                                             tty=True
                                                             )
        client_mock.create_host_config.assert_called_once_with(binds=['/host/path:/container/path',
                                                                      '/mkdtemp:/tmp/airflow'],
                                                               network_mode='bridge',
                                                               shm_size=1000,
                                                               cpu_shares=1024,
                                                               mem_limit=None,
                                                               auto_remove=False,
                                                               dns=None,
                                                               dns_search=None)
        mkdtemp_mock.assert_called_once_with(dir='/host/airflow', prefix='airflowtmp', suffix='')
        client_mock.images.assert_called_once_with(name='ubuntu:latest')
        client_mock.attach.assert_called_once_with(container='some_id', stdout=True,
                                                   stderr=True, stream=True)
        client_mock.pull.assert_called_once_with('ubuntu:latest', stream=True)
        client_mock.wait.assert_called_once_with('some_id')

    @mock.patch('airflow.operators.docker_operator.tls.TLSConfig')
    @mock.patch('airflow.operators.docker_operator.APIClient')
    def test_execute_tls(self, client_class_mock, tls_class_mock):
        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_container.return_value = {'Id': 'some_id'}
        client_mock.create_host_config.return_value = mock.Mock()
        client_mock.images.return_value = []
        client_mock.attach.return_value = []
        client_mock.pull.return_value = []
        client_mock.wait.return_value = {"StatusCode": 0}

        client_class_mock.return_value = client_mock
        tls_mock = mock.Mock()
        tls_class_mock.return_value = tls_mock

        operator = DockerOperator(docker_url='tcp://127.0.0.1:2376', image='ubuntu',
                                  owner='unittest', task_id='unittest', tls_client_cert='cert.pem',
                                  tls_ca_cert='ca.pem', tls_client_key='key.pem')
        operator.execute(None)

        tls_class_mock.assert_called_once_with(assert_hostname=None, ca_cert='ca.pem',
                                               client_cert=('cert.pem', 'key.pem'),
                                               ssl_version=None, verify=True)

        client_class_mock.assert_called_once_with(base_url='https://127.0.0.1:2376',
                                                  tls=tls_mock, version=None)

    @mock.patch('airflow.operators.docker_operator.APIClient')
    def test_execute_unicode_logs(self, client_class_mock):
        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_container.return_value = {'Id': 'some_id'}
        client_mock.create_host_config.return_value = mock.Mock()
        client_mock.images.return_value = []
        client_mock.attach.return_value = ['unicode container log 😁']
        client_mock.pull.return_value = []
        client_mock.wait.return_value = {"StatusCode": 0}

        client_class_mock.return_value = client_mock

        originalRaiseExceptions = logging.raiseExceptions  # pylint: disable=invalid-name
        logging.raiseExceptions = True

        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')

        with mock.patch('traceback.print_exception') as print_exception_mock:
            operator.execute(None)
            logging.raiseExceptions = originalRaiseExceptions
            print_exception_mock.assert_not_called()

    @mock.patch('airflow.operators.docker_operator.APIClient')
    def test_execute_container_fails(self, client_class_mock):
        client_mock = mock.Mock(spec=APIClient)
        client_mock.create_container.return_value = {'Id': 'some_id'}
        client_mock.create_host_config.return_value = mock.Mock()
        client_mock.images.return_value = []
        client_mock.attach.return_value = []
        client_mock.pull.return_value = []
        client_mock.wait.return_value = {"StatusCode": 1}

        client_class_mock.return_value = client_mock

        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')

        with self.assertRaises(AirflowException):
            operator.execute(None)

    @staticmethod
    def test_on_kill():
        client_mock = mock.Mock(spec=APIClient)

        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')
        operator.cli = client_mock
        operator.container = {'Id': 'some_id'}

        operator.on_kill()

        client_mock.stop.assert_called_once_with('some_id')

    @mock.patch('airflow.operators.docker_operator.APIClient')
    def test_execute_no_docker_conn_id_no_hook(self, operator_client_mock):
        # Mock out a Docker client, so operations don't raise errors
        client_mock = mock.Mock(name='DockerOperator.APIClient mock', spec=APIClient)
        client_mock.images.return_value = []
        client_mock.create_container.return_value = {'Id': 'some_id'}
        client_mock.attach.return_value = []
        client_mock.pull.return_value = []
        client_mock.wait.return_value = {"StatusCode": 0}
        operator_client_mock.return_value = client_mock

        # Create the DockerOperator
        operator = DockerOperator(
            image='publicregistry/someimage',
            owner='unittest',
            task_id='unittest'
        )

        # Mock out the DockerHook
        hook_mock = mock.Mock(name='DockerHook mock', spec=DockerHook)
        hook_mock.get_conn.return_value = client_mock
        operator.get_hook = mock.Mock(
            name='DockerOperator.get_hook mock',
            spec=DockerOperator.get_hook,
            return_value=hook_mock
        )

        operator.execute(None)
        self.assertEqual(
            operator.get_hook.call_count, 0,
            'Hook called though no docker_conn_id configured'
        )

    @mock.patch('airflow.operators.docker_operator.DockerHook')
    @mock.patch('airflow.operators.docker_operator.APIClient')
    def test_execute_with_docker_conn_id_use_hook(self, operator_client_mock,
                                                  operator_docker_hook):
        # Mock out a Docker client, so operations don't raise errors
        client_mock = mock.Mock(name='DockerOperator.APIClient mock', spec=APIClient)
        client_mock.images.return_value = []
        client_mock.create_container.return_value = {'Id': 'some_id'}
        client_mock.attach.return_value = []
        client_mock.pull.return_value = []
        client_mock.wait.return_value = {"StatusCode": 0}
        operator_client_mock.return_value = client_mock

        # Create the DockerOperator
        operator = DockerOperator(
            image='publicregistry/someimage',
            owner='unittest',
            task_id='unittest',
            docker_conn_id='some_conn_id'
        )

        # Mock out the DockerHook
        hook_mock = mock.Mock(name='DockerHook mock', spec=DockerHook)
        hook_mock.get_conn.return_value = client_mock
        operator_docker_hook.return_value = hook_mock

        operator.execute(None)

        self.assertEqual(
            operator_client_mock.call_count, 0,
            'Client was called on the operator instead of the hook'
        )
        self.assertEqual(
            operator_docker_hook.call_count, 1,
            'Hook was not called although docker_conn_id configured'
        )
        self.assertEqual(
            client_mock.pull.call_count, 1,
            'Image was not pulled using operator client'
        )


if __name__ == "__main__":
    unittest.main()
