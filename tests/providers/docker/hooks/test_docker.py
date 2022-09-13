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
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils import db

try:
    from airflow.providers.docker.hooks.docker import DockerHook
except ImportError:
    pass


@mock.patch('airflow.providers.docker.hooks.docker.APIClient', autospec=True)
class TestDockerHook(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='docker_default',
                conn_type='docker',
                host='some.docker.registry.com',
                login='some_user',
                password='some_p4$$w0rd',
            )
        )
        db.merge_conn(
            Connection(
                conn_id='docker_with_extras',
                conn_type='docker',
                host='another.docker.registry.com',
                port=9876,
                login='some_user',
                password='some_p4$$w0rd',
                extra='{"email": "some@example.com", "reauth": "no"}',
            )
        )

    def test_init_fails_when_no_base_url_given(self, _):
        with pytest.raises(AirflowException):
            DockerHook(docker_conn_id='docker_default', version='auto', tls=None)

    def test_init_fails_when_no_api_version_given(self, _):
        with pytest.raises(AirflowException):
            DockerHook(docker_conn_id='docker_default', base_url='unix://var/run/docker.sock', tls=None)

    def test_get_conn_override_defaults(self, docker_client_mock):
        hook = DockerHook(
            docker_conn_id='docker_default',
            base_url='https://index.docker.io/v1/',
            version='1.23',
            tls='someconfig',
            timeout=100,
        )
        hook.get_conn()
        docker_client_mock.assert_called_once_with(
            base_url='https://index.docker.io/v1/',
            version='1.23',
            tls='someconfig',
            timeout=100,
        )

    def test_get_conn_with_standard_config(self, _):
        try:
            hook = DockerHook(
                docker_conn_id='docker_default', base_url='unix://var/run/docker.sock', version='auto'
            )
            client = hook.get_conn()
            assert client is not None
        except Exception:
            self.fail('Could not get connection from Airflow')

    def test_get_conn_with_extra_config(self, _):
        try:
            hook = DockerHook(
                docker_conn_id='docker_with_extras', base_url='unix://var/run/docker.sock', version='auto'
            )
            client = hook.get_conn()
            assert client is not None
        except Exception:
            self.fail('Could not get connection from Airflow')

    def test_conn_with_standard_config_passes_parameters(self, _):
        hook = DockerHook(
            docker_conn_id='docker_default', base_url='unix://var/run/docker.sock', version='auto'
        )
        client = hook.get_conn()
        client.login.assert_called_once_with(
            username='some_user',
            password='some_p4$$w0rd',
            registry='some.docker.registry.com',
            reauth=True,
            email=None,
        )

    def test_conn_with_extra_config_passes_parameters(self, _):
        hook = DockerHook(
            docker_conn_id='docker_with_extras', base_url='unix://var/run/docker.sock', version='auto'
        )
        client = hook.get_conn()
        client.login.assert_called_once_with(
            username='some_user',
            password='some_p4$$w0rd',
            registry='another.docker.registry.com:9876',
            reauth=False,
            email='some@example.com',
        )

    def test_conn_with_broken_config_missing_username_fails(self, _):
        db.merge_conn(
            Connection(
                conn_id='docker_without_username',
                conn_type='docker',
                host='some.docker.registry.com',
                password='some_p4$$w0rd',
                extra='{"email": "some@example.com"}',
            )
        )
        with pytest.raises(AirflowException):
            DockerHook(
                docker_conn_id='docker_without_username',
                base_url='unix://var/run/docker.sock',
                version='auto',
            )

    def test_conn_with_broken_config_missing_host_fails(self, _):
        db.merge_conn(
            Connection(
                conn_id='docker_without_host', conn_type='docker', login='some_user', password='some_p4$$w0rd'
            )
        )
        with pytest.raises(AirflowException):
            DockerHook(
                docker_conn_id='docker_without_host', base_url='unix://var/run/docker.sock', version='auto'
            )
