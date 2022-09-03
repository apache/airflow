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

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.docker.credentials.base import DockerLoginCredentials
from airflow.providers.docker.credentials.connection import AirflowConnectionDockerCredentialHelper


class TestAirflowConnectionDockerCredentialHelper:
    def test_conn_with_broken_config_missing_host_fails(self):
        test_conn = Connection(
            conn_id='docker_without_host', conn_type='docker', login='some_user', password='some_p4$$w0rd'
        )
        ch = AirflowConnectionDockerCredentialHelper(conn=test_conn)
        error_message = r"No Docker URL provided\."
        with pytest.raises(AirflowException, match=error_message):
            ch.get_credentials()

    def test_conn_with_broken_config_missing_username_fails(self):
        test_conn = Connection(
            conn_id='docker_without_username',
            conn_type='docker',
            host='some.docker.registry.com',
            password='some_p4$$w0rd',
        )
        ch = AirflowConnectionDockerCredentialHelper(conn=test_conn)
        error_message = r"No username provided\."
        with pytest.raises(AirflowException, match=error_message):
            ch.get_credentials()

    @pytest.mark.parametrize("port", [None, 42, "42"])
    @pytest.mark.parametrize("password", [None, "p@$$w0rd"])
    @pytest.mark.parametrize("email", ["John.Doe@apache.org", None])
    @pytest.mark.parametrize("reauth", [True, False])
    def test_credentials(self, password, port, email, reauth):
        test_conn_kwargs = {
            "host": "some.docker.registry.com",
            "login": "some_user",
            "extra": {"reauth": reauth},
        }
        if port:
            test_conn_kwargs["port"] = port
        if password:
            test_conn_kwargs["password"] = password
        if email:
            test_conn_kwargs["extra"]["email"] = email
        test_conn = Connection(conn_id='test_docker_conn', conn_type='docker', **test_conn_kwargs)
        ch = AirflowConnectionDockerCredentialHelper(conn=test_conn)
        creds = ch.get_credentials()
        assert len(creds) == 1
        credentials = creds[0]
        assert isinstance(credentials, DockerLoginCredentials)
        assert credentials.username == test_conn_kwargs["login"]
        assert credentials.password == password
        assert credentials.registry == (
            f"{test_conn_kwargs['host']}:{port}" if port else test_conn_kwargs["host"]
        )
        assert credentials.email == email
        assert credentials.reauth == reauth
