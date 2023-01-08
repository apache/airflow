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

from unittest import mock

import pytest

from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection
from airflow.providers.docker.protocols.docker_registry import (
    AirflowConnectionDockerRegistryAuth,
    DockerRegistryAuthProtocol,
    DockerRegistryCredentials,
    NoDockerRegistryAuth,
    RefreshableDockerRegistryAuthProtocol,
)

DUMMY_CONNECTIONS = [
    pytest.param(None, id="none"),
    pytest.param(Connection(), id="connection-object"),
    pytest.param(mock.ANY, id="mock.ANY"),
]
TEST_CONN = {"host": "some.docker.registry.com", "login": "some_user", "password": "some_p4$$w0rd"}


class FakeDockerRegistryAuth(DockerRegistryAuthProtocol):
    """This Fake Docker Registry Auth might use into other docker tests."""

    def __init__(self, *, registries: int = 1):
        super().__init__(self)
        self.mocked_credentials = [
            mock.MagicMock(name=f"Mock Credentials #{ix:02d}", autospec=DockerRegistryCredentials)
            for ix in range(registries)
        ]

    def get_credentials(self, conn):
        return self.mocked_credentials


class FakeRefreshableDockerRegistryAuth(FakeDockerRegistryAuth, RefreshableDockerRegistryAuthProtocol):
    """This Fake Docker Registry Auth might use into other docker tests."""

    need_refresh = True

    def __init__(self, *, registries: int = 1):
        super().__init__(registries=registries)
        self.mocked_refreshed_credentials = [
            mock.MagicMock(name=f"Mock Refreshed Credentials #{ix:02d}", autospec=DockerRegistryCredentials)
            for ix in range(registries)
        ]

    def refresh_credentials(self, conn: Connection | None) -> list[DockerRegistryCredentials]:
        return self.mocked_refreshed_credentials


@mock.patch("airflow.providers.docker.protocols.docker_registry.mask_secret")
class TestDockerRegistryCredentials:
    def test_mask_secrets(self, mock_masker: mock.MagicMock):
        """Test masking Docker Registry password."""
        DockerRegistryCredentials(username="foo", password="bar", registry="spam.egg")
        mock_masker.assert_called_once_with("bar")


class BaseDockerRegistryAuthTest:
    """Base Test class for classes which implements ``DockerRegistryAuthProtocol``."""

    class_type: DockerRegistryAuthProtocol
    default_kwargs: dict

    @pytest.fixture(autouse=True, scope="class")
    def setup_base_test(self, request):
        assert hasattr(self, "class_type")
        if not hasattr(self, "default_kwargs"):
            request.cls.default_kwargs = {}

    def test_is_instance_of_protocol(self):
        """Test is self.class_type is instance of DockerRegistryAuthProtocol."""
        assert isinstance(self.class_type, DockerRegistryAuthProtocol)


class TestNoDockerRegistryAuth:
    """Test NoDockerRegistryAuth class."""

    @pytest.mark.parametrize("conn", DUMMY_CONNECTIONS)
    def test_get_credentials(self, conn):
        """Test that ``NoDockerRegistryAuth.get_credential`` method always return zero-length list."""
        assert NoDockerRegistryAuth().get_credentials(conn=conn) == []


class TestAirflowConnectionDockerRegistryAuth(BaseDockerRegistryAuthTest):
    """Test AirflowConnectionDockerRegistryAuth class."""

    class_type = AirflowConnectionDockerRegistryAuth

    @pytest.mark.parametrize(
        "conn_params, expected_creds",
        [
            pytest.param(
                {"host": "some.docker.registry.com", "login": "some_user", "password": "some_p4$$w0rd"},
                DockerRegistryCredentials(
                    username="some_user",
                    password="some_p4$$w0rd",
                    registry="some.docker.registry.com",
                    email=None,
                    reauth=True,
                ),
                id="host-login-password",
            ),
            pytest.param(
                {
                    "host": "another.docker.registry.com",
                    "login": "another_user",
                    "password": "insecure_password",
                    "extra": {"email": "foo@bar.spam.egg", "reauth": "no"},
                },
                DockerRegistryCredentials(
                    username="another_user",
                    password="insecure_password",
                    registry="another.docker.registry.com",
                    email="foo@bar.spam.egg",
                    reauth=False,
                ),
                id="host-login-password-email-noreauth",
            ),
            pytest.param(
                {
                    "host": "localhost",
                    "port": 8080,
                    "login": "user",
                    "password": "pass",
                    "extra": {"email": "", "reauth": "TrUe"},
                },
                DockerRegistryCredentials(
                    username="user",
                    password="pass",
                    registry="localhost:8080",
                    email=None,
                    reauth=True,
                ),
                id="host-port-login-password-reauth",
            ),
        ],
    )
    def test_get_credentials(self, conn_params, expected_creds):
        fake_connection = Connection(conn_id="test-connection", **conn_params)
        result = AirflowConnectionDockerRegistryAuth().get_credentials(conn=fake_connection)
        assert len(result) == 1, "AirflowConnectionDockerRegistryAuth expected exactly one connection"
        creds = result[0]
        assert creds == expected_creds

    @pytest.mark.parametrize(
        "conn_params, ex, error_message",
        [
            pytest.param(
                {k: v for k, v in TEST_CONN.items() if k != "login"},
                AirflowNotFoundException,
                r"No Docker Registry username provided\.",
                id="missing-username",
            ),
            pytest.param(
                {k: v for k, v in TEST_CONN.items() if k != "host"},
                AirflowNotFoundException,
                r"No Docker Registry URL provided\.",
                id="missing-registry-host",
            ),
            pytest.param(
                {**TEST_CONN, **{"extra": {"reauth": "enabled"}}},
                ValueError,
                r"Unable parse `reauth` value '.*' to bool\.",
                id="wrong-reauth",
            ),
            pytest.param(
                {**TEST_CONN, **{"extra": {"reauth": "disabled"}}},
                ValueError,
                r"Unable parse `reauth` value '.*' to bool\.",
                id="wrong-noreauth",
            ),
        ],
    )
    def test_invalid_conn_parameters(self, conn_params, ex, error_message):
        """Test invalid/missing connection parameters."""
        fake_connection = Connection(conn_id="test-connection", **conn_params)
        with pytest.raises(ex, match=error_message):
            AirflowConnectionDockerRegistryAuth().get_credentials(conn=fake_connection)

    def test_no_connection_provided(self):
        with pytest.raises(AirflowNotFoundException, match="Docker Connection not set"):
            AirflowConnectionDockerRegistryAuth().get_credentials(conn=None)


class TestFakeDockerRegistryAuth(BaseDockerRegistryAuthTest):
    """Test FakeDockerRegistryAuth class."""

    class_type = FakeDockerRegistryAuth

    @pytest.mark.parametrize("conn", DUMMY_CONNECTIONS)
    def test_fake_docker_registry(self, conn):
        dra = FakeDockerRegistryAuth(registries=0)
        credentials = dra.get_credentials(conn)
        assert credentials is dra.mocked_credentials
        assert len(FakeDockerRegistryAuth(registries=5).get_credentials(conn)) == 5


class TestFakeRefreshableDockerRegistryAuth(TestFakeDockerRegistryAuth):
    """Test FakeRefreshableDockerRegistryAuth class."""

    class_type = FakeRefreshableDockerRegistryAuth

    @pytest.mark.parametrize("conn", DUMMY_CONNECTIONS)
    def test_fake_refresh_docker_registry(self, conn):
        dra = FakeRefreshableDockerRegistryAuth(registries=1)
        refreshed_credentials = dra.refresh_credentials(conn)
        assert refreshed_credentials is dra.mocked_refreshed_credentials
        assert len(FakeRefreshableDockerRegistryAuth(registries=42).refresh_credentials(conn)) == 42
