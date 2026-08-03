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

import logging
import re
from unittest import mock

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.secretmanager_v1.types import AccessSecretVersionResponse

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.secrets.secret_manager import TEAM_SEP, CloudSecretManagerBackend

CREDENTIALS = "test-creds"
KEY_FILE = "test-file.json"
PROJECT_ID = "test-project-id"
OVERRIDDEN_PROJECT_ID = "overridden-test-project-id"
CONNECTIONS_PREFIX = "test-connections"
VARIABLES_PREFIX = "test-variables"
CONFIG_PREFIX = "test-config"
SEP = "-"
CONN_ID = "test-postgres"
CONN_URI = "postgresql://airflow:airflow@host:5432/airflow"
VAR_KEY = "hello"
VAR_VALUE = "world"
CONFIG_KEY = "sql_alchemy_conn"
CONFIG_VALUE = "postgresql://airflow:airflow@host:5432/airflow"

MODULE_NAME = "airflow.providers.google.cloud.secrets.secret_manager"
CLIENT_MODULE_NAME = "airflow.providers.google.cloud._internal_client.secret_manager_client"


class TestCloudSecretManagerBackend:
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_default_valid_and_sep(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        backend = CloudSecretManagerBackend()
        assert backend._is_valid_prefix_and_sep()

    @pytest.mark.parametrize(
        ("prefix", "sep"),
        [
            pytest.param("not:valid", ":", id="colon separator"),
            pytest.param("not/valid", "/", id="backslash separator"),
            pytest.param("a b", "", id="space with char and empty separator"),
            pytest.param(" ", "", id="space only and empty separator"),
        ],
    )
    def test_raise_exception_with_invalid_prefix_sep(self, prefix, sep):
        with pytest.raises(AirflowException):
            CloudSecretManagerBackend(connections_prefix=prefix, sep=sep)

    @pytest.mark.parametrize(
        ("prefix", "sep", "is_valid"),
        [
            pytest.param("valid1", "-", True, id="valid: dash separator"),
            pytest.param("isValid", "_", True, id="valid: underscore separator"),
            pytest.param("", "", True, id="valid: empty string and empty separator"),
            pytest.param("", " ", False, id="invalid: empty string and space separator"),
            pytest.param("not:valid", ":", False, id="invalid: colon separator"),
        ],
    )
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_is_valid_prefix_and_sep(self, mock_client_callable, mock_get_creds, prefix, sep, is_valid):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client
        backend = CloudSecretManagerBackend()
        backend.connections_prefix = prefix
        backend.sep = sep
        assert backend._is_valid_prefix_and_sep() == is_valid

    @pytest.mark.parametrize("connections_prefix", ["airflow-connections", "connections", "airflow"])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_conn_uri(self, mock_client_callable, mock_get_creds, connections_prefix):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = CONN_URI.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretManagerBackend(connections_prefix=connections_prefix)
        secret_id = secrets_manager_backend.build_path(connections_prefix, CONN_ID, SEP)
        returned_uri = secrets_manager_backend.get_conn_value(conn_id=CONN_ID)
        assert returned_uri == CONN_URI
        mock_client.secret_version_path.assert_called_once_with(PROJECT_ID, secret_id, "latest")

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + ".CloudSecretManagerBackend.get_conn_value")
    def test_get_connection(self, mock_get_value, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_get_value.return_value = CONN_URI
        conn = CloudSecretManagerBackend().get_connection(conn_id=CONN_ID)
        assert isinstance(conn, Connection)

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_conn_uri_non_existent_key(self, mock_client_callable, mock_get_creds, caplog):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client
        # The requested secret id or secret version does not exist
        mock_client.access_secret_version.side_effect = NotFound("test-msg")

        secrets_manager_backend = CloudSecretManagerBackend(connections_prefix=CONNECTIONS_PREFIX)
        secret_id = secrets_manager_backend.build_path(CONNECTIONS_PREFIX, CONN_ID, SEP)
        with caplog.at_level(level=logging.DEBUG, logger=secrets_manager_backend.client.log.name):
            assert secrets_manager_backend.get_conn_value(conn_id=CONN_ID) is None
            assert secrets_manager_backend.get_connection(conn_id=CONN_ID) is None
            assert re.search(
                f"Google Cloud API Call Error \\(NotFound\\): Secret ID {secret_id} not found",
                caplog.messages[0],
            )

    @pytest.mark.parametrize("variables_prefix", ["airflow-variables", "variables", "airflow"])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_variable(self, mock_client_callable, mock_get_creds, variables_prefix):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = VAR_VALUE.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretManagerBackend(variables_prefix=variables_prefix)
        secret_id = secrets_manager_backend.build_path(variables_prefix, VAR_KEY, SEP)
        returned_uri = secrets_manager_backend.get_variable(VAR_KEY)
        assert returned_uri == VAR_VALUE
        mock_client.secret_version_path.assert_called_once_with(PROJECT_ID, secret_id, "latest")

    @pytest.mark.parametrize("config_prefix", ["airflow-config", "config", "airflow"])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_config(self, mock_client_callable, mock_get_creds, config_prefix):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = CONFIG_VALUE.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretManagerBackend(config_prefix=config_prefix)
        secret_id = secrets_manager_backend.build_path(config_prefix, CONFIG_KEY, SEP)
        returned_val = secrets_manager_backend.get_config(CONFIG_KEY)
        assert returned_val == CONFIG_VALUE
        mock_client.secret_version_path.assert_called_once_with(PROJECT_ID, secret_id, "latest")

    @pytest.mark.parametrize("variables_prefix", ["airflow-variables", "variables", "airflow"])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_variable_override_project_id(self, mock_client_callable, mock_get_creds, variables_prefix):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = VAR_VALUE.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretManagerBackend(
            variables_prefix=variables_prefix, project_id=OVERRIDDEN_PROJECT_ID
        )
        secret_id = secrets_manager_backend.build_path(variables_prefix, VAR_KEY, SEP)
        returned_uri = secrets_manager_backend.get_variable(VAR_KEY)
        assert returned_uri == VAR_VALUE
        mock_client.secret_version_path.assert_called_once_with(OVERRIDDEN_PROJECT_ID, secret_id, "latest")

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_variable_non_existent_key(self, mock_client_callable, mock_get_creds, caplog):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client
        # The requested secret id or secret version does not exist
        mock_client.access_secret_version.side_effect = NotFound("test-msg")

        secrets_manager_backend = CloudSecretManagerBackend(variables_prefix=VARIABLES_PREFIX)
        secret_id = secrets_manager_backend.build_path(VARIABLES_PREFIX, VAR_KEY, SEP)
        with caplog.at_level(level=logging.DEBUG, logger=secrets_manager_backend.client.log.name):
            assert secrets_manager_backend.get_variable(VAR_KEY) is None
            assert re.search(
                f"Google Cloud API Call Error \\(NotFound\\): Secret ID {secret_id} not found",
                caplog.messages[0],
            )

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_connections_prefix_none_value(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        with mock.patch(MODULE_NAME + ".CloudSecretManagerBackend._get_secret") as mock_get_secret:
            with mock.patch(
                MODULE_NAME + ".CloudSecretManagerBackend._is_valid_prefix_and_sep"
            ) as mock_is_valid_prefix_sep:
                secrets_manager_backend = CloudSecretManagerBackend(connections_prefix=None)

                mock_is_valid_prefix_sep.assert_not_called()
                assert secrets_manager_backend.get_conn_value(conn_id=CONN_ID) is None
                mock_get_secret.assert_not_called()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_variables_prefix_none_value(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        with mock.patch(MODULE_NAME + ".CloudSecretManagerBackend._get_secret") as mock_get_secret:
            secrets_manager_backend = CloudSecretManagerBackend(variables_prefix=None)

            assert secrets_manager_backend.get_variable(VAR_KEY) is None
            mock_get_secret.assert_not_called()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_config_prefix_none_value(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        with mock.patch(MODULE_NAME + ".CloudSecretManagerBackend._get_secret") as mock_get_secret:
            secrets_manager_backend = CloudSecretManagerBackend(config_prefix=None)

            assert secrets_manager_backend.get_config(CONFIG_KEY) is None
            mock_get_secret.assert_not_called()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_init_raises_when_project_id_not_determined(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, ""
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        with pytest.raises(ValueError, match="Project ID could not be determined"):
            CloudSecretManagerBackend()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_init_succeeds_with_explicit_project_id(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, ""
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        backend = CloudSecretManagerBackend(project_id="explicit-project")
        assert backend.project_id == "explicit-project"


class TestCloudSecretManagerBackendTeamScope:
    """A team scoped secret must only be resolvable for the team it is stored for."""

    TEAM = "team_a"
    OTHER_TEAM = "team_b"
    # A team name may itself contain the team separator, so one team's namespace can start
    # with another's. Treating a prefix match as ownership is what this guards against.
    EXTENDING_TEAM = "team_a--prod"

    @staticmethod
    def _backend(mock_client_callable):
        """
        Build a backend over an in-memory secret store keyed by full secret name.

        The store is returned rather than taken as an argument so tests can populate it after
        building the backend -- the lookup closes over it by reference. Names are derived from
        the backend, so it has to exist first.
        """
        store: dict[str, str] = {}
        client = mock.MagicMock()
        client.get_secret.side_effect = lambda secret_id, project_id, **kwargs: store.get(secret_id)
        mock_client_callable.return_value = client
        backend = CloudSecretManagerBackend(
            connections_prefix=CONNECTIONS_PREFIX,
            variables_prefix=VARIABLES_PREFIX,
            config_prefix=CONFIG_PREFIX,
            project_id=PROJECT_ID,
        )
        return backend, store

    @staticmethod
    def _team_name(backend, team, secret_id, prefix=CONNECTIONS_PREFIX):
        return backend._build_team_secret_name(prefix, team, secret_id)

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_team_scoped_secret_is_resolved_for_its_own_team(self, mock_client, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, store = self._backend(mock_client)
        store[self._team_name(backend, self.TEAM, CONN_ID)] = CONN_URI

        assert backend.get_conn_value(conn_id=CONN_ID, team_name=self.TEAM) == CONN_URI

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_team_scoped_secret_is_not_resolved_for_another_team(self, mock_client, mock_get_creds):
        """The whole point: team B must not reach team A's secret by spelling out its name."""
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, store = self._backend(mock_client)
        encoded = f"{self.TEAM}--{CONN_ID}"
        store[self._team_name(backend, self.TEAM, CONN_ID)] = CONN_URI

        assert backend.get_conn_value(conn_id=encoded, team_name=self.OTHER_TEAM) is None

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_team_scoped_secret_is_not_resolved_without_a_team_scope(self, mock_client, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, store = self._backend(mock_client)
        encoded = f"{self.TEAM}--{CONN_ID}"
        store[self._team_name(backend, self.TEAM, CONN_ID)] = CONN_URI

        assert backend.get_conn_value(conn_id=encoded) is None

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_team_whose_name_extends_the_callers_is_not_readable(self, mock_client, mock_get_creds):
        """A prefix match on the caller's namespace is not proof of ownership."""
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, store = self._backend(mock_client)
        store[self._team_name(backend, self.EXTENDING_TEAM, CONN_ID)] = CONN_URI
        encoded = f"{self.EXTENDING_TEAM}--{CONN_ID}"

        assert backend.get_conn_value(conn_id=encoded, team_name=self.TEAM) is None
        # and the owning team still reaches it normally, with the bare id
        assert backend.get_conn_value(conn_id=CONN_ID, team_name=self.EXTENDING_TEAM) == CONN_URI

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_team_agnostic_secret_is_resolved_for_any_team_scope(self, mock_client, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, store = self._backend(mock_client)
        store[backend.build_path(CONNECTIONS_PREFIX, CONN_ID, SEP)] = CONN_URI

        assert backend.get_conn_value(conn_id=CONN_ID) == CONN_URI
        assert backend.get_conn_value(conn_id=CONN_ID, team_name=self.TEAM) == CONN_URI

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_team_scoped_variable_is_not_resolved_for_another_team(self, mock_client, mock_get_creds):
        """Variables share the lookup, so they must be scoped identically."""
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, store = self._backend(mock_client)
        encoded = f"{self.TEAM}--{VAR_KEY}"
        store[self._team_name(backend, self.TEAM, VAR_KEY, VARIABLES_PREFIX)] = VAR_VALUE

        assert backend.get_variable(key=VAR_KEY, team_name=self.TEAM) == VAR_VALUE
        assert backend.get_variable(key=encoded, team_name=self.OTHER_TEAM) is None

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_team_secret_name_is_built_literally(self, mock_client, mock_get_creds):
        """
        Pin the literal name rather than deriving it from the method under test.

        Every other test in this class keys its store with ``_build_team_secret_name``, so it
        agrees with the implementation by construction and cannot see a naming defect.
        """
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, _ = self._backend(mock_client)

        assert (
            backend._build_team_secret_name(CONNECTIONS_PREFIX, "team_a", "test_postgres")
            == f"{CONNECTIONS_PREFIX}{SEP}team_a{TEAM_SEP}test_postgres"
        )
        # Underscores survive in the secret id, matching the team agnostic name this backend
        # documents -- unlike Azure, nothing here normalises ``_`` to the separator.
        assert (
            backend._build_team_secret_name(CONNECTIONS_PREFIX, "team_a", "smtp_default")
            == f"{CONNECTIONS_PREFIX}{SEP}team_a{TEAM_SEP}smtp_default"
        )

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_underscores_cannot_manufacture_a_team_namespace(self, mock_client, mock_get_creds):
        """
        An id containing ``__`` must not resolve into a team whose name ends with that segment.

        Normalising ``_`` to the separator when building the team scoped name (as the Azure
        backend does) made team ``team_a`` asking for ``prod__conn`` build exactly the name
        team ``team_a--prod`` stores ``conn`` under.
        """
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, store = self._backend(mock_client)
        victim = backend._build_team_secret_name(CONNECTIONS_PREFIX, self.EXTENDING_TEAM, CONN_ID)
        store[victim] = CONN_URI

        assert backend.get_conn_value(conn_id=f"prod__{CONN_ID}", team_name=self.TEAM) is None
        # the two names must not coincide in the first place
        assert backend._build_team_secret_name(CONNECTIONS_PREFIX, self.TEAM, f"prod__{CONN_ID}") != victim

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_ambiguous_id_is_refused_even_for_its_own_team(self, mock_client, mock_get_creds):
        """
        An id containing the separator is ambiguous in both directions, so it is never resolved.

        ``team_a`` with id ``b--c`` and ``team_a--b`` with id ``c`` name the same secret, so
        honouring the id for its own team would still hand one team the other's secret.
        """
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, store = self._backend(mock_client)
        ambiguous = f"prod{TEAM_SEP}{CONN_ID}"
        store[backend._build_team_secret_name(CONNECTIONS_PREFIX, self.TEAM, ambiguous)] = CONN_URI

        assert backend.get_conn_value(conn_id=ambiguous, team_name=self.TEAM) is None

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_refusing_an_ambiguous_id_is_logged(self, mock_client, mock_get_creds, caplog):
        """A silent ``None`` is indistinguishable from a missing secret, so the refusal is logged."""
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, _ = self._backend(mock_client)
        ambiguous = f"prod{TEAM_SEP}{CONN_ID}"

        assert backend.get_conn_value(conn_id=ambiguous) is None
        assert backend.get_variable(key=ambiguous) is None

        refusals = [r for r in caplog.records if "is ambiguous and is not looked up" in r.getMessage()]
        assert len(refusals) == 2
        assert all(r.levelname == "WARNING" for r in refusals)
        assert all(ambiguous in r.getMessage() for r in refusals)

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + "._SecretManagerClient")
    def test_config_lookup_is_not_team_scoped(self, mock_client, mock_get_creds):
        """
        ``get_config`` has no team scope, so the namespace guard must not apply to it.

        Refusing config keys containing the separator would be stricter than the Azure backend
        this convention comes from, and there is no team namespace for a config key to invade.
        """
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        backend, store = self._backend(mock_client)
        key = f"some{TEAM_SEP}option"
        store[backend.build_path(CONFIG_PREFIX, key, SEP)] = "config-value"

        assert backend.get_config(key) == "config-value"
