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
from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.secrets.secrets_manager import TEAM_SEP, SecretsManagerBackend

from tests_common.test_utils.config import conf_vars

multi_team_enabled = conf_vars({("core", "multi_team"): "True"})


class TestSecretsManagerBackend:
    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend.get_conn_value")
    def test_aws_secrets_manager_get_connection(self, mock_get_value):
        mock_get_value.return_value = "scheme://user:pass@host:100"
        conn = SecretsManagerBackend().get_connection("fake_conn")
        assert conn.host == "host"

    @mock_aws
    def test_get_conn_value_full_url_mode(self):
        secret_id = "airflow/connections/test_postgres"
        create_param = {
            "Name": secret_id,
            "SecretString": "postgresql://airflow:airflow@host:5432/airflow",
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        returned_uri = secrets_manager_backend.get_conn_value(conn_id="test_postgres")
        assert returned_uri == "postgresql://airflow:airflow@host:5432/airflow"

    @mock_aws
    def test_get_conn_value_non_existent_key(self):
        """
        Test that if the key with connection ID is not present,
        SecretsManagerBackend.get_connection should return None
        """
        conn_id = "test_mysql"

        secret_id = "airflow/connections/test_postgres"
        create_param = {
            "Name": secret_id,
            "SecretString": "postgresql://airflow:airflow@host:5432/airflow",
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        assert secrets_manager_backend.get_conn_value(conn_id=conn_id) is None
        assert secrets_manager_backend.get_connection(conn_id=conn_id) is None

    @mock_aws
    def test_get_conn_value_with_team_name(self):
        secret_id = "airflow/connections/my_team--test_postgres"
        create_param = {
            "Name": secret_id,
            "SecretString": "postgresql://airflow:airflow@host:5432/airflow",
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        returned_uri = secrets_manager_backend.get_conn_value(conn_id="test_postgres", team_name="my_team")
        assert returned_uri == "postgresql://airflow:airflow@host:5432/airflow"

    @multi_team_enabled
    @mock_aws
    def test_global_caller_cannot_access_team_scoped_connection(self):
        secret_id = "airflow/connections/my_team--test_postgres"
        create_param = {
            "Name": secret_id,
            "SecretString": "postgresql://airflow:airflow@host:5432/airflow",
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        assert secrets_manager_backend.get_conn_value(conn_id="my_team--test_postgres") is None

    @multi_team_enabled
    @mock_aws
    def test_another_teams_secret_is_not_reachable(self):
        """A caller scoped to one team must not reach another team's secret by naming it."""
        secret_id = "airflow/connections/my_team--test_postgres"
        create_param = {"Name": secret_id, "SecretString": "postgresql://airflow:airflow@host:5432/airflow"}
        backend = SecretsManagerBackend()
        backend.client.create_secret(**create_param)

        assert backend.get_conn_value(conn_id="my_team--test_postgres", team_name="other_team") is None

    @multi_team_enabled
    @mock_aws
    def test_team_whose_name_extends_the_callers_is_not_reachable(self):
        """A prefix match on the caller's own namespace is not proof of ownership."""
        secret_id = "airflow/connections/my_team--prod--test_postgres"
        create_param = {"Name": secret_id, "SecretString": "postgresql://airflow:airflow@host:5432/airflow"}
        backend = SecretsManagerBackend()
        backend.client.create_secret(**create_param)

        assert backend.get_conn_value(conn_id="my_team--prod--test_postgres", team_name="my_team") is None

    @multi_team_enabled
    @mock_aws
    def test_team_scoped_lookup_cannot_reach_a_longer_teams_namespace(self):
        """The team scoped name is not safe by construction -- the id can extend it.

        Team ``my_team`` asking for ``prod--test_postgres`` builds exactly the name team
        ``my_team--prod`` builds for ``test_postgres``, so the team scoped lookup *hits*
        another team's secret. Refusing only the team agnostic fall-through leaves this
        open, because the fall-through is never reached.
        """
        secret_id = "airflow/connections/my_team--prod--test_postgres"
        create_param = {"Name": secret_id, "SecretString": "postgresql://airflow:airflow@host:5432/airflow"}
        backend = SecretsManagerBackend()
        backend.client.create_secret(**create_param)

        assert backend.get_conn_value(conn_id="prod--test_postgres", team_name="my_team") is None

    @multi_team_enabled
    @mock_aws
    def test_refusing_an_ambiguous_id_is_logged(self, caplog):
        """A silent ``None`` is indistinguishable from a missing secret, so the refusal is logged.

        Asserted on the record's structured ``args`` rather than the rendered message, so
        rewording the warning does not silently stop this from testing anything.
        """
        backend = SecretsManagerBackend()

        assert backend.get_conn_value(conn_id="prod--test_postgres") is None
        assert backend.get_variable(key="prod--hello") is None
        assert backend.get_config(key="prod--sql_alchemy_conn") is None

        # ``getMessage()`` rather than ``msg``: how a record carries its payload depends on the
        # Airflow version. Here structlog renders the format args into ``msg`` before the stdlib
        # record exists, so ``args`` is empty; on the versions the provider compat tests run
        # against, plain stdlib logging leaves ``msg`` as the format string with the values in
        # ``args``. ``getMessage()`` renders in both. Assert on level, logger and the refused id
        # (the load-bearing data) rather than the wording, so rephrasing the warning is free.
        refusals = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING and r.name.endswith(type(backend).__name__)
        ]
        assert len(refusals) == 3
        for refused_id in ("prod--test_postgres", "prod--hello", "prod--sql_alchemy_conn"):
            assert sum(refused_id in r.getMessage() for r in refusals) == 1
        assert all(TEAM_SEP in r.getMessage() for r in refusals)

    @mock_aws
    def test_ambiguous_id_resolves_when_multi_team_is_disabled(self):
        """No team scoped secret can exist without multi-team mode, so there is no ambiguity
        to refuse -- an ordinary id containing the separator must resolve normally."""
        secret_id = "airflow/connections/prod--test_postgres"
        create_param = {
            "Name": secret_id,
            "SecretString": "postgresql://airflow:airflow@host:5432/airflow",
        }
        backend = SecretsManagerBackend()
        backend.client.create_secret(**create_param)

        assert backend.get_conn_value(conn_id="prod--test_postgres") == (
            "postgresql://airflow:airflow@host:5432/airflow"
        )

    @mock_aws
    def test_team_caller_falls_back_to_global_connection(self):
        secret_id = "airflow/connections/test_postgres"
        create_param = {
            "Name": secret_id,
            "SecretString": "postgresql://airflow:airflow@host:5432/airflow",
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        returned_uri = secrets_manager_backend.get_conn_value(
            conn_id="test_postgres", team_name="non_existent_team"
        )
        assert returned_uri == "postgresql://airflow:airflow@host:5432/airflow"

    @mock_aws
    def test_get_variable(self):
        secret_id = "airflow/variables/hello"
        create_param = {"Name": secret_id, "SecretString": "world"}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        returned_uri = secrets_manager_backend.get_variable("hello")
        assert returned_uri == "world"

    @mock_aws
    def test_get_variable_non_existent_key(self):
        """
        Test that if Variable key is not present,
        SystemsManagerParameterStoreBackend.get_variables should return None
        """
        secret_id = "airflow/variables/hello"
        create_param = {"Name": secret_id, "SecretString": "world"}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        assert secrets_manager_backend.get_variable("test_mysql") is None

    @mock_aws
    def test_get_variable_with_team_name(self):
        secret_id = "airflow/variables/my_team--hello"
        create_param = {"Name": secret_id, "SecretString": "world"}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        assert secrets_manager_backend.get_variable(key="hello", team_name="my_team") == "world"

    @multi_team_enabled
    @mock_aws
    def test_global_caller_cannot_access_team_scoped_variable(self):
        secret_id = "airflow/variables/my_team--hello"
        create_param = {"Name": secret_id, "SecretString": "world"}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        assert secrets_manager_backend.get_variable(key="my_team--hello") is None

    @mock_aws
    def test_get_config_non_existent_key(self):
        """
        Test that if Config key is not present,
        SystemsManagerParameterStoreBackend.get_config should return None
        """
        secret_id = "airflow/config/hello"
        create_param = {"Name": secret_id, "SecretString": "world"}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        assert secrets_manager_backend.get_config("test") is None

    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend._get_secret")
    def test_connection_prefix_none_value(self, mock_get_secret):
        """
        Test that if Connection ID is not present in AWS Secrets Manager,
        SecretsManagerBackend.get_conn_value should return None,
        SecretsManagerBackend._get_secret should not be called
        """
        kwargs = {"connections_prefix": None}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)

        assert secrets_manager_backend.get_conn_value("test_mysql") is None
        mock_get_secret.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend._get_secret")
    def test_variable_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variable key is not present in AWS Secrets Manager,
        SecretsManagerBackend.get_variables should return None,
        SecretsManagerBackend._get_secret should not be called
        """
        kwargs = {"variables_prefix": None}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)

        assert secrets_manager_backend.get_variable("hello") is None
        mock_get_secret.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend._get_secret")
    def test_config_prefix_none_value(self, mock_get_secret):
        """
        Test that if Config key is not present in AWS Secrets Manager,
        SecretsManagerBackend.get_config should return None,
        SecretsManagerBackend._get_secret should not be called
        """
        kwargs = {"config_prefix": None}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)

        assert secrets_manager_backend.get_config("config") is None
        mock_get_secret.assert_not_called()

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend.client",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        ("connection_id", "connections_lookup_pattern", "num_client_calls"),
        [
            ("test", "test", 1),
            ("test", ".*", 1),
            ("test", "T.*", 1),
            ("test", "dummy-pattern", 0),
            ("test", None, 1),
        ],
    )
    def test_connection_lookup_pattern(
        self, mock_client, connection_id, connections_lookup_pattern, num_client_calls
    ):
        """
        Test that if Connection ID is looked up in AWS Secrets Manager
        """
        mock_client().get_secret_value.return_value = {"SecretString": None}
        kwargs = {"connections_lookup_pattern": connections_lookup_pattern}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)
        secrets_manager_backend.get_conn_value(connection_id)
        assert mock_client().get_secret_value.call_count == num_client_calls

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend.client",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        ("variable_key", "variables_lookup_pattern", "num_client_calls"),
        [
            ("test", "test", 1),
            ("test", ".*", 1),
            ("test", "T.*", 1),
            ("test", "dummy-pattern", 0),
            ("test", None, 1),
        ],
    )
    def test_variable_lookup_pattern(
        self, mock_client, variable_key, variables_lookup_pattern, num_client_calls
    ):
        """
        Test that if Variable key is looked up in AWS Secrets Manager
        """
        mock_client().get_secret_value.return_value = {"SecretString": None}
        kwargs = {"variables_lookup_pattern": variables_lookup_pattern}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)
        secrets_manager_backend.get_variable(variable_key)
        assert mock_client().get_secret_value.call_count == num_client_calls

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend.client",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        ("config_key", "config_lookup_pattern", "num_client_calls"),
        [
            ("test", "test", 1),
            ("test", ".*", 1),
            ("test", "T.*", 1),
            ("test", "dummy-pattern", 0),
            ("test", None, 1),
        ],
    )
    def test_config_lookup_pattern(self, mock_client, config_key, config_lookup_pattern, num_client_calls):
        """
        Test that if Variable key is looked up in AWS Secrets Manager
        """
        mock_client().get_secret_value.return_value = {"SecretString": None}
        kwargs = {"config_lookup_pattern": config_lookup_pattern}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)
        secrets_manager_backend.get_config(config_key)
        assert mock_client().get_secret_value.call_count == num_client_calls

    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.SessionFactory")
    def test_passing_client_kwargs(self, mock_session_factory):
        secrets_manager_backend = SecretsManagerBackend(
            use_ssl=False, role_arn="arn:aws:iam::222222222222:role/awesome-role", region_name="eu-central-1"
        )

        # Mock SessionFactory, session and client
        mock_session_factory_instance = mock_session_factory.return_value
        mock_ssm_client = mock.MagicMock(return_value="mock-secretsmanager-client")
        mock_session = mock.MagicMock()
        mock_session.client = mock_ssm_client
        mock_create_session = mock.MagicMock(return_value=mock_session)
        mock_session_factory_instance.create_session = mock_create_session

        secrets_manager_backend.client
        assert mock_session_factory.call_count == 1
        mock_session_factory_call_kwargs = mock_session_factory.call_args.kwargs
        assert "conn" in mock_session_factory_call_kwargs
        conn_wrapper = mock_session_factory_call_kwargs["conn"]

        assert conn_wrapper.conn_id == "SecretsManagerBackend__connection"
        assert conn_wrapper.role_arn == "arn:aws:iam::222222222222:role/awesome-role"
        assert conn_wrapper.region_name == "eu-central-1"

        mock_ssm_client.assert_called_once_with(
            service_name="secretsmanager", region_name="eu-central-1", use_ssl=False
        )
