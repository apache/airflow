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

import json
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.apache.kafka.hooks.base import (
    MSK_BOOTSTRAP_SERVERS_REGEX,
    KafkaBaseHook,
    _msk_iam_oauth_cb,
)
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    import importlib.util

    if not importlib.util.find_spec("airflow.sdk.bases.hook"):
        raise ImportError

    BASEHOOK_PATCH_PATH = "airflow.sdk.bases.hook.BaseHook"
except ImportError:
    BASEHOOK_PATCH_PATH = "airflow.hooks.base.BaseHook"


class SomeKafkaHook(KafkaBaseHook):
    def _get_client(self, config):
        return config


@pytest.fixture
def hook():
    return SomeKafkaHook()


TIMEOUT = 10


class TestKafkaBaseHook:
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn(self, mock_get_connection, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        assert hook.get_conn == config

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn_value_error(self, mock_get_connection, hook):
        mock_get_connection.return_value.extra_dejson = {}
        with pytest.raises(ValueError, match="must be provided"):
            hook.get_conn()

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_callbacks_resolved_from_connection_dotted_path(self, mock_get_connection):
        stats_cb = MagicMock()
        mock_get_connection.return_value.extra_dejson = {
            "bootstrap.servers": "localhost:9092",
            "error_cb": "json.loads",
            "oauth_cb": "json.dumps",
            "stats_cb": stats_cb,
        }

        config = SomeKafkaHook().get_conn
        assert config["error_cb"] is json.loads
        assert config["oauth_cb"] is json.dumps
        # Already-callable values on the connection are passed through unchanged.
        assert config["stats_cb"] is stats_cb

    @mock.patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_test_connection(self, mock_get_connection, admin_client, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        connection = hook.test_connection()
        admin_client.assert_called_once_with(config)
        mock_admin_instance = admin_client.return_value
        mock_admin_instance.list_topics.assert_called_once_with(timeout=TIMEOUT)
        assert connection == (True, "Connection successful.")

    @mock.patch(
        "airflow.providers.apache.kafka.hooks.base.AdminClient",
        return_value=MagicMock(list_topics=MagicMock(return_value=[])),
    )
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_test_connection_no_topics(self, mock_get_connection, admin_client, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        connection = hook.test_connection()
        admin_client.assert_called_once_with(config)
        mock_admin_instance = admin_client.return_value
        mock_admin_instance.list_topics.assert_called_once_with(timeout=TIMEOUT)
        assert connection == (False, "Failed to establish connection.")

    @mock.patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_test_connection_resolves_callbacks(self, mock_get_connection, admin_client, hook):
        # ``test_connection`` runs the same callback resolution as the runtime clients,
        # so dotted-path strings on the connection are exercised by the UI test too.
        mock_get_connection.return_value.extra_dejson = {
            "bootstrap.servers": "localhost:9092",
            "error_cb": "json.loads",
        }
        assert hook.test_connection() == (True, "Connection successful.")
        admin_client.assert_called_once_with({"bootstrap.servers": "localhost:9092", "error_cb": json.loads})

    @mock.patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_test_connection_exception(self, mock_get_connection, admin_client, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        admin_client.return_value.list_topics.side_effect = [ValueError("some error")]
        connection = hook.test_connection()
        assert connection == (False, "some error")

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn_msk_iam_provisioned(self, mock_get_connection, hook):
        config = {
            "bootstrap.servers": "b-1.demo.abcde1.c2.kafka.us-east-1.amazonaws.com:9098",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "OAUTHBEARER",
        }
        mock_get_connection.return_value.extra_dejson = config
        with mock.patch.dict("sys.modules", {"aws_msk_iam_sasl_signer": MagicMock()}):
            result = hook.get_conn
        assert "oauth_cb" in result
        assert result["oauth_cb"].func is _msk_iam_oauth_cb
        assert result["oauth_cb"].args == ("us-east-1",)

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn_msk_iam_serverless(self, mock_get_connection, hook):
        config = {
            "bootstrap.servers": "boot-abcde1.c2.kafka-serverless.eu-west-1.amazonaws.com:9098",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "OAUTHBEARER",
        }
        mock_get_connection.return_value.extra_dejson = config
        with mock.patch.dict("sys.modules", {"aws_msk_iam_sasl_signer": MagicMock()}):
            result = hook.get_conn
        assert "oauth_cb" in result
        assert result["oauth_cb"].args == ("eu-west-1",)

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn_regular_host_no_msk_injection(self, mock_get_connection, hook):
        config = {
            "bootstrap.servers": "localhost:9092",
            "sasl.mechanism": "OAUTHBEARER",
        }
        mock_get_connection.return_value.extra_dejson = config
        result = hook.get_conn
        assert "oauth_cb" not in result

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn_msk_host_without_oauthbearer_no_injection(self, mock_get_connection, hook):
        config = {
            "bootstrap.servers": "b-1.demo.abcde1.c2.kafka.us-east-1.amazonaws.com:9098",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512",
        }
        mock_get_connection.return_value.extra_dejson = config
        result = hook.get_conn
        assert "oauth_cb" not in result

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn_msk_iam_does_not_override_user_oauth_cb(self, mock_get_connection, hook):
        user_cb = MagicMock()
        config = {
            "bootstrap.servers": "b-1.demo.abcde1.c2.kafka.us-east-1.amazonaws.com:9098",
            "sasl.mechanism": "OAUTHBEARER",
            "oauth_cb": user_cb,
        }
        mock_get_connection.return_value.extra_dejson = config
        result = hook.get_conn
        assert result["oauth_cb"] is user_cb

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn_msk_iam_missing_library(self, mock_get_connection, hook):
        config = {
            "bootstrap.servers": "b-1.demo.abcde1.c2.kafka.us-east-1.amazonaws.com:9098",
            "sasl.mechanism": "OAUTHBEARER",
        }
        mock_get_connection.return_value.extra_dejson = config
        with mock.patch.dict("sys.modules", {"aws_msk_iam_sasl_signer": None}):
            with pytest.raises(AirflowOptionalProviderFeatureException, match="msk"):
                _ = hook.get_conn

    def test_msk_iam_oauth_cb_returns_seconds(self):
        fake_signer = MagicMock()
        fake_signer.MSKAuthTokenProvider.generate_auth_token.return_value = ("my-token", 1_700_000_900_000)
        with mock.patch.dict("sys.modules", {"aws_msk_iam_sasl_signer": fake_signer}):
            token, expiry = _msk_iam_oauth_cb("us-east-1", "")
        fake_signer.MSKAuthTokenProvider.generate_auth_token.assert_called_once_with("us-east-1")
        assert token == "my-token"
        assert expiry == 1_700_000_900.0

    @pytest.mark.parametrize(
        ("bootstrap_servers", "expected_region"),
        [
            ("b-1.demo.abcde1.c2.kafka.us-east-1.amazonaws.com:9098", "us-east-1"),
            ("boot-abcde1.c2.kafka-serverless.us-east-1.amazonaws.com:9098", "us-east-1"),
            ("b-1.x.kafka.cn-north-1.amazonaws.com.cn:9098", "cn-north-1"),
            # Hostnames are case-insensitive; the region must be normalised to lower case
            # because the SigV4 credential scope requires it.
            ("b-1.x.kafka.US-EAST-1.amazonaws.com:9098", "us-east-1"),
            ("b1:9092,b2.kafka.us-west-2.amazonaws.com:9098", "us-west-2"),
            # A look-alike host that merely embeds an MSK-shaped substring must not match,
            # otherwise the hook would sign an IAM token for an untrusted broker.
            ("b-1.x.kafka.us-east-1.amazonaws.com.evil.example.com:9092", None),
            ("localhost:9092", None),
            ("kafka.example.com:9092", None),
        ],
    )
    def test_msk_bootstrap_servers_regex(self, bootstrap_servers, expected_region):
        match = MSK_BOOTSTRAP_SERVERS_REGEX.search(bootstrap_servers)
        if expected_region is None:
            assert match is None
        else:
            assert match is not None
            assert match.group("region").lower() == expected_region
