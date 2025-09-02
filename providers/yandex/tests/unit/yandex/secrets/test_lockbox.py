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
from unittest.mock import MagicMock, Mock, patch

import pytest

yandexcloud = pytest.importorskip("yandexcloud")

import yandex.cloud.lockbox.v1.payload_pb2 as payload_pb
import yandex.cloud.lockbox.v1.secret_pb2 as secret_pb
import yandex.cloud.lockbox.v1.secret_service_pb2 as secret_service_pb

from airflow.providers.yandex.secrets.lockbox import LockboxSecretBackend
from airflow.providers.yandex.utils.defaults import default_conn_name


class TestLockboxSecretBackend:
    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_connection(self, mock_get_value):
        conn_id = "fake_conn"
        conn_type = "scheme"
        host = "host"
        login = "user"
        password = "pass"
        port = 100
        uri = f"{conn_type}://{login}:{password}@{host}:{port}"

        mock_get_value.return_value = uri

        conn = LockboxSecretBackend().get_connection(conn_id)

        assert conn.conn_id == conn_id
        assert conn.conn_type == conn_type
        assert conn.host == host
        assert conn.schema == ""
        assert conn.login == login
        assert conn.password == password
        assert conn.port == port
        assert conn.get_uri() == uri

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_connection_from_json(self, mock_get_value):
        conn_id = "airflow_to_yandexcloud"
        conn_type = "yandex_cloud"
        extra = '{"some": "extra values"}'
        c = {
            "conn_type": conn_type,
            "extra": extra,
        }

        mock_get_value.return_value = json.dumps(c)

        conn = LockboxSecretBackend().get_connection(conn_id)

        assert conn.extra == extra

        assert conn.conn_id == conn_id
        assert conn.conn_type == conn_type

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_variable(self, mock_get_value):
        k = "this-is-key"
        v = "this-is-value"

        mock_get_value.return_value = v

        value = LockboxSecretBackend().get_variable(k)

        assert value == v

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_config(self, mock_get_value):
        k = "this-is-key"
        v = "this-is-value"

        mock_get_value.return_value = v

        value = LockboxSecretBackend().get_config(k)

        assert value == v

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_connection_prefix_is_none(self, mock_get_value):
        uri = "scheme://user:pass@host:100"

        mock_get_value.return_value = uri

        conn = LockboxSecretBackend(
            connections_prefix=None,
        ).get_connection("fake_conn")

        assert conn is None

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_connection_with_oauth_token_auth(self, mock_get_value):
        conn_id = "yandex_cloud"
        uri = "scheme://user:pass@host:100"

        mock_get_value.return_value = uri

        conn = LockboxSecretBackend(
            yc_oauth_token="y3_Vd3eub7w9bIut67GHeL345gfb5GAnd3dZnf08FR1vjeUFve7Yi8hGvc",
        ).get_connection(conn_id)

        assert conn.conn_id == conn_id
        assert conn.get_uri() == uri

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_connection_conn_id_for_backend(self, mock_get_value):
        conn_id = "yandex_cloud"
        uri = "scheme://user:pass@host:100"

        mock_get_value.return_value = uri

        conn = LockboxSecretBackend(
            yc_connection_id=conn_id,
        ).get_connection(conn_id)

        assert conn is None

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_connection_default_conn_id(self, mock_get_value):
        conn_id = default_conn_name
        uri = "scheme://user:pass@host:100"

        mock_get_value.return_value = uri

        conn = LockboxSecretBackend().get_connection(conn_id)

        assert conn is None

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_variable_prefix_is_none(self, mock_get_value):
        k = "this-is-key"
        v = "this-is-value"

        mock_get_value.return_value = v

        value = LockboxSecretBackend(
            variables_prefix=None,
        ).get_variable(k)

        assert value is None

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secret_value")
    def test_yandex_lockbox_secret_backend_get_config_prefix_is_none(self, mock_get_value):
        k = "this-is-key"
        v = "this-is-value"

        mock_get_value.return_value = v

        value = LockboxSecretBackend(
            config_prefix=None,
        ).get_config(k)

        assert value is None

    def test_yandex_lockbox_secret_backend__client_created_without_exceptions(self):
        yc_oauth_token = "y3_Vd3eub7w9bIut67GHeL345gfb5GAnd3dZnf08FR1vjeUFve7Yi8hGvc"

        sm = LockboxSecretBackend(
            yc_oauth_token=yc_oauth_token,
        )

        assert sm._client is not None

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_field")
    def test_yandex_lockbox_secret_backend__client_credentials_received_from_connection(self, mock_get_field):
        yc_oauth_token = "y3_Vd3eub7w9bIut67GHeL345gfb5GAnd3dZnf08FR1vjeUFve7Yi8hGvc"
        yc_sa_key_json = "sa_key_json"
        yc_sa_key_json_path = "sa_key_json_path"
        folder_id = "folder_id123"
        endpoint = "some-custom-api-endpoint.cloud.yandex.net"
        yc_connection_id = "connection_id"

        fields = {
            "oauth": yc_oauth_token,
            "service_account_json": yc_sa_key_json,
            "service_account_json_path": yc_sa_key_json_path,
            "folder_id": folder_id,
            "endpoint": endpoint,
        }
        mock_get_field.side_effect = lambda key: fields[key]

        sm = LockboxSecretBackend(
            yc_connection_id=yc_connection_id,
        )
        client = sm._client

        assert client is not None
        assert sm.yc_oauth_token == yc_oauth_token
        assert sm.yc_sa_key_json == yc_sa_key_json
        assert sm.yc_sa_key_json_path == yc_sa_key_json_path
        assert sm.folder_id == folder_id
        assert sm.endpoint == endpoint
        assert sm.yc_connection_id == yc_connection_id

    def test_yandex_lockbox_secret_backend__get_endpoint(self):
        endpoint = "some-custom-api-endpoint.cloud.yandex.net"
        expected = {
            "endpoint": endpoint,
        }

        res = LockboxSecretBackend(
            endpoint=endpoint,
        )._get_endpoint()

        assert res == expected

    def test_yandex_lockbox_secret_backend__get_endpoint_not_specified(self):
        expected = {}

        res = LockboxSecretBackend()._get_endpoint()

        assert res == expected

    def test_yandex_lockbox_secret_backend__build_secret_name(self):
        prefix = "this-is-prefix"
        key = "this-is-key"
        expected = "this-is-prefix/this-is-key"

        res = LockboxSecretBackend()._build_secret_name(prefix, key)

        assert res == expected

    def test_yandex_lockbox_secret_backend__build_secret_name_no_prefix(self):
        prefix = ""
        key = "this-is-key"
        expected = "this-is-key"

        res = LockboxSecretBackend()._build_secret_name(prefix, key)

        assert res == expected

    def test_yandex_lockbox_secret_backend__build_secret_name_custom_sep(self):
        sep = "_"
        prefix = "this-is-prefix"
        key = "this-is-key"
        expected = "this-is-prefix_this-is-key"

        res = LockboxSecretBackend(
            sep=sep,
        )._build_secret_name(prefix, key)

        assert res == expected

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secrets")
    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_payload")
    def test_yandex_lockbox_secret_backend__get_secret_value(self, mock_get_payload, mock_get_secrets):
        target_name = "target_name"
        target_text = "target_text"

        mock_get_secrets.return_value = [
            secret_pb.Secret(
                id="123",
                name="one",
            ),
            secret_pb.Secret(
                id="456",
                name=target_name,
            ),
            secret_pb.Secret(
                id="789",
                name="two",
            ),
        ]
        mock_get_payload.return_value = payload_pb.Payload(
            entries=[
                payload_pb.Payload.Entry(text_value=target_text),
            ],
        )

        res = LockboxSecretBackend()._get_secret_value("", target_name)

        assert res == target_text

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secrets")
    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_payload")
    def test_yandex_lockbox_secret_backend__get_secret_value_not_found(
        self, mock_get_payload, mock_get_secrets
    ):
        target_name = "target_name"
        target_text = "target_text"

        mock_get_secrets.return_value = [
            secret_pb.Secret(
                id="123",
                name="one",
            ),
            secret_pb.Secret(
                id="789",
                name="two",
            ),
        ]
        mock_get_payload.return_value = payload_pb.Payload(
            entries=[
                payload_pb.Payload.Entry(text_value=target_text),
            ],
        )

        res = LockboxSecretBackend()._get_secret_value("", target_name)

        assert res is None

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_secrets")
    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._get_payload")
    def test_yandex_lockbox_secret_backend__get_secret_value_no_text_entries(
        self, mock_get_payload, mock_get_secrets
    ):
        target_name = "target_name"
        target_value = b"01010101"

        mock_get_secrets.return_value = [
            secret_pb.Secret(
                id="123",
                name="one",
            ),
            secret_pb.Secret(
                id="456",
                name="two",
            ),
            secret_pb.Secret(
                id="789",
                name=target_name,
            ),
        ]
        mock_get_payload.return_value = payload_pb.Payload(
            entries=[
                payload_pb.Payload.Entry(binary_value=target_value),
            ],
        )

        res = LockboxSecretBackend()._get_secret_value("", target_name)

        assert res is None

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._client")
    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._list_secrets")
    def test_yandex_lockbox_secret_backend__get_secrets(self, mock_list_secrets, mock_client):
        secrets = secret_service_pb.ListSecretsResponse(
            secrets=[
                secret_pb.Secret(
                    id="123",
                ),
                secret_pb.Secret(
                    id="456",
                ),
            ],
        )

        mock_list_secrets.return_value = secrets
        mock_client.return_value = None

        res = LockboxSecretBackend(
            folder_id="some-id",
        )._get_secrets()

        assert res == secrets.secrets

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._client")
    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._list_secrets")
    def test_yandex_lockbox_secret_backend__get_secrets_page_token(self, mock_list_secrets, mock_client):
        first_secrets = secret_service_pb.ListSecretsResponse(
            secrets=[
                secret_pb.Secret(
                    id="123",
                ),
                secret_pb.Secret(
                    id="456",
                ),
            ],
            next_page_token="token",
        )
        second_secrets = secret_service_pb.ListSecretsResponse(
            secrets=[
                secret_pb.Secret(
                    id="789",
                ),
                secret_pb.Secret(
                    id="000",
                ),
            ],
            next_page_token="",
        )

        mock_list_secrets.side_effect = [
            first_secrets,
            second_secrets,
        ]
        mock_client.return_value = None

        res = LockboxSecretBackend(
            folder_id="some-id",
        )._get_secrets()

        assert res == [*first_secrets.secrets, *second_secrets.secrets]

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._client")
    def test_yandex_lockbox_secret_backend__get_payload(self, mock_client):
        mock_stub = MagicMock()
        mock_response = payload_pb.Payload()
        mock_stub.Get.return_value = mock_response
        mock_client.return_value = mock_stub

        result = LockboxSecretBackend()._get_payload(
            secret_id="test_secret",
            version_id="test_version",
        )

        mock_client.assert_called_once()
        mock_stub.Get.assert_called_once()
        assert result == mock_response

    @patch("airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend._client")
    def test_yandex_lockbox_secret_backend__list_secrets(self, mock_client):
        mock_stub = MagicMock()
        mock_response = secret_service_pb.ListSecretsResponse()
        mock_stub.List.return_value = mock_response
        mock_client.return_value = mock_stub

        result = LockboxSecretBackend()._list_secrets(
            folder_id="test_folder",
        )

        mock_client.assert_called_once()
        mock_stub.List.assert_called_once()
        assert result == mock_response

    def test_yandex_lockbox_secret_backend_folder_id(self):
        folder_id = "id1"

        res = LockboxSecretBackend(
            folder_id=folder_id,
        ).folder_id

        assert res == folder_id

    @patch("airflow.models.connection.Connection.get_connection_from_secrets")
    def test_yandex_lockbox_secret_backend_folder_id_from_connection(self, mock_get_connection):
        folder_id = "id1"

        mock_get_connection.return_value = Mock(
            connection_id=default_conn_name,
            extra_dejson={"folder_id": folder_id},
        )

        sm = LockboxSecretBackend()
        _ = sm._client
        res = sm.folder_id

        assert res == folder_id

    def test_yandex_lockbox_secret_backend__get_field_connection_not_specified(self):
        sm = LockboxSecretBackend()
        sm.yc_connection_id = None
        res = sm._get_field("some-field")

        assert res is None
