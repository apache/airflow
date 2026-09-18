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

from unittest import mock

import pytest
from botocore.credentials import CredentialProvider

from airflow.providers.amazon.aws.hooks.msk import MskHook, oauth_cb

MOCK_MSK_SIGNER_MODULE = mock.MagicMock()


class TestMskHook:
    def setup_method(self):
        self.hook = MskHook(aws_conn_id="aws_msk", region_name="us-east-1")

    def test_init(self):
        assert self.hook.aws_conn_id == "aws_msk"
        assert self.hook.client_type == "kafka"

    @mock.patch.dict("sys.modules", {"aws_msk_iam_sasl_signer": MOCK_MSK_SIGNER_MODULE})
    @mock.patch.object(MskHook, "get_session")
    def test_confluent_token(self, mock_get_session):
        credentials = mock_get_session.return_value.get_credentials.return_value
        mock_generate_auth_token = (
            MOCK_MSK_SIGNER_MODULE.MSKAuthTokenProvider.generate_auth_token_from_credentials_provider
        )
        mock_generate_auth_token.reset_mock()
        mock_generate_auth_token.return_value = ("token", 1_700_000_900_000)

        token, expiry = self.hook.confluent_token("")

        region, credentials_provider = mock_generate_auth_token.call_args.args
        assert region == "us-east-1"
        assert isinstance(credentials_provider, CredentialProvider)
        assert credentials_provider.load() is credentials
        mock_get_session.assert_called_once_with(region_name="us-east-1")
        assert token == "token"
        assert expiry == 1_700_000_900.0

    @mock.patch.object(MskHook, "region_name", new_callable=mock.PropertyMock)
    def test_confluent_token_requires_region(self, mock_region_name):
        mock_region_name.return_value = None

        with pytest.raises(ValueError, match="AWS region is required"):
            self.hook.confluent_token("")


class TestOauthCallback:
    @pytest.mark.parametrize(
        ("config_str", "region_name"),
        [
            ('{"aws_conn_id":"aws_msk"}', None),
            ('{"aws_conn_id":"aws_msk","region_name":"us-east-1"}', "us-east-1"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.msk.MskHook", autospec=True)
    def test_uses_configured_aws_connection(self, mock_hook, config_str, region_name):
        mock_hook.return_value.confluent_token.return_value = ("token", 1_700_000_900.0)

        assert oauth_cb(config_str) == ("token", 1_700_000_900.0)

        assert mock_hook.mock_calls == [
            mock.call(aws_conn_id="aws_msk", region_name=region_name),
            mock.call().confluent_token(config_str),
        ]

    @pytest.mark.parametrize(
        ("config_str", "error"),
        [
            ("invalid", "Invalid JSON in config_str"),
            ("[]", "config_str must contain a JSON object"),
            ("", "Missing 'aws_conn_id' in config_str"),
            ('{"aws_conn_id":null}', "Missing 'aws_conn_id' in config_str"),
            ('{"aws_conn_id":42}', "Missing 'aws_conn_id' in config_str"),
        ],
    )
    def test_rejects_invalid_config(self, config_str, error):
        with pytest.raises(ValueError, match=error):
            oauth_cb(config_str)
