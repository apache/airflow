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

from airflow.providers.amazon.aws.hooks.msk import MskHook

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
