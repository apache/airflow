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

from boto3.session import Session

from airflow.providers.amazon.aws.utils.redshift import build_credentials_block


class TestS3ToRedshiftTransfer:
    @mock.patch("boto3.session.Session")
    def test_build_credentials_block(self, mock_session):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        token = "aws_secret_token"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None

        credentials_block = build_credentials_block(mock_session.return_value)

        assert access_key in credentials_block
        assert secret_key in credentials_block
        assert token not in credentials_block

    @mock.patch("boto3.session.Session")
    def test_build_credentials_block_sts(self, mock_session):
        access_key = "ASIA_aws_access_key_id"
        secret_key = "aws_secret_access_key"
        token = "aws_secret_token"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = token

        credentials_block = build_credentials_block(mock_session.return_value)

        assert access_key in credentials_block
        assert secret_key in credentials_block
        assert token in credentials_block
