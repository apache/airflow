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

import os
from unittest.mock import patch

import pytest
import responses
from botocore.awsrequest import AWSRequest

pytest.importorskip("s3fs")

TEST_CONN = "aws_test_conn"
TEST_SIGNER_URL = "https://nowhere.to.be.found"
TEST_SIGNER_TOKEN = "TOKEN1234"
TEST_SIGNER_RESP_URL = "https://where.are.you"
TEST_HEADER_KEY = "x-airflow"
TEST_HEADER_VALUE = "payload"
TEST_REQ_URI = "s3://bucket/key"


class TestFilesystem:
    def test_get_s3fs(self):
        from airflow.providers.amazon.aws.fs.s3 import get_fs

        fs = get_fs(conn_id=TEST_CONN)

        assert "s3" in fs.protocol

    @patch("s3fs.S3FileSystem", autospec=True)
    def test_get_s3fs_anonymous(self, s3fs, monkeypatch):
        from airflow.providers.amazon.aws.fs.s3 import get_fs

        # remove all AWS_* env vars
        for env_name in os.environ:
            if env_name.startswith("AWS"):
                monkeypatch.delenv(env_name, raising=False)

        get_fs(conn_id=None)

        assert s3fs.call_args.kwargs["anon"] is True

    @responses.activate
    def test_signer(self):
        from airflow.providers.amazon.aws.fs.s3 import s3v4_rest_signer

        req = AWSRequest(
            method="GET",
            url=TEST_REQ_URI,
            headers={"x": "y"},
        )
        req.context = {"client_region": "antarctica"}

        responses.add(
            responses.POST,
            f"{TEST_SIGNER_URL}/v1/aws/s3/sign",
            json={
                "uri": TEST_SIGNER_RESP_URL,
                "headers": {
                    TEST_HEADER_KEY: [TEST_HEADER_VALUE],
                },
            },
        )

        req = s3v4_rest_signer(
            {
                "uri": TEST_SIGNER_URL,
                "token": TEST_SIGNER_TOKEN,
            },
            req,
        )

        assert req.url == TEST_SIGNER_RESP_URL
        assert req.headers[TEST_HEADER_KEY] == TEST_HEADER_VALUE
