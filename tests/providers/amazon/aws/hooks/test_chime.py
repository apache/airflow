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

import json

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.chime import ChimeWebhookHook
from airflow.utils import db


class TestChimeWebhookHook:

    _config = {
        "http_conn_id": "default-chime-webhook",
        "webhook_endpoint": "incomingwebhooks/abcd-1134?token=somechimetoken_111",
        "message": "your message here",
    }

    expected_payload_dict = {
        "Content": _config["message"],
    }

    expected_payload = json.dumps(expected_payload_dict)

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="default-chime-webhook",
                conn_type="chime",
                host="https://hooks.chime.aws",
                extra='{"webhook_endpoint": "incomingwebhooks/abcd-1134?token=somechimetoken_111"}',
            )
        )

    def test_get_webhook_endpoint_manual_token(self):
        # Given
        provided_endpoint = "incomingwebhooks/abcd-1134?token=somechimetoken_111"
        hook = ChimeWebhookHook(webhook_endpoint=provided_endpoint)

        # When
        webhook_endpoint = hook._get_webhook_endpoint(None, provided_endpoint)

        # Then
        assert webhook_endpoint == provided_endpoint

    def test_get_webhook_endpoint_invalid_url(self):
        # Given
        provided_endpoint = "https://hooks.chime.aws/some-invalid-webhook-url"

        # When/Then
        expected_message = "Expected Chime webhook endpoint in the form of"
        with pytest.raises(AirflowException, match=expected_message):
            ChimeWebhookHook(webhook_endpoint=provided_endpoint)

    def test_get_webhook_endpoint_conn_id(self):
        # Given
        conn_id = "default-chime-webhook"
        hook = ChimeWebhookHook(http_conn_id=conn_id)
        expected_webhook_endpoint = "incomingwebhooks/abcd-1134?token=somechimetoken_111"

        # When
        webhook_endpoint = hook._get_webhook_endpoint(conn_id, None)

        # Then
        assert webhook_endpoint == expected_webhook_endpoint

    def test_build_chime_payload(self):
        # Given
        hook = ChimeWebhookHook(**self._config)

        # When
        payload = hook._build_chime_payload()

        # Then
        assert self.expected_payload == payload

    def test_build_chime_payload_message_length(self):
        # Given
        config = self._config.copy()
        # create message over the character limit
        config["message"] = "c" * 4097
        hook = ChimeWebhookHook(**config)

        # When/Then
        expected_message = "Chime message must be 4096 characters or less."
        with pytest.raises(AirflowException, match=expected_message):
            hook._build_chime_payload()
