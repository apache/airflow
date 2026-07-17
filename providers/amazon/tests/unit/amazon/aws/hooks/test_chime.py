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

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.chime import ChimeWebhookHook
from airflow.providers.common.compat.sdk import AirflowException


class TestChimeWebhookHook:
    _config = {
        "chime_conn_id": "default-chime-webhook",
        "webhook_endpoint": "incomingwebhooks/abcd-1134-ZeDA?token=somechimetoken-111",
        "message": "your message here",
    }

    expected_payload_dict = {
        "Content": _config["message"],
    }

    expected_payload = json.dumps(expected_payload_dict)

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="default-chime-webhook",
                conn_type="chime",
                host="hooks.chime.aws/incomingwebhooks/",
                password="abcd-1134-ZeDA?token=somechimetoken111",
                schema="https",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="chime-bad-url",
                conn_type="chime",
                host="https://hooks.chime.aws/",
                password="somebadurl",
                schema="https",
            )
        )

    def test_get_webhook_endpoint_invalid_url(self):
        # Given

        # When/Then
        expected_message = r"Expected Chime webhook token in the form"
        hook = ChimeWebhookHook(chime_conn_id="chime-bad-url")
        with pytest.raises(AirflowException, match=expected_message):
            assert not hook.webhook_endpoint

    def test_get_webhook_endpoint_conn_id(self):
        # Given
        conn_id = "default-chime-webhook"
        hook = ChimeWebhookHook(chime_conn_id=conn_id)
        expected_webhook_endpoint = (
            "https://hooks.chime.aws/incomingwebhooks/abcd-1134-ZeDA?token=somechimetoken111"
        )

        # When
        webhook_endpoint = hook._get_webhook_endpoint(conn_id)

        # Then
        assert webhook_endpoint == expected_webhook_endpoint

    def test_build_chime_payload(self):
        # Given
        hook = ChimeWebhookHook(self._config["chime_conn_id"])
        message = self._config["message"]
        # When
        payload = hook._build_chime_payload(message)
        # Then
        assert self.expected_payload == payload

    def test_build_chime_payload_message_length(self):
        # Given
        self._config.copy()
        # create message over the character limit
        message = "c" * 4097
        hook = ChimeWebhookHook(self._config["chime_conn_id"])

        # When/Then
        expected_message = "Chime message must be 4096 characters or less."
        with pytest.raises(AirflowException, match=expected_message):
            hook._build_chime_payload(message)
