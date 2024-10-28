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

from airflow.providers.google.cloud.hooks.translate import CloudTranslateHook
from airflow.providers.google.common.consts import CLIENT_INFO

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
)

PROJECT_ID_TEST = "project-id"


class TestCloudTranslateHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            CloudTranslateHook(gcp_conn_id="GCP_CONN_ID", delegate_to="delegate_to")

    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.translate.CloudTranslateHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudTranslateHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.cloud.hooks.translate.CloudTranslateHook.get_credentials"
    )
    @mock.patch("airflow.providers.google.cloud.hooks.translate.Client")
    def test_translate_client_creation(self, mock_client, mock_get_creds):
        result = self.hook.get_conn()
        mock_client.assert_called_once_with(
            credentials=mock_get_creds.return_value, client_info=CLIENT_INFO
        )
        assert mock_client.return_value == result
        assert self.hook._client == result

    @mock.patch(
        "airflow.providers.google.cloud.hooks.translate.CloudTranslateHook.get_conn"
    )
    def test_translate_called(self, get_conn):
        # Given
        translate_method = get_conn.return_value.translate
        translate_method.return_value = {
            "translatedText": "Yellowing self Gęśle",
            "detectedSourceLanguage": "pl",
            "model": "base",
            "input": "zażółć gęślą jaźń",
        }
        # When
        result = self.hook.translate(
            values=["zażółć gęślą jaźń"],
            target_language="en",
            format_="text",
            source_language=None,
            model="base",
        )
        # Then
        assert result == {
            "translatedText": "Yellowing self Gęśle",
            "detectedSourceLanguage": "pl",
            "model": "base",
            "input": "zażółć gęślą jaźń",
        }
        translate_method.assert_called_once_with(
            values=["zażółć gęślą jaźń"],
            target_language="en",
            format_="text",
            source_language=None,
            model="base",
        )
