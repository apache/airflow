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

from google.cloud.translate_v3.types import TranslateTextResponse

from airflow.providers.google.cloud.hooks.translate import CloudTranslateHook, TranslateHook
from airflow.providers.google.common.consts import CLIENT_INFO

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

PROJECT_ID_TEST = "project-id"


class TestCloudTranslateHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.translate.CloudTranslateHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudTranslateHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.translate.CloudTranslateHook.get_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.translate.Client")
    def test_translate_client_creation(self, mock_client, mock_get_creds):
        result = self.hook.get_conn()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value, client_info=CLIENT_INFO)
        assert mock_client.return_value == result
        assert self.hook._client == result

    @mock.patch("airflow.providers.google.cloud.hooks.translate.CloudTranslateHook.get_conn")
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


class TestTranslateHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.translate.TranslateHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = TranslateHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.translate.TranslateHook.get_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.translate.TranslationServiceClient")
    def test_translate_client_creation(self, mock_client, mock_get_creds):
        result = self.hook.get_client()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value, client_info=CLIENT_INFO)
        assert mock_client.return_value == result
        assert self.hook._client == result

    @mock.patch("airflow.providers.google.cloud.hooks.translate.TranslateHook.get_client")
    def test_translate_text_method(self, get_client):
        translation_result_data = {
            "translations": [
                {"translated_text": "Hello World!", "model": "", "detected_language_code": ""},
                {
                    "translated_text": "Can you get me a cup of coffee, please?",
                    "model": "",
                    "detected_language_code": "",
                },
            ],
            "glossary_translations": [],
        }
        data_to_translate = ["Ciao mondo!", "Mi puoi prendere una tazza di caffè, per favore?"]
        translate_client = get_client.return_value
        translate_text_client_method = translate_client.translate_text
        translate_text_client_method.return_value = TranslateTextResponse(translation_result_data)

        input_translation_args = dict(
            project_id=PROJECT_ID_TEST,
            contents=data_to_translate,
            source_language_code="it",
            target_language_code="en",
            mime_type="text/plain",
            location="global",
            glossary_config=None,
            transliteration_config=None,
            model=None,
            labels=None,
            metadata=(),
            timeout=30,
            retry=None,
        )
        result = self.hook.translate_text(**input_translation_args)
        assert result == translation_result_data

        expected_call_args = {
            "request": {
                "parent": f"projects/{PROJECT_ID_TEST}/locations/global",
                "contents": data_to_translate,
                "source_language_code": "it",
                "target_language_code": "en",
                "mime_type": "text/plain",
                "glossary_config": None,
                "transliteration_config": None,
                "model": None,
                "labels": None,
            },
            "retry": None,
            "metadata": (),
            "timeout": 30,
        }
        translate_text_client_method.assert_called_once_with(**expected_call_args)

    @mock.patch("airflow.providers.google.cloud.hooks.translate.TranslateHook.get_client")
    def test_batch_translate_text_method(self, get_client):
        sample_method_result = "batch_translate_api_call_result_obj"
        translate_client = get_client.return_value
        translate_text_client_method = translate_client.batch_translate_text
        translate_text_client_method.return_value = sample_method_result
        BATCH_TRANSLATE_INPUT = {
            "gcs_source": {"input_uri": "input_source_uri"},
            "mime_type": "text/plain",
        }
        GCS_OUTPUT_DST = {"gcs_destination": {"output_uri_prefix": "translate_output_uri_prefix"}}
        LOCATION = "us-central1"
        input_translation_args = dict(
            project_id=PROJECT_ID_TEST,
            source_language_code="de",
            target_language_codes=["en", "uk"],
            location=LOCATION,
            input_configs=[BATCH_TRANSLATE_INPUT],
            output_config=GCS_OUTPUT_DST,
            glossaries=None,
            models=None,
            labels=None,
            metadata=(),
            timeout=30,
            retry=None,
        )
        result = self.hook.batch_translate_text(**input_translation_args)
        expected_call_args = {
            "request": {
                "parent": f"projects/{PROJECT_ID_TEST}/locations/{LOCATION}",
                "source_language_code": "de",
                "target_language_codes": ["en", "uk"],
                "input_configs": [BATCH_TRANSLATE_INPUT],
                "output_config": GCS_OUTPUT_DST,
                "glossaries": None,
                "models": None,
                "labels": None,
            },
            "retry": None,
            "metadata": (),
            "timeout": 30,
        }
        translate_text_client_method.assert_called_once_with(**expected_call_args)
        assert result == sample_method_result
