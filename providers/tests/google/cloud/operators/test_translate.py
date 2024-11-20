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

from airflow.providers.google.cloud.operators.translate import (
    CloudTranslateTextOperator,
    TranslateTextBatchOperator,
    TranslateTextOperator,
)

GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
PROJECT_ID = "test-project-id"


class TestCloudTranslate:
    @mock.patch("airflow.providers.google.cloud.operators.translate.CloudTranslateHook")
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.translate.return_value = [
            {
                "translatedText": "Yellowing self Gęśle",
                "detectedSourceLanguage": "pl",
                "model": "base",
                "input": "zażółć gęślą jaźń",
            }
        ]
        op = CloudTranslateTextOperator(
            values=["zażółć gęślą jaźń"],
            target_language="en",
            format_="text",
            source_language=None,
            model="base",
            gcp_conn_id=GCP_CONN_ID,
            task_id="id",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        return_value = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.translate.assert_called_once_with(
            values=["zażółć gęślą jaźń"],
            target_language="en",
            format_="text",
            source_language=None,
            model="base",
        )
        assert [
            {
                "translatedText": "Yellowing self Gęśle",
                "detectedSourceLanguage": "pl",
                "model": "base",
                "input": "zażółć gęślą jaźń",
            }
        ] == return_value


class TestTranslateText:
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook):
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
        mock_hook.return_value.translate_text.return_value = translation_result_data
        data_to_translate = ["Ciao mondo!", "Mi puoi prendere una tazza di caffè, per favore?"]
        op = TranslateTextOperator(
            task_id="task_id",
            contents=data_to_translate,
            source_language_code="it",
            target_language_code="en",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=30,
            retry=None,
            model=None,
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.translate_text.assert_called_once_with(
            contents=data_to_translate,
            source_language_code="it",
            target_language_code="en",
            mime_type=None,
            location=None,
            labels=None,
            model=None,
            transliteration_config=None,
            glossary_config=None,
            timeout=30,
            retry=None,
            metadata=(),
        )
        assert translation_result_data == result


class TestTranslateTextBatchOperator:
    @mock.patch("airflow.providers.google.cloud.links.translate.TranslateTextBatchLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_link_persist):
        input_config_item = {
            "gcs_source": {"input_uri": "gs://source_bucket_uri/sample_data_src_lang.txt"},
            "mime_type": "text/plain",
        }
        SRC_LANG_CODE = "src_lang_code"
        TARGET_LANG_CODES = ["target_lang_code1", "target_lang_code2"]
        LOCATION = "location-id"
        TIMEOUT = 30
        INPUT_CONFIGS = [input_config_item]
        OUTPUT_CONFIG = {"gcs_destination": {"output_uri_prefix": "gs://source_bucket_uri/output/"}}
        batch_translation_results_data = {"batch_text_translate_results": OUTPUT_CONFIG["gcs_destination"]}
        mock_hook.return_value.batch_translate_text.return_value = batch_translation_results_data

        op = TranslateTextBatchOperator(
            task_id="task_id_test",
            project_id=PROJECT_ID,
            source_language_code=SRC_LANG_CODE,
            target_language_codes=TARGET_LANG_CODES,
            location=LOCATION,
            models=None,
            glossaries=None,
            input_configs=INPUT_CONFIGS,
            output_config=OUTPUT_CONFIG,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT,
            retry=None,
        )
        context = {"ti": mock.MagicMock()}
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.batch_translate_text.assert_called_once_with(
            project_id=PROJECT_ID,
            source_language_code=SRC_LANG_CODE,
            target_language_codes=TARGET_LANG_CODES,
            location=LOCATION,
            input_configs=INPUT_CONFIGS,
            output_config=OUTPUT_CONFIG,
            timeout=TIMEOUT,
            models=None,
            glossaries=None,
            labels=None,
            retry=None,
            metadata=(),
        )
        assert batch_translation_results_data == result

        mock_link_persist.assert_called_once_with(
            context=context,
            task_instance=op,
            project_id=PROJECT_ID,
            output_config=OUTPUT_CONFIG,
        )
