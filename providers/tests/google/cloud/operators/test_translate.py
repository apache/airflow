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

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.translate_v3.types import (
    BatchTranslateDocumentResponse,
    TranslateDocumentResponse,
    automl_translation,
    translation_service,
)

from airflow.providers.google.cloud.hooks.translate import TranslateHook
from airflow.providers.google.cloud.operators.translate import (
    CloudTranslateTextOperator,
    TranslateCreateDatasetOperator,
    TranslateCreateGlossaryOperator,
    TranslateCreateModelOperator,
    TranslateDatasetsListOperator,
    TranslateDeleteDatasetOperator,
    TranslateDeleteGlossaryOperator,
    TranslateDeleteModelOperator,
    TranslateDocumentBatchOperator,
    TranslateDocumentOperator,
    TranslateImportDataOperator,
    TranslateListGlossariesOperator,
    TranslateModelsListOperator,
    TranslateTextBatchOperator,
    TranslateTextOperator,
    TranslateUpdateGlossaryOperator,
)

GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
PROJECT_ID = "test-project-id"
DATASET_ID = "sample_ds_id"
MODEL_ID = "sample_model_id"
GLOSSARY_ID = "sample_glossary_id"
TIMEOUT_VALUE = 30
LOCATION = "location_id"


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
        assert return_value == [
            {
                "translatedText": "Yellowing self Gęśle",
                "detectedSourceLanguage": "pl",
                "model": "base",
                "input": "zażółć gęślą jaźń",
            }
        ]


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
            timeout=TIMEOUT_VALUE,
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
            timeout=TIMEOUT_VALUE,
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


class TestTranslateDatasetCreate:
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslationNativeDatasetLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateCreateDatasetOperator.xcom_push")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_xcom_push, mock_link_persist):
        DS_CREATION_RESULT_SAMPLE = {
            "display_name": "",
            "example_count": 0,
            "name": f"projects/{PROJECT_ID}/locations/{LOCATION}/datasets/{DATASET_ID}",
            "source_language_code": "",
            "target_language_code": "",
            "test_example_count": 0,
            "train_example_count": 0,
            "validate_example_count": 0,
        }
        sample_operation = mock.MagicMock()
        sample_operation.result.return_value = automl_translation.Dataset(DS_CREATION_RESULT_SAMPLE)

        mock_hook.return_value.create_dataset.return_value = sample_operation
        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()
        mock_hook.return_value.extract_object_id = TranslateHook.extract_object_id

        DATASET_DATA = {
            "display_name": "sample ds name",
            "source_language_code": "es",
            "target_language_code": "uk",
        }
        op = TranslateCreateDatasetOperator(
            task_id="task_id",
            dataset=DATASET_DATA,
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=None,
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_dataset.assert_called_once_with(
            dataset=DATASET_DATA,
            project_id=PROJECT_ID,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=None,
            metadata=(),
        )
        mock_xcom_push.assert_called_once_with(context, key="dataset_id", value=DATASET_ID)
        mock_link_persist.assert_called_once_with(
            context=context,
            dataset_id=DATASET_ID,
            task_instance=op,
            project_id=PROJECT_ID,
        )
        assert result == DS_CREATION_RESULT_SAMPLE


class TestTranslateListDatasets:
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslationDatasetsListLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_link_persist):
        DS_ID_1 = "sample_ds_1"
        DS_ID_2 = "sample_ds_2"
        dataset_result_1 = automl_translation.Dataset(
            dict(
                name=f"projects/{PROJECT_ID}/locations/{LOCATION}/datasets/{DS_ID_1}",
                display_name="ds1_display_name",
            )
        )
        dataset_result_2 = automl_translation.Dataset(
            dict(
                name=f"projects/{PROJECT_ID}/locations/{LOCATION}/datasets/{DS_ID_2}",
                display_name="ds1_display_name",
            )
        )
        mock_hook.return_value.list_datasets.return_value = [dataset_result_1, dataset_result_2]
        mock_hook.return_value.extract_object_id = TranslateHook.extract_object_id

        op = TranslateDatasetsListOperator(
            task_id="task_id",
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list_datasets.assert_called_once_with(
            project_id=PROJECT_ID,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
            metadata=(),
        )
        mock_link_persist.assert_called_once_with(
            context=context,
            task_instance=op,
            project_id=PROJECT_ID,
        )
        assert result == [DS_ID_1, DS_ID_2]


class TestTranslateImportData:
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslationNativeDatasetLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_link_persist):
        INPUT_CONFIG = {
            "input_files": [{"usage": "UNASSIGNED", "gcs_source": {"input_uri": "import data gcs path"}}]
        }
        mock_hook.return_value.import_dataset_data.return_value = mock.MagicMock()
        op = TranslateImportDataOperator(
            task_id="task_id",
            dataset_id=DATASET_ID,
            input_config=INPUT_CONFIG,
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.import_dataset_data.assert_called_once_with(
            dataset_id=DATASET_ID,
            input_config=INPUT_CONFIG,
            project_id=PROJECT_ID,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
            metadata=(),
        )
        mock_link_persist.assert_called_once_with(
            context=context,
            dataset_id=DATASET_ID,
            task_instance=op,
            project_id=PROJECT_ID,
        )


class TestTranslateDeleteData:
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook):
        m_delete_method_result = mock.MagicMock()
        mock_hook.return_value.delete_dataset.return_value = m_delete_method_result

        wait_for_done = mock_hook.return_value.wait_for_operation_done

        op = TranslateDeleteDatasetOperator(
            task_id="task_id",
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_dataset.assert_called_once_with(
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
            metadata=(),
        )
        wait_for_done.assert_called_once_with(operation=m_delete_method_result, timeout=TIMEOUT_VALUE)


class TestTranslateModelCreate:
    @mock.patch("airflow.providers.google.cloud.links.translate.TranslationModelLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateCreateModelOperator.xcom_push")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_xcom_push, mock_link_persist):
        MODEL_DISPLAY_NAME = "model_display_name_01"
        MODEL_CREATION_RESULT_SAMPLE = {
            "display_name": MODEL_DISPLAY_NAME,
            "name": f"projects/{PROJECT_ID}/locations/{LOCATION}/models/{MODEL_ID}",
            "dataset": f"projects/{PROJECT_ID}/locations/{LOCATION}/datasets/{DATASET_ID}",
            "source_language_code": "",
            "target_language_code": "",
            "create_time": "2024-11-15T14:05:00Z",
            "update_time": "2024-11-16T01:09:03Z",
            "test_example_count": 1000,
            "train_example_count": 115,
            "validate_example_count": 140,
        }
        sample_operation = mock.MagicMock()
        sample_operation.result.return_value = automl_translation.Model(MODEL_CREATION_RESULT_SAMPLE)

        mock_hook.return_value.create_model.return_value = sample_operation
        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()
        mock_hook.return_value.extract_object_id = TranslateHook.extract_object_id
        op = TranslateCreateModelOperator(
            task_id="task_id",
            display_name=MODEL_DISPLAY_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=None,
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_model.assert_called_once_with(
            display_name=MODEL_DISPLAY_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=None,
            metadata=(),
        )
        mock_xcom_push.assert_called_once_with(context, key="model_id", value=MODEL_ID)
        mock_link_persist.assert_called_once_with(
            context=context,
            task_instance=op,
            model_id=MODEL_ID,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )
        assert result == MODEL_CREATION_RESULT_SAMPLE


class TestTranslateListModels:
    @mock.patch("airflow.providers.google.cloud.links.translate.TranslationModelsListLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_link_persist):
        MODEL_ID_1 = "sample_model_1"
        MODEL_ID_2 = "sample_model_2"
        model_result_1 = automl_translation.Model(
            dict(
                display_name="model_1_display_name",
                name=f"projects/{PROJECT_ID}/locations/{LOCATION}/models/{MODEL_ID_1}",
                dataset=f"projects/{PROJECT_ID}/locations/{LOCATION}/datasets/ds_for_model_1",
                source_language_code="en",
                target_language_code="es",
            )
        )
        model_result_2 = automl_translation.Model(
            dict(
                display_name="model_2_display_name",
                name=f"projects/{PROJECT_ID}/locations/{LOCATION}/models/{MODEL_ID_2}",
                dataset=f"projects/{PROJECT_ID}/locations/{LOCATION}/datasets/ds_for_model_2",
                source_language_code="uk",
                target_language_code="en",
            )
        )
        mock_hook.return_value.list_models.return_value = [model_result_1, model_result_2]
        mock_hook.return_value.extract_object_id = TranslateHook.extract_object_id

        op = TranslateModelsListOperator(
            task_id="task_id",
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list_models.assert_called_once_with(
            project_id=PROJECT_ID,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
            metadata=(),
        )
        assert result == [MODEL_ID_1, MODEL_ID_2]
        mock_link_persist.assert_called_once_with(
            context=context,
            task_instance=op,
            project_id=PROJECT_ID,
        )


class TestTranslateDeleteModel:
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook):
        m_delete_method_result = mock.MagicMock()
        mock_hook.return_value.delete_model.return_value = m_delete_method_result
        wait_for_done = mock_hook.return_value.wait_for_operation_done

        op = TranslateDeleteModelOperator(
            task_id="task_id",
            model_id=MODEL_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
        )
        context = mock.MagicMock()
        op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_model.assert_called_once_with(
            model_id=MODEL_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
            metadata=(),
        )
        wait_for_done.assert_called_once_with(operation=m_delete_method_result, timeout=TIMEOUT_VALUE)


class TestTranslateDocumentBatchOperator:
    @mock.patch("airflow.providers.google.cloud.links.translate.TranslateResultByOutputConfigLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_link_persist):
        input_config_item_1 = {
            "gcs_source": {"input_uri": "gs://source_bucket_uri/sample_data_src_lang_1.txt"},
        }
        input_config_item_2 = {
            "gcs_source": {"input_uri": "gs://source_bucket_uri/sample_data_src_lang_2.txt"},
        }
        SRC_LANG_CODE = "src_lang_code"
        TARGET_LANG_CODES = ["target_lang_code1", "target_lang_code2"]
        TIMEOUT = 30
        INPUT_CONFIGS = [input_config_item_1, input_config_item_2]
        OUTPUT_CONFIG = {"gcs_destination": {"output_uri_prefix": "gs://source_bucket_uri/output/"}}
        BATCH_DOC_TRANSLATION_RESULT = {
            "submit_time": "2024-12-01T00:01:16Z",
            "end_time": "2024-12-01T00:10:01Z",
            "failed_characters": "0",
            "failed_pages": "0",
            "total_billable_characters": "0",
            "total_billable_pages": "6",
            "total_characters": "4240",
            "total_pages": "6",
            "translated_characters": "4240",
            "translated_pages": "6",
        }
        sample_operation = mock.MagicMock()
        sample_operation.result.return_value = BatchTranslateDocumentResponse(BATCH_DOC_TRANSLATION_RESULT)

        mock_hook.return_value.batch_translate_document.return_value = sample_operation
        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()

        op = TranslateDocumentBatchOperator(
            task_id="task_id_test",
            project_id=PROJECT_ID,
            source_language_code=SRC_LANG_CODE,
            target_language_codes=TARGET_LANG_CODES,
            location=LOCATION,
            models=None,
            glossaries=None,
            input_configs=INPUT_CONFIGS,
            output_config=OUTPUT_CONFIG,
            customized_attribution=None,
            format_conversions=None,
            enable_shadow_removal_native_pdf=False,
            enable_rotation_correction=False,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            metadata=(),
            timeout=TIMEOUT,
            retry=None,
        )
        context = {"ti": mock.MagicMock()}
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.batch_translate_document.assert_called_once_with(
            project_id=PROJECT_ID,
            source_language_code=SRC_LANG_CODE,
            target_language_codes=TARGET_LANG_CODES,
            location=LOCATION,
            input_configs=INPUT_CONFIGS,
            output_config=OUTPUT_CONFIG,
            customized_attribution=None,
            format_conversions=None,
            enable_shadow_removal_native_pdf=False,
            enable_rotation_correction=False,
            timeout=TIMEOUT,
            models=None,
            glossaries=None,
            retry=None,
            metadata=(),
        )

        assert result == BATCH_DOC_TRANSLATION_RESULT
        mock_link_persist.assert_called_once_with(
            context=context,
            task_instance=op,
            project_id=PROJECT_ID,
            output_config=OUTPUT_CONFIG,
        )


class TestTranslateDocumentOperator:
    @mock.patch("airflow.providers.google.cloud.links.translate.TranslateResultByOutputConfigLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_link_persist):
        SRC_LANG_CODE = "src_lang_code"
        TARGET_LANG_CODE = "target_lang_code1"
        TIMEOUT = 30
        INPUT_CONFIG = {"gcs_source": {"input_uri": "gs://source_bucket_uri/sample_data_src_lang_1.txt"}}
        OUTPUT_CONFIG = {"gcs_destination": {"output_uri_prefix": "gs://source_bucket_uri/output/"}}
        DOC_TRANSLATION_RESULT = {
            "document_translation": {
                "byte_stream_outputs": ["c29tZV9kYXRh"],
                "detected_language_code": "",
                "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
            "model": f"projects/{PROJECT_ID}/locations/us-central1/models/general/nmt",
        }

        mock_hook.return_value.translate_document.return_value = TranslateDocumentResponse(
            DOC_TRANSLATION_RESULT
        )

        op = TranslateDocumentOperator(
            task_id="task_id_test",
            project_id=PROJECT_ID,
            source_language_code=SRC_LANG_CODE,
            target_language_code=TARGET_LANG_CODE,
            location=LOCATION,
            model=None,
            glossary_config=None,
            labels=None,
            document_input_config=INPUT_CONFIG,
            document_output_config=OUTPUT_CONFIG,
            customized_attribution=None,
            is_translate_native_pdf_only=False,
            enable_shadow_removal_native_pdf=False,
            enable_rotation_correction=False,
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
        mock_hook.return_value.translate_document.assert_called_once_with(
            source_language_code=SRC_LANG_CODE,
            target_language_code=TARGET_LANG_CODE,
            location=LOCATION,
            model=None,
            glossary_config=None,
            labels=None,
            document_input_config=INPUT_CONFIG,
            document_output_config=OUTPUT_CONFIG,
            customized_attribution=None,
            is_translate_native_pdf_only=False,
            enable_shadow_removal_native_pdf=False,
            enable_rotation_correction=False,
            timeout=TIMEOUT,
            retry=None,
            metadata=(),
        )

        assert result == DOC_TRANSLATION_RESULT
        mock_link_persist.assert_called_once_with(
            context=context,
            task_instance=op,
            project_id=PROJECT_ID,
            output_config=OUTPUT_CONFIG,
        )


class TestTranslateGlossaryCreate:
    @mock.patch(
        "airflow.providers.google.cloud.operators.translate.TranslateCreateGlossaryOperator.xcom_push"
    )
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_xcom_push):
        GLOSSARY_CREATION_RESULT = {
            "name": f"projects/{PROJECT_ID}/locations/{LOCATION}/glossaries/{GLOSSARY_ID}",
            "display_name": f"{GLOSSARY_ID}",
            "entry_count": 42,
            "input_config": {"gcs_source": {"input_uri": "gs://input_bucket_path/glossary.csv"}},
            "language_pair": {"source_language_code": "en", "target_language_code": "es"},
            "submit_time": "2024-11-17T14:05:00Z",
            "end_time": "2024-11-17T17:09:03Z",
        }
        sample_operation = mock.MagicMock()
        sample_operation.result.return_value = translation_service.Glossary(GLOSSARY_CREATION_RESULT)

        mock_hook.return_value.create_glossary.return_value = sample_operation
        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()
        mock_hook.return_value.extract_object_id = TranslateHook.extract_object_id

        GLOSSARY_FILE_INPUT = {"gcs_source": {"input_uri": "gs://RESOURCE_BUCKET/glossary_sample.tsv"}}
        op = TranslateCreateGlossaryOperator(
            task_id="task_id",
            glossary_id=f"{GLOSSARY_ID}",
            input_config=GLOSSARY_FILE_INPUT,
            language_pair={"source_language_code": "en", "target_language_code": "es"},
            language_codes_set=None,
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=None,
        )
        context = mock.MagicMock()
        result = op.execute(context=context)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_glossary.assert_called_once_with(
            glossary_id=f"{GLOSSARY_ID}",
            input_config=GLOSSARY_FILE_INPUT,
            language_pair={"source_language_code": "en", "target_language_code": "es"},
            language_codes_set=None,
            project_id=PROJECT_ID,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=None,
            metadata=(),
        )
        mock_xcom_push.assert_called_once_with(context, key="glossary_id", value=GLOSSARY_ID)
        assert result == GLOSSARY_CREATION_RESULT


class TestTranslateGlossaryUpdate:
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook):
        UPDATE_GLOSSARY_RESULT = {
            "name": f"projects/{PROJECT_ID}/locations/{LOCATION}/glossaries/{GLOSSARY_ID}",
            "display_name": "new_glossary_display_name",
            "entry_count": 42,
            "input_config": {"gcs_source": {"input_uri": "gs://input_bucket_path/glossary_updated.csv"}},
            "language_pair": {"source_language_code": "en", "target_language_code": "es"},
            "submit_time": "2024-11-17T14:05:00Z",
            "end_time": "2024-11-17T17:09:03Z",
        }
        get_glossary_mock = mock.MagicMock()
        mock_hook.return_value.get_glossary.return_value = get_glossary_mock
        sample_operation = mock.MagicMock()
        sample_operation.result.return_value = translation_service.Glossary(UPDATE_GLOSSARY_RESULT)

        mock_hook.return_value.update_glossary.return_value = sample_operation
        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()

        UPDATE_GLOSSARY_FILE_INPUT = {"gcs_source": {"input_uri": "gs://RESOURCE_BUCKET/glossary_sample.tsv"}}
        op = TranslateUpdateGlossaryOperator(
            task_id="task_id",
            new_input_config=UPDATE_GLOSSARY_FILE_INPUT,
            new_display_name="new_glossary_display_name",
            glossary_id=GLOSSARY_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=None,
        )
        context = mock.MagicMock()
        result = op.execute(context=context)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.update_glossary.assert_called_once_with(
            glossary=get_glossary_mock,
            new_input_config=UPDATE_GLOSSARY_FILE_INPUT,
            new_display_name="new_glossary_display_name",
            timeout=TIMEOUT_VALUE,
            retry=None,
            metadata=(),
        )
        assert result == UPDATE_GLOSSARY_RESULT


class TestTranslateListGlossaries:
    @mock.patch("airflow.providers.google.cloud.links.translate.TranslationGlossariesListLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook, mock_link_persist):
        GLOSSARY_ID_1 = "sample_glossary_1"
        GLOSSARY_ID_2 = "sample_glossary_2"
        glossary_result_1 = translation_service.Glossary(
            dict(
                name=f"projects/{PROJECT_ID}/locations/{LOCATION}/glossaries/{GLOSSARY_ID_1}",
                display_name=f"{GLOSSARY_ID_1}",
                entry_count=100,
                input_config={"gcs_source": {"input_uri": "gs://input1.csv"}},
                language_pair={"source_language_code": "en", "target_language_code": "es"},
                submit_time="2024-11-17T14:05:00Z",
                end_time="2024-11-17T17:09:03Z",
            )
        )
        glossary_result_2 = translation_service.Glossary(
            dict(
                name=f"projects/{PROJECT_ID}/locations/{LOCATION}/glossaries/{GLOSSARY_ID_2}",
                display_name=f"{GLOSSARY_ID_2}",
                entry_count=200,
                input_config={"gcs_source": {"input_uri": "gs://input2.csv"}},
                language_pair={"source_language_code": "es", "target_language_code": "en"},
                submit_time="2024-11-17T14:05:00Z",
                end_time="2024-11-17T17:09:03Z",
            )
        )
        mock_hook.return_value.list_glossaries.return_value = [glossary_result_1, glossary_result_2]
        mock_hook.return_value.extract_object_id = TranslateHook.extract_object_id

        op = TranslateListGlossariesOperator(
            task_id="task_id",
            project_id=PROJECT_ID,
            page_size=100,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.list_glossaries.assert_called_once_with(
            project_id=PROJECT_ID,
            page_size=100,
            filter_str=None,
            page_token=None,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
            metadata=(),
        )
        assert result == [GLOSSARY_ID_1, GLOSSARY_ID_2]
        mock_link_persist.assert_called_once_with(
            context=context,
            task_instance=op,
            project_id=PROJECT_ID,
        )


class TestTranslateDeleteGlossary:
    @mock.patch("airflow.providers.google.cloud.operators.translate.TranslateHook")
    def test_minimal_green_path(self, mock_hook):
        DELETION_RESULT_SAMPLE = {
            "submit_time": "2024-11-17T14:05:00Z",
            "end_time": "2024-11-17T17:09:03Z",
            "name": f"projects/{PROJECT_ID}/locations/{LOCATION}/glossaries/{GLOSSARY_ID}",
        }
        sample_operation = mock.MagicMock()
        sample_operation.result.return_value = translation_service.DeleteGlossaryResponse(
            DELETION_RESULT_SAMPLE
        )
        gl_delete_method = mock_hook.return_value.delete_glossary
        gl_delete_method.return_value = sample_operation

        mock_hook.return_value.wait_for_operation_result.side_effect = lambda operation: operation.result()

        op = TranslateDeleteGlossaryOperator(
            task_id="task_id",
            glossary_id=GLOSSARY_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
        )
        context = mock.MagicMock()
        result = op.execute(context=context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        gl_delete_method.assert_called_once_with(
            glossary_id=GLOSSARY_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            timeout=TIMEOUT_VALUE,
            retry=DEFAULT,
            metadata=(),
        )
        assert result == DELETION_RESULT_SAMPLE
