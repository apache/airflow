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
from typing import TYPE_CHECKING, Generator
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentHook, BedrockHook, BedrockRuntimeHook
from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockCreateDataSourceOperator,
    BedrockCreateKnowledgeBaseOperator,
    BedrockCreateProvisionedModelThroughputOperator,
    BedrockCustomizeModelOperator,
    BedrockIngestDataOperator,
    BedrockInvokeModelOperator,
    BedrockRaGOperator,
)

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection


class TestBedrockInvokeModelOperator:
    MODEL_ID = "meta.llama2-13b-chat-v1"
    TEST_PROMPT = "A very important question."
    GENERATED_RESPONSE = "An important answer."

    @pytest.fixture
    def mock_runtime_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockRuntimeHook, "conn") as _conn:
            _conn.invoke_model.return_value["body"].read.return_value = json.dumps(
                {
                    "generation": self.GENERATED_RESPONSE,
                    "prompt_token_count": len(self.TEST_PROMPT),
                    "generation_token_count": len(self.GENERATED_RESPONSE),
                    "stop_reason": "stop",
                }
            )
            yield _conn

    @pytest.fixture
    def runtime_hook(self) -> Generator[BedrockRuntimeHook, None, None]:
        with mock_aws():
            yield BedrockRuntimeHook(aws_conn_id="aws_default")

    def test_invoke_model_prompt_good_combinations(self, mock_runtime_conn):
        operator = BedrockInvokeModelOperator(
            task_id="test_task",
            model_id=self.MODEL_ID,
            input_data={"input_data": {"prompt": self.TEST_PROMPT}},
        )

        response = operator.execute({})

        assert response["generation"] == self.GENERATED_RESPONSE


class TestBedrockCustomizeModelOperator:
    CUSTOMIZE_JOB_ARN = "valid_arn"
    CUSTOMIZE_JOB_NAME = "testModelJob"

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockHook, "conn") as _conn:
            _conn.create_model_customization_job.return_value = {
                "ResponseMetadata": {"HTTPStatusCode": 201},
                "jobArn": self.CUSTOMIZE_JOB_ARN,
            }
            _conn.get_model_customization_job.return_value = {
                "jobName": self.CUSTOMIZE_JOB_NAME,
                "status": "InProgress",
            }
            yield _conn

    @pytest.fixture
    def bedrock_hook(self) -> Generator[BedrockHook, None, None]:
        with mock_aws():
            hook = BedrockHook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.operator = BedrockCustomizeModelOperator(
            task_id="test_task",
            job_name=self.CUSTOMIZE_JOB_NAME,
            custom_model_name="testModelName",
            role_arn="valid_arn",
            base_model_id="base_model_id",
            hyperparameters={
                "epochCount": "1",
                "batchSize": "1",
                "learningRate": ".0005",
                "learningRateWarmupSteps": "0",
            },
            training_data_uri="s3://uri",
            output_data_uri="s3://uri/output",
        )
        self.operator.defer = mock.MagicMock()

    @pytest.mark.parametrize(
        "wait_for_completion, deferrable",
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(BedrockHook, "get_waiter")
    def test_customize_model_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, bedrock_hook
    ):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == self.CUSTOMIZE_JOB_ARN
        assert bedrock_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    conflict_msg = "The provided job name is currently in use."
    conflict_exception = ClientError(
        error_response={"Error": {"Message": conflict_msg, "Code": "ValidationException"}},
        operation_name="UnitTest",
    )
    success = {"ResponseMetadata": {"HTTPStatusCode": 201}, "jobArn": CUSTOMIZE_JOB_ARN}

    @pytest.mark.parametrize(
        "side_effect, ensure_unique_name",
        [
            pytest.param([conflict_exception, success], True, id="conflict_and_ensure_unique"),
            pytest.param([conflict_exception, success], False, id="conflict_and_not_ensure_unique"),
            pytest.param(
                [conflict_exception, conflict_exception, success],
                True,
                id="multiple_conflict_and_ensure_unique",
            ),
            pytest.param(
                [conflict_exception, conflict_exception, success],
                False,
                id="multiple_conflict_and_not_ensure_unique",
            ),
            pytest.param([success], True, id="no_conflict_and_ensure_unique"),
            pytest.param([success], False, id="no_conflict_and_not_ensure_unique"),
        ],
    )
    @mock.patch.object(BedrockHook, "get_waiter")
    def test_ensure_unique_job_name(self, _, side_effect, ensure_unique_name, mock_conn, bedrock_hook):
        mock_conn.create_model_customization_job.side_effect = side_effect
        expected_call_count = len(side_effect) if ensure_unique_name else 1
        self.operator.wait_for_completion = False

        response = self.operator.execute({})

        assert response == self.CUSTOMIZE_JOB_ARN
        mock_conn.create_model_customization_job.call_count == expected_call_count
        bedrock_hook.get_waiter.assert_not_called()
        self.operator.defer.assert_not_called()


class TestBedrockCreateProvisionedModelThroughputOperator:
    MODEL_ARN = "testProvisionedModelArn"

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockHook, "conn") as _conn:
            _conn.create_provisioned_model_throughput.return_value = {"provisionedModelArn": self.MODEL_ARN}
            yield _conn

    @pytest.fixture
    def bedrock_hook(self) -> Generator[BedrockHook, None, None]:
        with mock_aws():
            hook = BedrockHook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.operator = BedrockCreateProvisionedModelThroughputOperator(
            task_id="provision_throughput",
            model_units=1,
            provisioned_model_name="testProvisionedModelName",
            model_id="test_model_arn",
        )
        self.operator.defer = mock.MagicMock()

    @pytest.mark.parametrize(
        "wait_for_completion, deferrable",
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(BedrockHook, "get_waiter")
    def test_provisioned_model_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, bedrock_hook
    ):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == self.MODEL_ARN
        assert bedrock_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable


class TestBedrockCreateKnowledgeBaseOperator:
    KNOWLEDGE_BASE_ID = "knowledge_base_id"

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockAgentHook, "conn") as _conn:
            _conn.create_knowledge_base.return_value = {
                "knowledgeBase": {"knowledgeBaseId": self.KNOWLEDGE_BASE_ID}
            }
            yield _conn

    @pytest.fixture
    def bedrock_hook(self) -> Generator[BedrockAgentHook, None, None]:
        with mock_aws():
            hook = BedrockAgentHook()
            yield hook

    def setup_method(self):
        self.operator = BedrockCreateKnowledgeBaseOperator(
            task_id="create_knowledge_base",
            name=self.KNOWLEDGE_BASE_ID,
            embedding_model_arn="arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1",
            role_arn="role-arn",
            storage_config={
                "type": "OPENSEARCH_SERVERLESS",
                "opensearchServerlessConfiguration": {
                    "collectionArn": "collection_arn",
                    "vectorIndexName": "index_name",
                    "fieldMapping": {
                        "vectorField": "vector",
                        "textField": "text",
                        "metadataField": "text-metadata",
                    },
                },
            },
        )
        self.operator.defer = mock.MagicMock()

    @pytest.mark.parametrize(
        "wait_for_completion, deferrable",
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(BedrockAgentHook, "get_waiter")
    def test_create_knowledge_base_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, bedrock_hook
    ):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == self.KNOWLEDGE_BASE_ID
        assert bedrock_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    def test_returns_id(self, mock_conn):
        self.operator.wait_for_completion = False
        result = self.operator.execute({})

        assert result == self.KNOWLEDGE_BASE_ID


class TestBedrockCreateDataSourceOperator:
    DATA_SOURCE_ID = "data_source_id"

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockAgentHook, "conn") as _conn:
            _conn.create_data_source.return_value = {"dataSource": {"dataSourceId": self.DATA_SOURCE_ID}}
            yield _conn

    @pytest.fixture
    def bedrock_hook(self) -> Generator[BedrockAgentHook, None, None]:
        with mock_aws():
            hook = BedrockAgentHook()
            yield hook

    def setup_method(self):
        self.operator = BedrockCreateDataSourceOperator(
            task_id="create_data_source",
            name=self.DATA_SOURCE_ID,
            knowledge_base_id="test_knowledge_base_id",
            bucket_name="test_bucket",
        )

    def test_id_returned(self, mock_conn):
        result = self.operator.execute({})

        assert result == self.DATA_SOURCE_ID


class TestBedrockIngestDataOperator:
    INGESTION_JOB_ID = "ingestion_job_id"

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockAgentHook, "conn") as _conn:
            _conn.start_ingestion_job.return_value = {
                "ingestionJob": {"ingestionJobId": self.INGESTION_JOB_ID}
            }
            yield _conn

    @pytest.fixture
    def bedrock_hook(self) -> Generator[BedrockAgentHook, None, None]:
        with mock_aws():
            hook = BedrockAgentHook()
            yield hook

    def setup_method(self):
        self.operator = BedrockIngestDataOperator(
            task_id="create_data_source",
            data_source_id="data_source_id",
            knowledge_base_id="knowledge_base_id",
            wait_for_completion=False,
        )

    def test_id_returned(self, mock_conn):
        result = self.operator.execute({})

        assert result == self.INGESTION_JOB_ID


class TestBedrockRaGOperator:
    VECTOR_SEARCH_CONFIG = {"filter": {"equals": {"key": "some key", "value": "some value"}}}
    KNOWLEDGE_BASE_ID = "knowledge_base_id"
    SOURCES = [{"sourceType": "S3", "s3Location": "bucket"}]
    MODEL_ARN = "model arn"

    @pytest.mark.parametrize(
        "source_type, vector_search_config, knowledge_base_id, sources, expect_success",
        [
            pytest.param(
                "invalid_source_type",
                None,
                None,
                None,
                False,
                id="invalid_source_type",
            ),
            pytest.param(
                "KNOWLEDGE_BASE",
                VECTOR_SEARCH_CONFIG,
                None,
                None,
                False,
                id="KNOWLEDGE_BASE_without_knowledge_base_id_fails",
            ),
            pytest.param(
                "KNOWLEDGE_BASE",
                None,
                KNOWLEDGE_BASE_ID,
                None,
                True,
                id="KNOWLEDGE_BASE_passes",
            ),
            pytest.param(
                "KNOWLEDGE_BASE",
                VECTOR_SEARCH_CONFIG,
                KNOWLEDGE_BASE_ID,
                SOURCES,
                False,
                id="KNOWLEDGE_BASE_with_sources_fails",
            ),
            pytest.param(
                "KNOWLEDGE_BASE",
                VECTOR_SEARCH_CONFIG,
                KNOWLEDGE_BASE_ID,
                None,
                True,
                id="KNOWLEDGE_BASE_with_vector_config_passes",
            ),
            pytest.param(
                "EXTERNAL_SOURCES",
                VECTOR_SEARCH_CONFIG,
                None,
                SOURCES,
                False,
                id="EXTERNAL_SOURCES_with_search_config_fails",
            ),
            pytest.param(
                "EXTERNAL_SOURCES",
                None,
                KNOWLEDGE_BASE_ID,
                SOURCES,
                False,
                id="EXTERNAL_SOURCES_with_knohwledge_base_id_fails",
            ),
            pytest.param(
                "EXTERNAL_SOURCES",
                None,
                None,
                SOURCES,
                True,
                id="EXTERNAL_SOURCES_with_sources_passes",
            ),
        ],
    )
    def test_input_validation(
        self, source_type, vector_search_config, knowledge_base_id, sources, expect_success
    ):
        op = BedrockRaGOperator(
            task_id="test_rag",
            input="some text prompt",
            source_type=source_type,
            model_arn=self.MODEL_ARN,
            knowledge_base_id=knowledge_base_id,
            vector_search_config=vector_search_config,
            sources=sources,
        )

        if expect_success:
            op.validate_inputs()
        else:
            with pytest.raises(AttributeError):
                op.validate_inputs()

    @pytest.mark.parametrize(
        "prompt_template",
        [
            pytest.param(None, id="no_prompt_template"),
            pytest.param("valid template", id="prompt_template_provided"),
        ],
    )
    def test_knowledge_base_build_rag_config(self, prompt_template):
        expected_source_type = "KNOWLEDGE_BASE"
        op = BedrockRaGOperator(
            task_id="test_rag",
            input="some text prompt",
            source_type=expected_source_type,
            model_arn=self.MODEL_ARN,
            knowledge_base_id=self.KNOWLEDGE_BASE_ID,
            vector_search_config=self.VECTOR_SEARCH_CONFIG,
            prompt_template=prompt_template,
        )
        expected_config_without_template = {
            "knowledgeBaseId": self.KNOWLEDGE_BASE_ID,
            "modelArn": self.MODEL_ARN,
            "retrievalConfiguration": {"vectorSearchConfiguration": self.VECTOR_SEARCH_CONFIG},
        }
        expected_config_template = {
            "generationConfiguration": {"promptTemplate": {"textPromptTemplate": prompt_template}}
        }
        config = op.build_rag_config()

        assert len(config.keys()) == 2
        assert config.get("knowledgeBaseConfiguration", False)
        assert config["type"] == expected_source_type

        if not prompt_template:
            assert config["knowledgeBaseConfiguration"] == expected_config_without_template
        else:
            assert config["knowledgeBaseConfiguration"] == {
                **expected_config_without_template,
                **expected_config_template,
            }

    @pytest.mark.parametrize(
        "prompt_template",
        [
            pytest.param(None, id="no_prompt_template"),
            pytest.param("valid template", id="prompt_template_provided"),
        ],
    )
    def test_external_sources_build_rag_config(self, prompt_template):
        expected_source_type = "EXTERNAL_SOURCES"
        op = BedrockRaGOperator(
            task_id="test_rag",
            input="some text prompt",
            source_type=expected_source_type,
            model_arn=self.MODEL_ARN,
            sources=self.SOURCES,
            prompt_template=prompt_template,
        )
        expected_config_without_template = {
            "modelArn": self.MODEL_ARN,
            "sources": self.SOURCES,
        }
        expected_config_template = {
            "generationConfiguration": {"promptTemplate": {"textPromptTemplate": prompt_template}}
        }
        config = op.build_rag_config()

        assert len(config.keys()) == 2
        assert config.get("externalSourcesConfiguration", False)
        assert config["type"] == expected_source_type

        if not prompt_template:
            assert config["externalSourcesConfiguration"] == expected_config_without_template
        else:
            assert config["externalSourcesConfiguration"] == {
                **expected_config_without_template,
                **expected_config_template,
            }
