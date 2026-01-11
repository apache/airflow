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

from collections.abc import Generator
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.operators.comprehend import (
    ComprehendBaseOperator,
    ComprehendCreateDocumentClassifierOperator,
    ComprehendStartPiiEntitiesDetectionJobOperator,
)
from airflow.providers.amazon.version_compat import NOTSET

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection

INPUT_DATA_CONFIG = {
    "S3Uri": "s3://input-data-comprehend/sample_data.txt",
    "InputFormat": "ONE_DOC_PER_LINE",
}
OUTPUT_DATA_CONFIG = {"S3Uri": "s3://output-data-comprehend/redacted_output/"}
LANGUAGE_CODE = "en"
ROLE_ARN = "role_arn"


class TestComprehendBaseOperator:
    @pytest.mark.parametrize("aws_conn_id", [None, NOTSET, "aws_test_conn"])
    @pytest.mark.parametrize("region_name", [None, NOTSET, "ca-central-1"])
    def test_initialize_comprehend_base_operator(self, aws_conn_id, region_name):
        op_kw = {"aws_conn_id": aws_conn_id, "region_name": region_name}
        op_kw = {k: v for k, v in op_kw.items() if v is not NOTSET}

        comprehend_base_op = ComprehendBaseOperator(
            task_id="comprehend_base_operator",
            input_data_config=INPUT_DATA_CONFIG,
            output_data_config=OUTPUT_DATA_CONFIG,
            language_code=LANGUAGE_CODE,
            data_access_role_arn=ROLE_ARN,
            **op_kw,
        )

        assert comprehend_base_op.aws_conn_id == (aws_conn_id if aws_conn_id is not NOTSET else "aws_default")
        assert comprehend_base_op.region_name == (region_name if region_name is not NOTSET else None)

    @mock.patch.object(ComprehendBaseOperator, "hook", new_callable=mock.PropertyMock)
    def test_initialize_comprehend_base_operator_hook(self, comprehend_base_operator_mock_hook):
        comprehend_base_op = ComprehendBaseOperator(
            task_id="comprehend_base_operator",
            input_data_config=INPUT_DATA_CONFIG,
            output_data_config=OUTPUT_DATA_CONFIG,
            language_code=LANGUAGE_CODE,
            data_access_role_arn=ROLE_ARN,
        )
        mocked_hook = mock.MagicMock(name="MockHook")
        mocked_client = mock.MagicMock(name="MockClient")
        mocked_hook.conn = mocked_client
        comprehend_base_operator_mock_hook.return_value = mocked_hook
        assert comprehend_base_op.client == mocked_client
        comprehend_base_operator_mock_hook.assert_called_once()

    def test_overwritten_conn_passed_to_hook(self):
        OVERWRITTEN_CONN = "new-conn-id"
        op = ComprehendBaseOperator(
            task_id="comprehend_base_operator",
            input_data_config=INPUT_DATA_CONFIG,
            output_data_config=OUTPUT_DATA_CONFIG,
            language_code=LANGUAGE_CODE,
            data_access_role_arn=ROLE_ARN,
            aws_conn_id=OVERWRITTEN_CONN,
        )
        assert op.hook.aws_conn_id == OVERWRITTEN_CONN

    def test_default_conn_passed_to_hook(self):
        DEFAULT_CONN = "aws_default"
        op = ComprehendBaseOperator(
            task_id="comprehend_base_operator",
            input_data_config=INPUT_DATA_CONFIG,
            output_data_config=OUTPUT_DATA_CONFIG,
            language_code=LANGUAGE_CODE,
            data_access_role_arn=ROLE_ARN,
        )
        assert op.hook.aws_conn_id == DEFAULT_CONN


class TestComprehendStartPiiEntitiesDetectionJobOperator:
    JOB_ID = "random-job-id-1234567"
    MODE = "ONLY_REDACTION"
    JOB_NAME = "TEST_START_PII_ENTITIES_DETECTION_JOB-1"
    DEFAULT_JOB_NAME_STARTS_WITH = "start_pii_entities_detection_job"
    REDACTION_CONFIG = {"PiiEntityTypes": ["NAME", "ADDRESS"], "MaskMode": "REPLACE_WITH_PII_ENTITY_TYPE"}

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(ComprehendHook, "conn") as _conn:
            _conn.start_pii_entities_detection_job.return_value = {"JobId": self.JOB_ID}
            yield _conn

    @pytest.fixture
    def comprehend_hook(self) -> Generator[ComprehendHook, None, None]:
        with mock_aws():
            hook = ComprehendHook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.operator = ComprehendStartPiiEntitiesDetectionJobOperator(
            task_id="start_pii_entities_detection_job",
            input_data_config=INPUT_DATA_CONFIG,
            output_data_config=OUTPUT_DATA_CONFIG,
            data_access_role_arn=ROLE_ARN,
            mode=self.MODE,
            language_code=LANGUAGE_CODE,
            start_pii_entities_kwargs={"JobName": self.JOB_NAME, "RedactionConfig": self.REDACTION_CONFIG},
        )
        self.operator.defer = mock.MagicMock()

    def test_init(self):
        assert self.operator.input_data_config == INPUT_DATA_CONFIG
        assert self.operator.output_data_config == OUTPUT_DATA_CONFIG
        assert self.operator.data_access_role_arn == ROLE_ARN
        assert self.operator.mode == self.MODE
        assert self.operator.language_code == LANGUAGE_CODE
        assert self.operator.start_pii_entities_kwargs.get("JobName") == self.JOB_NAME
        assert self.operator.start_pii_entities_kwargs.get("RedactionConfig") == self.REDACTION_CONFIG

    @mock.patch.object(ComprehendHook, "conn")
    def test_start_pii_entities_detection_job_name_starts_with_service_name(self, comprehend_mock_conn):
        self.op = ComprehendStartPiiEntitiesDetectionJobOperator(
            task_id="start_pii_entities_detection_job",
            input_data_config=INPUT_DATA_CONFIG,
            output_data_config=OUTPUT_DATA_CONFIG,
            data_access_role_arn=ROLE_ARN,
            mode=self.MODE,
            language_code=LANGUAGE_CODE,
            start_pii_entities_kwargs={"RedactionConfig": self.REDACTION_CONFIG},
        )
        self.op.wait_for_completion = False
        self.op.execute({})
        assert self.op.start_pii_entities_kwargs.get("JobName").startswith(self.DEFAULT_JOB_NAME_STARTS_WITH)
        comprehend_mock_conn.start_pii_entities_detection_job.assert_called_once_with(
            InputDataConfig=INPUT_DATA_CONFIG,
            OutputDataConfig=OUTPUT_DATA_CONFIG,
            Mode=self.MODE,
            DataAccessRoleArn=ROLE_ARN,
            LanguageCode=LANGUAGE_CODE,
            RedactionConfig=self.REDACTION_CONFIG,
            JobName=self.op.start_pii_entities_kwargs.get("JobName"),
        )

    @pytest.mark.parametrize(
        ("wait_for_completion", "deferrable"),
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(ComprehendHook, "get_waiter")
    def test_start_pii_entities_detection_job_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, comprehend_hook
    ):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == self.JOB_ID
        assert comprehend_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestComprehendCreateDocumentClassifierOperator:
    CLASSIFIER_ARN = (
        "arn:aws:comprehend:us-east-1:123456789012:document-classifier/insurance-classifier/version/v1"
    )
    ROLE_ARN = "arn:aws:iam::123456789012:role/ComprehendExecutionRole"
    INPUT_DATA_CONFIG = {
        "DataFormat": "COMPREHEND_CSV",
        "S3Uri": "s3://test/native-doc.csv",
        "DocumentType": "SEMI_STRUCTURED_DOCUMENT",
        "Documents": {"S3Uri": "s3://test/input-docs/"},
        "DocumentReaderConfig": {
            "DocumentReadAction": "TEXTRACT_DETECT_DOCUMENT_TEXT",
            "DocumentReadMode": "SERVICE_DEFAULT",
        },
    }

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(ComprehendHook, "conn") as _conn:
            _conn.create_document_classifier.return_value = {"DocumentClassifierArn": self.CLASSIFIER_ARN}
            yield _conn

    @pytest.fixture
    def comprehend_hook(self) -> Generator[ComprehendHook, None, None]:
        with mock_aws():
            hook = ComprehendHook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.operator = ComprehendCreateDocumentClassifierOperator(
            task_id="create_document_classifier",
            data_access_role_arn=self.ROLE_ARN,
            document_classifier_name="custom_test_document_classifier",
            input_data_config=self.INPUT_DATA_CONFIG,
            output_data_config={"S3Uri": "s3://test/training_output/"},
            language_code="en",
            mode="MULTI_CLASS",
            document_classifier_kwargs={"VersionName": "v1"},
        )
        self.operator.defer = mock.MagicMock()

    def test_init(self):
        assert self.operator.input_data_config == self.INPUT_DATA_CONFIG
        assert self.operator.data_access_role_arn == self.ROLE_ARN
        assert self.operator.document_classifier_name == "custom_test_document_classifier"
        assert self.operator.output_data_config == {"S3Uri": "s3://test/training_output/"}
        assert self.operator.mode == "MULTI_CLASS"
        assert self.operator.language_code == "en"
        assert self.operator.document_classifier_kwargs == {"VersionName": "v1"}
        assert self.operator.fail_on_warnings is False

    @mock.patch.object(ComprehendHook, "conn")
    def test_create_document_classifier(self, mock_conn):
        self.op = ComprehendCreateDocumentClassifierOperator(
            task_id="create_document_classifier",
            data_access_role_arn=self.ROLE_ARN,
            document_classifier_name="custom_test_document_classifier",
            input_data_config=self.INPUT_DATA_CONFIG,
            output_data_config={"S3Uri": "s3://test/training_output/"},
            language_code="en",
            mode="MULTI_CLASS",
            document_classifier_kwargs={"VersionName": "v1"},
        )
        self.op.wait_for_completion = False
        self.op.execute({})
        mock_conn.create_document_classifier.assert_called_once_with(
            DocumentClassifierName="custom_test_document_classifier",
            DataAccessRoleArn=self.ROLE_ARN,
            InputDataConfig=self.INPUT_DATA_CONFIG,
            OutputDataConfig={"S3Uri": "s3://test/training_output/"},
            LanguageCode="en",
            Mode="MULTI_CLASS",
            VersionName="v1",
        )

    @pytest.mark.parametrize(
        ("wait_for_completion", "deferrable"),
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(ComprehendHook, "get_waiter")
    def test_create_document_classifier_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, comprehend_hook
    ):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == self.CLASSIFIER_ARN
        assert comprehend_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    def test_template_fields(self):
        validate_template_fields(self.operator)
