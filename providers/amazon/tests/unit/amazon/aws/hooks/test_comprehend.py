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

from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.common.compat.sdk import AirflowException


class TestComprehendHook:
    @pytest.mark.parametrize(
        ("test_hook", "service_name"),
        [pytest.param(ComprehendHook(), "comprehend", id="comprehend")],
    )
    def test_comprehend_hook(self, test_hook, service_name):
        comprehend_hook = ComprehendHook()
        assert comprehend_hook.conn is not None

    @mock.patch.object(ComprehendHook, "conn")
    def test_validate_document_classifier_training_status_for_success(self, mock_conn):
        mock_conn.describe_document_classifier.return_value = {
            "DocumentClassifierProperties": {
                "DocumentClassifierArn": "arn:aws:comprehend:us-east-1:123456789012:document-classifier/docs-clasf/version/v1",
                "LanguageCode": "en",
                "Status": "TRAINED",
                "Message": "",
                "SubmitTime": "2024-06-08T21:41:36.867Z",
                "EndTime": "2024-06-08T21:52:02.249Z",
                "TrainingStartTime": "2024-06-08T21:44:39.596Z",
                "TrainingEndTime": "2024-06-08T21:51:00.021Z",
                "InputDataConfig": {
                    "DataFormat": "COMPREHEND_CSV",
                    "S3Uri": "s3://test/native-doc.csv",
                    "DocumentType": "SEMI_STRUCTURED_DOCUMENT",
                    "Documents": {"S3Uri": "s3://test/input-docs/"},
                    "DocumentReaderConfig": {
                        "DocumentReadAction": "TEXTRACT_DETECT_DOCUMENT_TEXT",
                        "DocumentReadMode": "SERVICE_DEFAULT",
                    },
                },
                "OutputDataConfig": {
                    "S3Uri": "s3://test/training_output/183167903796-CLR-b56fd4cf0b5bcc11c9409dfb431cd585/output/output.tar.gz"
                },
                "ClassifierMetadata": {
                    "NumberOfLabels": 2,
                    "NumberOfTrainedDocuments": 16,
                    "NumberOfTestDocuments": 2,
                    "EvaluationMetrics": {
                        "Accuracy": 1,
                        "Precision": 1,
                        "Recall": 1,
                        "F1Score": 1,
                        "MicroPrecision": 1,
                        "MicroRecall": 1,
                        "MicroF1Score": 1,
                        "HammingLoss": 0,
                    },
                },
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/ComprehendExecutionRole",
                "Mode": "MULTI_CLASS",
                "VersionName": "v1",
            }
        }
        classifier_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/docs-clasf/version/v1"
        ComprehendHook().validate_document_classifier_training_status(document_classifier_arn=classifier_arn)

    @mock.patch.object(ComprehendHook, "conn")
    def test_validate_document_classifier_training_status_for_warning(self, mock_conn):
        mock_conn.describe_document_classifier.return_value = {
            "DocumentClassifierProperties": {
                "DocumentClassifierArn": "arn:aws:comprehend:us-east-1:123456789012:document-classifier/docs-clasf/version/v1",
                "LanguageCode": "en",
                "Status": "TRAINED_WITH_WARNING",
                "Message": "Unable to parse some documents. See details in the output S3 location. These documents have been skipped.",
                "SubmitTime": "2024-06-08T21:41:36.867Z",
                "EndTime": "2024-06-08T21:52:02.249Z",
                "TrainingStartTime": "2024-06-08T21:44:39.596Z",
                "TrainingEndTime": "2024-06-08T21:51:00.021Z",
                "InputDataConfig": {
                    "DataFormat": "COMPREHEND_CSV",
                    "S3Uri": "s3://test/native-doc.csv",
                    "DocumentType": "SEMI_STRUCTURED_DOCUMENT",
                    "Documents": {"S3Uri": "s3://test/input-docs/"},
                    "DocumentReaderConfig": {
                        "DocumentReadAction": "TEXTRACT_DETECT_DOCUMENT_TEXT",
                        "DocumentReadMode": "SERVICE_DEFAULT",
                    },
                },
                "OutputDataConfig": {
                    "S3Uri": "s3://test/training_output/183167903796-CLR-b56fd4cf0b5bcc11c9409dfb431cd585/output/output.tar.gz"
                },
                "ClassifierMetadata": {
                    "NumberOfLabels": 2,
                    "NumberOfTrainedDocuments": 16,
                    "NumberOfTestDocuments": 2,
                    "EvaluationMetrics": {
                        "Accuracy": 1,
                        "Precision": 1,
                        "Recall": 1,
                        "F1Score": 1,
                        "MicroPrecision": 1,
                        "MicroRecall": 1,
                        "MicroF1Score": 1,
                        "HammingLoss": 0,
                    },
                },
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/ComprehendExecutionRole",
                "Mode": "MULTI_CLASS",
                "VersionName": "v1",
            }
        }
        classifier_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/docs-clasf/version/v1"
        with pytest.raises(
            AirflowException, match="Warnings in AWS Comprehend document classifier training."
        ):
            ComprehendHook().validate_document_classifier_training_status(
                document_classifier_arn=classifier_arn, fail_on_warnings=True
            )
