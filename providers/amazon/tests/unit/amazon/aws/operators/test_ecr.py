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

from airflow.providers.amazon.aws.hooks.ecr import EcrHook
from airflow.providers.amazon.aws.operators.ecr import (
    EcrCreateRepositoryOperator,
    EcrDeleteRepositoryOperator,
    EcrSetRepositoryPolicyOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

REPOSITORY_NAME = "test-repository"
REGISTRY_ID = "123456789012"
REPOSITORY_RESPONSE = {
    "repository": {
        "repositoryArn": f"arn:aws:ecr:us-east-1:{REGISTRY_ID}:repository/{REPOSITORY_NAME}",
        "registryId": REGISTRY_ID,
        "repositoryName": REPOSITORY_NAME,
        "repositoryUri": f"{REGISTRY_ID}.dkr.ecr.us-east-1.amazonaws.com/{REPOSITORY_NAME}",
    },
    "ResponseMetadata": {"HTTPStatusCode": 200},
}
POLICY_TEXT = '{"Version":"2012-10-17","Statement":[]}'
POLICY_RESPONSE = {
    "registryId": REGISTRY_ID,
    "repositoryName": REPOSITORY_NAME,
    "policyText": POLICY_TEXT,
    "ResponseMetadata": {"HTTPStatusCode": 200},
}


class TestEcrCreateRepositoryOperator:
    @pytest.mark.parametrize(
        ("operator_parameters", "boto3_parameters"),
        [
            pytest.param(
                {},
                {
                    "repositoryName": REPOSITORY_NAME,
                },
                id="required-parameters",
            ),
            pytest.param(
                {
                    "registry_id": REGISTRY_ID,
                    "tags": [{"Key": "environment", "Value": "test"}],
                    "image_tag_mutability": "IMMUTABLE_WITH_EXCLUSION",
                    "image_tag_mutability_exclusion_filters": [
                        {"filterType": "WILDCARD", "filter": "latest"}
                    ],
                    "image_scanning_configuration": {"scanOnPush": True},
                    "encryption_configuration": {
                        "encryptionType": "KMS",
                        "kmsKey": "arn:aws:kms:us-east-1:123456789012:key/test-key",
                    },
                },
                {
                    "repositoryName": REPOSITORY_NAME,
                    "registryId": REGISTRY_ID,
                    "tags": [{"Key": "environment", "Value": "test"}],
                    "imageTagMutability": "IMMUTABLE_WITH_EXCLUSION",
                    "imageTagMutabilityExclusionFilters": [{"filterType": "WILDCARD", "filter": "latest"}],
                    "imageScanningConfiguration": {"scanOnPush": True},
                    "encryptionConfiguration": {
                        "encryptionType": "KMS",
                        "kmsKey": "arn:aws:kms:us-east-1:123456789012:key/test-key",
                    },
                },
                id="all-parameters",
            ),
        ],
    )
    @mock.patch.object(EcrHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn, operator_parameters, boto3_parameters):
        mock_client = mock.MagicMock(spec=["create_repository"])
        mock_client.create_repository.return_value = REPOSITORY_RESPONSE
        mock_conn.return_value = mock_client
        operator = EcrCreateRepositoryOperator(
            task_id="create_repository",
            repository_name=REPOSITORY_NAME,
            **operator_parameters,
        )

        result = operator.execute({})

        mock_client.create_repository.assert_called_once_with(**boto3_parameters)
        assert result == REPOSITORY_RESPONSE

    def test_template_fields(self):
        operator = EcrCreateRepositoryOperator(
            task_id="create_repository",
            repository_name=REPOSITORY_NAME,
        )

        validate_template_fields(operator)


class TestEcrSetRepositoryPolicyOperator:
    @pytest.mark.parametrize(
        ("operator_parameters", "boto3_parameters"),
        [
            pytest.param(
                {},
                {
                    "repositoryName": REPOSITORY_NAME,
                    "policyText": POLICY_TEXT,
                    "force": False,
                },
                id="defaults",
            ),
            pytest.param(
                {"registry_id": REGISTRY_ID, "force": True},
                {
                    "registryId": REGISTRY_ID,
                    "repositoryName": REPOSITORY_NAME,
                    "policyText": POLICY_TEXT,
                    "force": True,
                },
                id="registry-and-force",
            ),
        ],
    )
    @mock.patch.object(EcrHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn, operator_parameters, boto3_parameters):
        mock_client = mock.MagicMock(spec=["set_repository_policy"])
        mock_client.set_repository_policy.return_value = POLICY_RESPONSE
        mock_conn.return_value = mock_client
        operator = EcrSetRepositoryPolicyOperator(
            task_id="set_repository_policy",
            repository_name=REPOSITORY_NAME,
            policy_text=POLICY_TEXT,
            **operator_parameters,
        )

        result = operator.execute({})

        mock_client.set_repository_policy.assert_called_once_with(**boto3_parameters)
        assert result == POLICY_RESPONSE

    def test_template_fields(self):
        operator = EcrSetRepositoryPolicyOperator(
            task_id="set_repository_policy",
            repository_name=REPOSITORY_NAME,
            policy_text=POLICY_TEXT,
        )

        validate_template_fields(operator)


class TestEcrDeleteRepositoryOperator:
    @pytest.mark.parametrize(
        ("operator_parameters", "boto3_parameters"),
        [
            pytest.param(
                {},
                {"repositoryName": REPOSITORY_NAME, "force": False},
                id="defaults",
            ),
            pytest.param(
                {"registry_id": REGISTRY_ID, "force": True},
                {"repositoryName": REPOSITORY_NAME, "registryId": REGISTRY_ID, "force": True},
                id="registry-and-force",
            ),
        ],
    )
    @mock.patch.object(EcrHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn, operator_parameters, boto3_parameters):
        mock_client = mock.MagicMock(spec=["delete_repository"])
        mock_client.delete_repository.return_value = REPOSITORY_RESPONSE
        mock_conn.return_value = mock_client
        operator = EcrDeleteRepositoryOperator(
            task_id="delete_repository",
            repository_name=REPOSITORY_NAME,
            **operator_parameters,
        )

        result = operator.execute({})

        mock_client.delete_repository.assert_called_once_with(**boto3_parameters)
        assert result == REPOSITORY_RESPONSE

    def test_template_fields(self):
        operator = EcrDeleteRepositoryOperator(
            task_id="delete_repository",
            repository_name=REPOSITORY_NAME,
        )

        validate_template_fields(operator)
