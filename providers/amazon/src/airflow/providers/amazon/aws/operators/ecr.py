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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.amazon.aws.hooks.ecr import EcrHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.sdk import Context


class EcrCreateRepositoryOperator(AwsBaseOperator[EcrHook]):
    """
    Create an Amazon ECR repository.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcrCreateRepositoryOperator`

    :param repository_name: The name of the repository to create. (templated)
    :param registry_id: The AWS account ID associated with the registry. (templated)
    :param tags: Metadata to apply to the repository. (templated)
    :param image_tag_mutability: The tag mutability setting for the repository. (templated)
    :param image_tag_mutability_exclusion_filters: Filters that override the repository's
        image tag mutability setting. (templated)
    :param image_scanning_configuration: The image scanning configuration for the repository. (templated)
    :param encryption_configuration: The encryption configuration for the repository. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = EcrHook
    template_fields: Sequence[str] = aws_template_fields(
        "repository_name",
        "registry_id",
        "tags",
        "image_tag_mutability",
        "image_tag_mutability_exclusion_filters",
        "image_scanning_configuration",
        "encryption_configuration",
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        "tags": "json",
        "image_tag_mutability_exclusion_filters": "json",
        "image_scanning_configuration": "json",
        "encryption_configuration": "json",
    }

    def __init__(
        self,
        *,
        repository_name: str,
        registry_id: str | None = None,
        tags: list[dict[str, str]] | None = None,
        image_tag_mutability: str | None = None,
        image_tag_mutability_exclusion_filters: list[dict[str, str]] | None = None,
        image_scanning_configuration: dict[str, bool] | None = None,
        encryption_configuration: dict[str, str] | None = None,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.repository_name = repository_name
        self.registry_id = registry_id
        self.tags = tags
        self.image_tag_mutability = image_tag_mutability
        self.image_tag_mutability_exclusion_filters = image_tag_mutability_exclusion_filters
        self.image_scanning_configuration = image_scanning_configuration
        self.encryption_configuration = encryption_configuration

    def execute(self, context: Context) -> dict[str, Any]:
        self.log.info("Creating Amazon ECR repository %s", self.repository_name)
        response = self.hook.conn.create_repository(
            **prune_dict(
                {
                    "registryId": self.registry_id,
                    "repositoryName": self.repository_name,
                    "tags": self.tags,
                    "imageTagMutability": self.image_tag_mutability,
                    "imageTagMutabilityExclusionFilters": self.image_tag_mutability_exclusion_filters,
                    "imageScanningConfiguration": self.image_scanning_configuration,
                    "encryptionConfiguration": self.encryption_configuration,
                }
            )
        )
        self.log.info("Created Amazon ECR repository %s", self.repository_name)
        return response


class EcrSetRepositoryPolicyOperator(AwsBaseOperator[EcrHook]):
    """
    Set the repository policy for an Amazon ECR repository.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcrSetRepositoryPolicyOperator`

    :param repository_name: The name of the repository to receive the policy. (templated)
    :param policy_text: The JSON repository policy text to apply. (templated)
    :param registry_id: The AWS account ID associated with the registry. (templated)
    :param force: Whether to replace an existing policy that prevents setting a new policy. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = EcrHook
    template_fields: Sequence[str] = aws_template_fields(
        "repository_name", "policy_text", "registry_id", "force"
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {"policy_text": "json"}

    def __init__(
        self,
        *,
        repository_name: str,
        policy_text: str,
        registry_id: str | None = None,
        force: bool = False,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.repository_name = repository_name
        self.policy_text = policy_text
        self.registry_id = registry_id
        self.force = force

    def execute(self, context: Context) -> dict[str, Any]:
        self.log.info("Setting repository policy for Amazon ECR repository %s", self.repository_name)
        response = self.hook.conn.set_repository_policy(
            **prune_dict(
                {
                    "registryId": self.registry_id,
                    "repositoryName": self.repository_name,
                    "policyText": self.policy_text,
                    "force": self.force,
                }
            )
        )
        self.log.info("Set repository policy for Amazon ECR repository %s", self.repository_name)
        return response


class EcrDeleteRepositoryOperator(AwsBaseOperator[EcrHook]):
    """
    Delete an Amazon ECR repository.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcrDeleteRepositoryOperator`

    :param repository_name: The name of the repository to delete. (templated)
    :param registry_id: The AWS account ID associated with the registry. (templated)
    :param force: Whether to delete the repository when it contains images. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = EcrHook
    template_fields: Sequence[str] = aws_template_fields("repository_name", "registry_id", "force")

    def __init__(
        self,
        *,
        repository_name: str,
        registry_id: str | None = None,
        force: bool = False,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.repository_name = repository_name
        self.registry_id = registry_id
        self.force = force

    def execute(self, context: Context) -> dict[str, Any]:
        self.log.info("Deleting Amazon ECR repository %s", self.repository_name)
        response = self.hook.conn.delete_repository(
            **prune_dict(
                {
                    "registryId": self.registry_id,
                    "repositoryName": self.repository_name,
                    "force": self.force,
                }
            )
        )
        self.log.info("Deleted Amazon ECR repository %s", self.repository_name)
        return response
