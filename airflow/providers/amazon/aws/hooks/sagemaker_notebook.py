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

from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.utils import trim_none_values
from airflow.providers.amazon.aws.utils.tags import format_tags


class SageMakerNotebookHook(AwsBaseHook):
    """Interact with Amazon SageMaker to execute notebooks.

    Provide thick wrapper around
    :external+boto3:py:class:`boto3.client('sagemaker') <SageMaker.Client>`

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "sagemaker"
        super().__init__(*args, **kwargs)

    def create_instance(
        self,
        instance_name: str,
        instance_type: str,
        role_arn: str,
        volume_size_in_gb: int | None = None,
        volume_kms_key_id: str | None = None,
        tags: dict = {},
        subnet_id: str | None = None,
        security_group_ids: list = [],
        lifecycle_config_name: str | None = None,
        direct_internet_access: str | None = None,
        accelerator_types: list = [],
        default_code_repo: list = [],
        additional_code_repos: list = [],
        root_access: str | None = None,
        platform_id: str | None = None,
        imds_config: dict | None = None,
        **kwargs: Any,
    ) -> dict:
        """Create a SageMaker notebook instance.

        :param instance_name: The name of the notebook instance.
        :param instance_type: The type of instance to create. For example, 'ml.t2.medium'.
        :param image_uri: The Amazon EC2 Image URI for the SageMaker image to use.
        :param role_arn: The Amazon Resource Name (ARN) of the IAM role to associate with the notebook
            instance.
        :param volume_size_in_gb: The size of the EBS volume to attach to the notebook instance.
        :param volume_kms_key_id: The KMS key ID to use when creating the notebook instance.
        :param tags: A list of tags to associate with the notebook instance.
        :param subnet_id: The ID of the subnet in which to launch the instance.
        :param security_group_ids: A list of security groups to associate with the notebook instance.

        :param lifecycle_config_name: The name of the lifecycle configuration to associate with the notebook
            instance.
        :param direct_internet_access: Whether to enable direct internet access for the notebook instance.
        :param accelerator_types: The list of Elastic Inference (EI) accelerator types to associate with the
            notebook instance.
        :param default_code_repo: The URL of the Git repository that contains the default code for a notebook
            instance.
        :param additional_code_repos: A list of URLs for Git repositories that contain custom code for a
            notebook instance.
        :param root_access: Whether to give the notebook instance root access to the Amazon S3 bucket.
        :param platform_id: The ID of the platform.
        :param imds_config: The configuration for the instance metadata service.
        :param config: Additional configuration options for the create call.
        :param aws_conn_id: The AWS connection ID to use.

        :return: A dict containing the information about the create notebook instance call.
        """
        if tags is not None:
            tags = format_tags(tags)

        create_notebook_instance_kwargs = {
            "NotebookInstanceName": instance_name,
            "InstanceType": instance_type,
            "RoleArn": role_arn,
            "Tags": tags,
            "VolumeSizeInGB": volume_size_in_gb,
            "KmsKeyId": volume_kms_key_id,
            "SubnetId": subnet_id,
            "SecurityGroupIds": security_group_ids,
            "LifecycleConfigName": lifecycle_config_name,
            "DirectInternetAccess": direct_internet_access,
            "AcceleratorTypes": accelerator_types,
            "DefaultCodeRepository": default_code_repo,
            "AdditionalCodeRepositories": additional_code_repos,
            "RootAccess": root_access,
            "PlatformIdentifier": platform_id,
        }

        print(f"trimmed values are: {trim_none_values(create_notebook_instance_kwargs)}")
        test = trim_none_values(create_notebook_instance_kwargs)
        resp = self.conn.create_notebook_instance(**test, **kwargs)
        return resp

    def start_instance(self, *, instance_name: str, **kwargs: Any) -> None:
        """Start a notebook instance.

        :param instance_name: The name of the notebook instance.
        :param wait_for_completion: Whether to wait for the start to complete.
        :param kwargs: Additional arguments for the start notebook instance call.
        :return: A dict containing the information about the start notebook instance call.
        """
        resp = self.conn.start_notebook_instance(NotebookInstanceName=instance_name, **kwargs)

        return resp

    def stop_instance(self, *, instance_name: str, **kwargs: Any) -> None:
        """Stop a notebook instance.

        :param instance_name: The name of the notebook instance.
        :param wait_for_completion: Whether to wait for the stop to complete.
        :param kwargs: Additional arguments for the stop notebook instance call.
        :return: A dict containing the information about the stop notebook instance call.
        """
        resp = self.conn.stop_notebook_instance(NotebookInstanceName=instance_name, **kwargs)

        return resp

    def delete_instance(self, *, instance_name: str, **kwargs: Any) -> None:
        """Delete a notebook instance.

        :param instance_name: The name of the notebook instance.
        :param kwargs: Additional arguments for the delete notebook instance call.
        :return: A dict containing the information about the delete notebook instance call.
        """
        resp = self.conn.delete_notebook_instance(NotebookInstanceName=instance_name, **kwargs)

        return resp
