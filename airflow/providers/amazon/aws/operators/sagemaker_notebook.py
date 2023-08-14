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

from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.sagemaker_notebook import SageMakerNotebookHook
from airflow.providers.amazon.aws.utils import trim_none_values
from airflow.providers.amazon.aws.utils.tags import format_tags

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SageMakerCreateNotebookOperator(BaseOperator):
    """
    Create a SageMaker notebook.

    More information regarding parameters of this operator can be found here
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker/client/create_notebook_instance.html.

    .. seealso:
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerCreateNotebookOperator`

    :param instance_name: The name of the notebook instance.
    :param instance_type: The type of instance to create.
    :param role_arn: The Amazon Resource Name (ARN) of the IAM role that SageMaker can assume to access
    :param volume_size_in_gb: Size in GB of the EBS root device volume of the notebook instance.
    :param volume_kms_key_id: The KMS key ID for the EBS root device volume.
    :param lifecycle_config_name: The name of the lifecycle configuration to associate with the notebook
    :param direct_internet_access: Whether to enable direct internet access for the notebook instance.
    :param root_access: Whether to give the notebook instance root access to the Amazon S3 bucket.
    :param wait_for_completion: Whether or not to wait for the notebook to be InService before returning
    :param create_instance_kwargs: Additional configuration options for the create call.
    :param config: Additional configuration options for the create call.
    :param aws_conn_id: The AWS connection ID to use.

    This operator returns The ARN of the created notebook.
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "instance_type",
        "role_arn",
        "volume_size_in_gb",
        "volume_kms_key_id",
        "lifecycle_config_name",
        "direct_internet_access",
        "root_access",
        "wait_for_completion",
        "create_instance_kwargs",
        "config",
    )

    ui_color = "#ff7300"

    def __init__(
        self,
        *,
        instance_name: str,
        instance_type: str,
        role_arn: str,
        volume_size_in_gb: int | None = None,
        volume_kms_key_id: str | None = None,
        lifecycle_config_name: str | None = None,
        direct_internet_access: str | None = None,
        root_access: str | None = None,
        create_instance_kwargs: dict[str, Any] = {},
        wait_for_completion: bool = True,
        config: dict = {},
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.instance_type = instance_type
        self.role_arn = role_arn
        self.volume_size_in_gb = volume_size_in_gb
        self.volume_kms_key_id = volume_kms_key_id
        self.lifecycle_config_name = lifecycle_config_name
        self.direct_internet_access = direct_internet_access
        self.root_access = root_access
        self.wait_for_completion = wait_for_completion
        self.config = config
        self.aws_conn_id = aws_conn_id
        self.create_instance_kwargs = create_instance_kwargs

        if "tags" in self.create_instance_kwargs and self.create_instance_kwargs["tags"] is not None:
            self.create_instance_kwargs["tags"] = format_tags(self.create_instance_kwargs["tags"])

        self.create_notebook_instance_kwargs = {
            "NotebookInstanceName": self.instance_name,
            "InstanceType": self.instance_type,
            "RoleArn": self.role_arn,
            "VolumeSizeInGB": self.volume_size_in_gb,
            "KmsKeyId": self.volume_kms_key_id,
            "LifecycleConfigName": self.lifecycle_config_name,
            "DirectInternetAccess": self.direct_internet_access,
            "RootAccess": self.root_access,
        }
        if len(self.create_instance_kwargs) > 0:
            self.create_notebook_instance_kwargs.update(self.create_instance_kwargs)

    @cached_property
    def hook(self) -> SageMakerNotebookHook:
        """Create and return SageMakerNotebookHook."""
        return SageMakerNotebookHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context):
        self.log.info("Creating SageMaker notebook %s.", self.instance_name)
        response = self.hook.conn.create_notebook_instance(
            **trim_none_values(self.create_notebook_instance_kwargs)
        )

        self.log.info("SageMaker notebook created: %s", response["NotebookInstanceArn"])

        if self.wait_for_completion:
            self.log.info("Waiting for SageMaker notebook %s to be in service", self.instance_name)
            waiter = self.hook.conn.get_waiter("notebook_instance_in_service")
            waiter.wait(NotebookInstanceName=self.instance_name)

        return response["NotebookInstanceArn"]


class SageMakerStopNotebookOperator(BaseOperator):
    """
    Stop a notebook instance.

    .. seealso:
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerStopNotebookOperator`

    :param instance_name: The name of the notebook instance to stop.
    :param config: Additional configuration options for the stop call.
    :param aws_conn_id: The AWS connection ID to use.
    """

    template_fields: Sequence[str] = ("instance_name", "wait_for_completion", "config")

    ui_color = "#ff7300"

    def __init__(
        self,
        instance_name: str,
        wait_for_completion: bool = True,
        config: dict = {},
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self) -> SageMakerNotebookHook:
        """Create and return SageMakerNotebookHook."""
        return SageMakerNotebookHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context):
        self.log.info("Stopping SageMaker notebook %s.", self.instance_name)
        self.hook.conn.stop_notebook_instance(NotebookInstanceName=self.instance_name)

        if self.wait_for_completion:
            self.log.info("Waiting for SageMaker notebook %s to stop", self.instance_name)
            self.hook.conn.get_waiter("notebook_instance_stopped").wait(
                NotebookInstanceName=self.instance_name
            )


class SageMakerDeleteNotebookOperator(BaseOperator):
    """

    Delete a notebook instance.

    .. seealso:
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerDeleteNotebookOperator`

    :param instance_name: The name of the notebook instance to delete.
    :param config: Additional configuration options for the delete call.
    :param wait_for_completion: Whether or not to wait for the notebook to delete before returning.
    :param aws_conn_id: The AWS connection ID to use.
    """

    template_fields: Sequence[str] = ("instance_name", "wait_for_completion", "config")

    ui_color = "#ff7300"

    def __init__(
        self,
        instance_name: str,
        wait_for_completion: bool = True,
        config: dict = {},
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.config = config
        self.aws_conn_id = aws_conn_id
        self.wait_for_completion = wait_for_completion

    @cached_property
    def hook(self) -> SageMakerNotebookHook:
        """Create and return SageMakerNotebookHook."""
        return SageMakerNotebookHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context):
        self.log.info("Deleting SageMaker notebook %s....", self.instance_name)
        self.hook.conn.delete_notebook_instance(NotebookInstanceName=self.instance_name)

        if self.wait_for_completion:
            self.log.info("Waiting for SageMaker notebook %s to delete...", self.instance_name)
            self.hook.conn.get_waiter("notebook_instance_deleted").wait(
                NotebookInstanceName=self.instance_name
            )


class SageMakerStartNoteBookOperator(BaseOperator):
    """

    Start a notebook instance.

    .. seealso:
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerStartNotebookOperator`

    :param instance_name: The name of the notebook instance to start.
    :param wait_for_completion: Whether or not to wait for notebook to be InService before returning
    :param config: Additional configuration options for the start call.
    :param aws_conn_id: The AWS connection ID to use.
    """

    template_fields: Sequence[str] = ("instance_name", "wait_for_completion", "config")

    ui_color = "#ff7300"

    def __init__(
        self,
        instance_name: str,
        wait_for_completion: bool = True,
        config: dict = {},
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.config = config
        self.aws_conn_id = aws_conn_id
        self.wait_for_completion = wait_for_completion

    @cached_property
    def hook(self) -> SageMakerNotebookHook:
        """Create and return SageMakerNotebookHook."""
        return SageMakerNotebookHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context):
        self.log.info("Starting SageMaker notebook %s....", self.instance_name)
        self.hook.conn.start_notebook_instance(NotebookInstanceName=self.instance_name)

        if self.wait_for_completion:
            self.log.info("Waiting for SageMaker notebook %s to start...", self.instance_name)
            self.hook.conn.get_waiter("notebook_instance_in_service").wait(
                NotebookInstanceName=self.instance_name
            )
