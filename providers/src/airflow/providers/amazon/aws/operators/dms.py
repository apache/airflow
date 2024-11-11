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

from typing import TYPE_CHECKING, ClassVar, Sequence

from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DmsCreateTaskOperator(AwsBaseOperator[DmsHook]):
    """
    Creates AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsCreateTaskOperator`

    :param replication_task_id: Replication task id
    :param source_endpoint_arn: Source endpoint ARN
    :param target_endpoint_arn: Target endpoint ARN
    :param replication_instance_arn: Replication instance ARN
    :param table_mappings: Table mappings
    :param migration_type: Migration type ('full-load'|'cdc'|'full-load-and-cdc'), full-load by default.
    :param create_task_kwargs: Extra arguments for DMS replication task creation.
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

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields(
        "replication_task_id",
        "source_endpoint_arn",
        "target_endpoint_arn",
        "replication_instance_arn",
        "table_mappings",
        "migration_type",
        "create_task_kwargs",
    )
    template_fields_renderers: ClassVar[dict] = {
        "table_mappings": "json",
        "create_task_kwargs": "json",
    }

    def __init__(
        self,
        *,
        replication_task_id: str,
        source_endpoint_arn: str,
        target_endpoint_arn: str,
        replication_instance_arn: str,
        table_mappings: dict,
        migration_type: str = "full-load",
        create_task_kwargs: dict | None = None,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.replication_task_id = replication_task_id
        self.source_endpoint_arn = source_endpoint_arn
        self.target_endpoint_arn = target_endpoint_arn
        self.replication_instance_arn = replication_instance_arn
        self.migration_type = migration_type
        self.table_mappings = table_mappings
        self.create_task_kwargs = create_task_kwargs or {}
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        """
        Create AWS DMS replication task from Airflow.

        :return: replication task arn
        """
        task_arn = self.hook.create_replication_task(
            replication_task_id=self.replication_task_id,
            source_endpoint_arn=self.source_endpoint_arn,
            target_endpoint_arn=self.target_endpoint_arn,
            replication_instance_arn=self.replication_instance_arn,
            migration_type=self.migration_type,
            table_mappings=self.table_mappings,
            **self.create_task_kwargs,
        )
        self.log.info("DMS replication task(%s) is ready.", self.replication_task_id)

        return task_arn


class DmsDeleteTaskOperator(AwsBaseOperator[DmsHook]):
    """
    Deletes AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsDeleteTaskOperator`

    :param replication_task_arn: Replication task ARN
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

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("replication_task_arn")

    def __init__(self, *, replication_task_arn: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn

    def execute(self, context: Context):
        """
        Delete AWS DMS replication task from Airflow.

        :return: replication task arn
        """
        self.hook.delete_replication_task(replication_task_arn=self.replication_task_arn)
        self.log.info("DMS replication task(%s) has been deleted.", self.replication_task_arn)


class DmsDescribeTasksOperator(AwsBaseOperator[DmsHook]):
    """
    Describes AWS DMS replication tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsDescribeTasksOperator`

    :param describe_tasks_kwargs: Describe tasks command arguments
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

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("describe_tasks_kwargs")
    template_fields_renderers: ClassVar[dict[str, str]] = {"describe_tasks_kwargs": "json"}

    def __init__(self, *, describe_tasks_kwargs: dict | None = None, **kwargs):
        super().__init__(**kwargs)
        self.describe_tasks_kwargs = describe_tasks_kwargs or {}

    def execute(self, context: Context) -> tuple[str | None, list]:
        """
        Describe AWS DMS replication tasks from Airflow.

        :return: Marker and list of replication tasks
        """
        return self.hook.describe_replication_tasks(**self.describe_tasks_kwargs)


class DmsStartTaskOperator(AwsBaseOperator[DmsHook]):
    """
    Starts AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsStartTaskOperator`

    :param replication_task_arn: Replication task ARN
    :param start_replication_task_type: Replication task start type (default='start-replication')
        ('start-replication'|'resume-processing'|'reload-target')
    :param start_task_kwargs: Extra start replication task arguments
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

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields(
        "replication_task_arn",
        "start_replication_task_type",
        "start_task_kwargs",
    )
    template_fields_renderers = {"start_task_kwargs": "json"}

    def __init__(
        self,
        *,
        replication_task_arn: str,
        start_replication_task_type: str = "start-replication",
        start_task_kwargs: dict | None = None,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn
        self.start_replication_task_type = start_replication_task_type
        self.start_task_kwargs = start_task_kwargs or {}
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        """Start AWS DMS replication task from Airflow."""
        self.hook.start_replication_task(
            replication_task_arn=self.replication_task_arn,
            start_replication_task_type=self.start_replication_task_type,
            **self.start_task_kwargs,
        )
        self.log.info("DMS replication task(%s) is starting.", self.replication_task_arn)


class DmsStopTaskOperator(AwsBaseOperator[DmsHook]):
    """
    Stops AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsStopTaskOperator`

    :param replication_task_arn: Replication task ARN
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

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("replication_task_arn")

    def __init__(self, *, replication_task_arn: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn

    def execute(self, context: Context):
        """Stop AWS DMS replication task from Airflow."""
        self.hook.stop_replication_task(replication_task_arn=self.replication_task_arn)
        self.log.info("DMS replication task(%s) is stopping.", self.replication_task_arn)
