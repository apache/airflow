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

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.dms import DmsHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DmsCreateTaskOperator(BaseOperator):
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
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    """

    template_fields: Sequence[str] = (
        'replication_task_id',
        'source_endpoint_arn',
        'target_endpoint_arn',
        'replication_instance_arn',
        'table_mappings',
        'migration_type',
        'create_task_kwargs',
    )
    template_ext: Sequence[str] = ()
    template_fields_renderers = {
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
        migration_type: str = 'full-load',
        create_task_kwargs: dict | None = None,
        aws_conn_id: str = 'aws_default',
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
        Creates AWS DMS replication task from Airflow

        :return: replication task arn
        """
        dms_hook = DmsHook(aws_conn_id=self.aws_conn_id)

        task_arn = dms_hook.create_replication_task(
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


class DmsDeleteTaskOperator(BaseOperator):
    """
    Deletes AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsDeleteTaskOperator`

    :param replication_task_arn: Replication task ARN
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    """

    template_fields: Sequence[str] = ('replication_task_arn',)
    template_ext: Sequence[str] = ()
    template_fields_renderers: dict[str, str] = {}

    def __init__(
        self,
        *,
        replication_task_arn: str | None = None,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        """
        Deletes AWS DMS replication task from Airflow

        :return: replication task arn
        """
        dms_hook = DmsHook(aws_conn_id=self.aws_conn_id)
        dms_hook.delete_replication_task(replication_task_arn=self.replication_task_arn)
        self.log.info("DMS replication task(%s) has been deleted.", self.replication_task_arn)


class DmsDescribeTasksOperator(BaseOperator):
    """
    Describes AWS DMS replication tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsDescribeTasksOperator`

    :param describe_tasks_kwargs: Describe tasks command arguments
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    """

    template_fields: Sequence[str] = ('describe_tasks_kwargs',)
    template_ext: Sequence[str] = ()
    template_fields_renderers: dict[str, str] = {'describe_tasks_kwargs': 'json'}

    def __init__(
        self,
        *,
        describe_tasks_kwargs: dict | None = None,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.describe_tasks_kwargs = describe_tasks_kwargs or {}
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        """
        Describes AWS DMS replication tasks from Airflow

        :return: Marker and list of replication tasks
        :rtype: (Optional[str], list)
        """
        dms_hook = DmsHook(aws_conn_id=self.aws_conn_id)
        return dms_hook.describe_replication_tasks(**self.describe_tasks_kwargs)


class DmsStartTaskOperator(BaseOperator):
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
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    """

    template_fields: Sequence[str] = (
        'replication_task_arn',
        'start_replication_task_type',
        'start_task_kwargs',
    )
    template_ext: Sequence[str] = ()
    template_fields_renderers = {'start_task_kwargs': 'json'}

    def __init__(
        self,
        *,
        replication_task_arn: str,
        start_replication_task_type: str = 'start-replication',
        start_task_kwargs: dict | None = None,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn
        self.start_replication_task_type = start_replication_task_type
        self.start_task_kwargs = start_task_kwargs or {}
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        """
        Starts AWS DMS replication task from Airflow

        :return: replication task arn
        """
        dms_hook = DmsHook(aws_conn_id=self.aws_conn_id)

        dms_hook.start_replication_task(
            replication_task_arn=self.replication_task_arn,
            start_replication_task_type=self.start_replication_task_type,
            **self.start_task_kwargs,
        )
        self.log.info("DMS replication task(%s) is starting.", self.replication_task_arn)


class DmsStopTaskOperator(BaseOperator):
    """
    Stops AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsStopTaskOperator`

    :param replication_task_arn: Replication task ARN
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    """

    template_fields: Sequence[str] = ('replication_task_arn',)
    template_ext: Sequence[str] = ()
    template_fields_renderers: dict[str, str] = {}

    def __init__(
        self,
        *,
        replication_task_arn: str | None = None,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        """
        Stops AWS DMS replication task from Airflow

        :return: replication task arn
        """
        dms_hook = DmsHook(aws_conn_id=self.aws_conn_id)
        dms_hook.stop_replication_task(replication_task_arn=self.replication_task_arn)
        self.log.info("DMS replication task(%s) is stopping.", self.replication_task_arn)
