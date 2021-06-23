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

from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.utils.decorators import apply_defaults


class DmsCreateTaskOperator(BaseOperator):
    """
    Creates AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsCreateTaskOperator`

    :param replication_task_id: Replication task id
    :type replication_task_id: str
    :param source_endpoint_arn: Source endpoint ARN
    :type source_endpoint_arn: str
    :param target_endpoint_arn: Target endpoint ARN
    :type target_endpoint_arn: str
    :param replication_instance_arn: Replication instance ARN
    :type replication_instance_arn: str
    :param table_mappings: Table mappings
    :type table_mappings: dict
    :param migration_type: Migration type ('full-load'|'cdc'|'full-load-and-cdc'), full-load by default.
    :type migration_type: str
    :param create_task_kwargs: Extra arguments for DMS replication task creation.
    :type create_task_kwargs: Optional[dict]
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :type aws_conn_id: Optional[str]
    """

    template_fields = (
        'replication_task_id',
        'source_endpoint_arn',
        'target_endpoint_arn',
        'replication_instance_arn',
        'table_mappings',
        'migration_type',
        'create_task_kwargs',
    )
    template_ext = ()
    template_fields_renderers = {
        "table_mappings": "json",
        "create_task_kwargs": "json",
    }

    @apply_defaults
    def __init__(
        self,
        *,
        replication_task_id: str,
        source_endpoint_arn: str,
        target_endpoint_arn: str,
        replication_instance_arn: str,
        table_mappings: dict,
        migration_type: Optional[str] = 'full-load',
        create_task_kwargs: Optional[dict] = None,
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

    def execute(self, context):
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
