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


class DmsStartTaskOperator(BaseOperator):
    """Starts AWS DMS replication task."""

    template_fields = (
        'replication_task_arn',
        'start_replication_task_type',
        'start_task_kwargs',
    )
    template_ext = ()
    template_fields_renderers = {'start_task_kwargs': 'json'}

    @apply_defaults
    def __init__(
        self,
        *,
        replication_task_arn: str,
        start_replication_task_type: Optional[str] = 'start-replication',
        start_task_kwargs: Optional[dict] = None,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn
        self.start_replication_task_type = start_replication_task_type
        self.start_task_kwargs = start_task_kwargs
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        """
        Starts AWS DMS replication task from Airflow

        :return: replication task arn
        """
        dms_hook = DmsHook(aws_conn_id=self.aws_conn_id)

        dms_hook.start_replication_task(
            replication_task_arn=self.replication_task_arn,
            start_replication_task_type=self.start_replication_task_type,
            start_task_kwargs=self.start_task_kwargs,
        )
        self.log.info("DMS replication task(%s) is starting.", self.replication_task_arn)
