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


class DmsDescribeTasksOperator(BaseOperator):
    """
    Describes AWS DMS replication tasks.

    :param describe_tasks_kwargs: Describe tasks command arguments
    :type describe_tasks_kwargs: Optional[dict]
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :type aws_conn_id: Optional[str]
    """

    template_fields = ('describe_tasks_kwargs',)
    template_ext = ()
    template_fields_renderers = {'describe_tasks_kwargs': 'json'}

    @apply_defaults
    def __init__(
        self,
        *,
        describe_tasks_kwargs: Optional[dict] = None,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.describe_tasks_kwargs = describe_tasks_kwargs or {}
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        """
        Describes AWS DMS replication tasks from Airflow

        :return: Marker and list of replication tasks
        :rtype: (str, list)
        """
        dms_hook = DmsHook(aws_conn_id=self.aws_conn_id)
        return dms_hook.describe_replication_tasks(**self.describe_tasks_kwargs)
