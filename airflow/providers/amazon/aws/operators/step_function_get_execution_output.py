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

import json

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.utils.decorators import apply_defaults


class StepFunctionGetExecutionOutputOperator(BaseOperator):
    """
    An Operator that begins execution of an Step Function State Machine

    Additional arguments may be specified and are passed down to the underlying BaseOperator.

    .. seealso::
        :class:`~airflow.models.BaseOperator`

    :param execution_arn: ARN of the Step Function State Machine Execution
    :type execution_arn: str
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :type aws_conn_id: str
    """
    template_fields = ['execution_arn']
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(self, execution_arn: str, aws_conn_id='aws_default', region_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.execution_arn = execution_arn
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def execute(self, context):
        hook = StepFunctionHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

        execution_status = hook.describe_execution(self.execution_arn)
        execution_output = json.loads(execution_status['output']) if 'output' in execution_status else None

        self.log.info('Got State Machine Execution output for %s', self.execution_arn)

        return execution_output
