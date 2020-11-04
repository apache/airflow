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

from typing import Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.utils.decorators import apply_defaults


class StepFunctionStartExecutionOperator(BaseOperator):
    """
    An Operator that begins execution of an Step Function State Machine

    Additional arguments may be specified and are passed down to the underlying BaseOperator.

    .. seealso::
        :class:`~airflow.models.BaseOperator`

    :param state_machine_arn: ARN of the Step Function State Machine
    :type state_machine_arn: str
    :param name: The name of the execution.
    :type name: Optional[str]
    :param state_machine_input: JSON data input to pass to the State Machine
    :type state_machine_input: Union[Dict[str, any], str, None]
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param do_xcom_push: if True, execution_arn is pushed to XCom with key execution_arn.
    :type do_xcom_push: bool
    """

    template_fields = ['state_machine_arn', 'name', 'input']
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
        self,
        *,
        state_machine_arn: str,
        name: Optional[str] = None,
        state_machine_input: Union[dict, str, None] = None,
        aws_conn_id: str = 'aws_default',
        region_name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.state_machine_arn = state_machine_arn
        self.name = name
        self.input = state_machine_input
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def execute(self, context):
        hook = StepFunctionHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

        execution_arn = hook.start_execution(self.state_machine_arn, self.name, self.input)

        if execution_arn is None:
            raise AirflowException(f'Failed to start State Machine execution for: {self.state_machine_arn}')

        self.log.info('Started State Machine execution for %s: %s', self.state_machine_arn, execution_arn)

        return execution_arn
