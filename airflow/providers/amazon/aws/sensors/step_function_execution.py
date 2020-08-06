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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class StepFunctionExecutionSensor(BaseSensorOperator):
    """
    Asks for the state of the Step Function State Machine Execution until it
    reaches a failure state or success state.
    If it fails, failing the task.

    On successful completion of the Execution the Sensor will do an XCom Push
    of the State Machine's output to `output`

    :param execution_arn: execution_arn to check the state of
    :type execution_arn: str
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :type aws_conn_id: str
    """

    INTERMEDIATE_STATES = ('RUNNING',)
    FAILURE_STATES = ('FAILED', 'TIMED_OUT', 'ABORTED',)
    SUCCESS_STATES = ('SUCCEEDED',)

    template_fields = ['execution_arn']
    template_ext = ()
    ui_color = '#66c3ff'

    @apply_defaults
    def __init__(self, *, execution_arn: str, aws_conn_id='aws_default', region_name=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.execution_arn = execution_arn
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.hook = None

    def poke(self, context):
        execution_status = self.get_hook().describe_execution(self.execution_arn)
        state = execution_status['status']
        output = json.loads(execution_status['output']) if 'output' in execution_status else None

        if state in self.FAILURE_STATES:
            raise AirflowException(f'Step Function sensor failed. State Machine Output: {output}')

        if state in self.INTERMEDIATE_STATES:
            return False

        self.log.info('Doing xcom_push of output')
        self.xcom_push(context, 'output', output)
        return True

    def get_hook(self):
        """Create and return a StepFunctionHook"""
        if not self.hook:
            self.hook = StepFunctionHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        return self.hook
