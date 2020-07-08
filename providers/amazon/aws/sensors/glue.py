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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.glue import AwsGlueJobHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class AwsGlueJobSensor(BaseSensorOperator):
    """
    Waits for an AWS Glue Job to reach any of the status below
    'FAILED', 'STOPPED', 'SUCCEEDED'

    :param job_name: The AWS Glue Job unique name
    :type job_name: str
    :param run_id: The AWS Glue current running job identifier
    :type run_id: str
    """
    template_fields = ('job_name', 'run_id')

    @apply_defaults
    def __init__(self,
                 job_name,
                 run_id,
                 aws_conn_id='aws_default',
                 *args,
                 **kwargs):
        super(AwsGlueJobSensor, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.run_id = run_id
        self.aws_conn_id = aws_conn_id
        self.success_states = ['SUCCEEDED']
        self.errored_states = ['FAILED', 'STOPPED', 'TIMEOUT']

    def poke(self, context):
        self.log.info(
            "Poking for job run status :"
            "for Glue Job %s and ID %s", self.job_name, self.run_id)
        hook = AwsGlueJobHook(aws_conn_id=self.aws_conn_id)
        job_state = hook.job_completion(job_name=self.job_name,
                                        run_id=self.run_id)
        if job_state in self.success_states:
            self.log.info("Exiting Job %s Run State: %s", self.run_id, job_state)
            return True
        elif job_state in self.errored_states:
            job_error_message = "Exiting Job " + self.run_id + " Run State: " + job_state
            raise AirflowException(job_error_message)
        else:
            return False
