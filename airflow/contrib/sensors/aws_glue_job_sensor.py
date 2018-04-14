# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_glue_job_hook import AwsGlueJobHook


class AwsGlueJobSensor(BaseSensorOperator):
    """
    Waits for an AWS Glue Job to reach any of the status below
    'FAILED', 'STOPPED', 'SUCCEEDED'

    :param job_name: The AWS Glue Job unique name
    :type str
    :param run_id: The AWS Glue current running job identifier
    :type str
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
        self.targeted_status = ['FAILED', 'STOPPED', 'SUCCEEDED']

    def poke(self, context):
        self.log.info("Poking for job run status : {self.targeted_status}\n"
                      "for Glue Job {self.job_name} and ID {self.run_id}"
                      .format(**locals()))
        hook = AwsGlueJobHook(aws_conn_id=self.aws_conn_id)
        job_state = hook.job_completion(job_name=self.job_name,
                                        run_id=self.run_id)
        return job_state.upper() in self.targeted_status
