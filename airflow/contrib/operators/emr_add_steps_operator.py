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

import logging

from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.emr_hook import EmrHook


class EmrAddStepsOperator(BaseOperator):
    """
    An operator that adds steps to an existing EMR job_flow.

    :param job_flow_id: id of the JobFlow to add steps to
    :type job_flow_name: str
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param steps: boto3 style steps to be added to the jobflow
    :type steps: list
    """
    template_fields = ['job_flow_id']
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            job_flow_id,
            aws_conn_id='s3_default',
            steps=[],
            *args, **kwargs):
        super(EmrAddStepsOperator, self).__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id
        self.aws_conn_id = aws_conn_id
        self.steps = steps

    def execute(self, context):
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        logging.info('Adding steps to %s', self.job_flow_id)
        response = emr.add_job_flow_steps(JobFlowId=self.job_flow_id, Steps=self.steps)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('Adding steps failed: %s' % response)
        else:
            logging.info('Steps %s added to JobFlow', response['StepIds'])
            return response['StepIds']
