# -*- coding: utf-8 -*-
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
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class EmrAddStepsOperator(BaseOperator):
    """
    An operator that adds steps to an existing EMR job_flow.

    :param job_flow_id: id of the JobFlow to add steps to. (templated)
    :type job_flow_id: str
    :param job_flow_name: name of the JobFlow to add steps to (alternative to passing job_flow_id. will
        search for id of first running/waiting JobFlow with matching name). (templated)
    :type job_flow_name: str
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param steps: boto3 style steps to be added to the jobflow. (templated)
    :type steps: list
    """
    template_fields = ['job_flow_id', 'steps']
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
        self,
        job_flow_id=None,
        job_flow_name=None,
        aws_conn_id='s3_default',
        steps=None,
        *args, **kwargs):
        super().__init__(*args, **kwargs)
        steps = steps or []
        self.aws_conn_id = aws_conn_id
        self.emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        self.job_flow_id = job_flow_id
        if not self.job_flow_id:
            if job_flow_name:
                self.job_flow_id = self.emr_hook.get_cluster_id_by_name(job_flow_name)
            else:
                raise AirflowException('Either job_flow_id or job_flow_name must be specified.')
        self.job_flow_name = job_flow_name
        self.steps = steps

    def execute(self, context):
        emr = self.emr_hook.get_conn()

        self.log.info('Adding steps to %s', self.job_flow_id)
        response = emr.add_job_flow_steps(JobFlowId=self.job_flow_id, Steps=self.steps)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('Adding steps failed: %s' % response)
        else:
            self.log.info('Steps %s added to JobFlow', response['StepIds'])
            return response['StepIds']
