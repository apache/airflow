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
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.emr_hook import EmrHook


class EmrTerminateJobFlowOperator(BaseOperator):
    """
    Operator to terminate EMR JobFlows.

    :param job_flow_id: id of the JobFlow to terminate. (templated)
    :type job_flow_id: str
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    """
    template_fields = ['job_flow_id']
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            job_flow_id,
            aws_conn_id='s3_default',
            *args, **kwargs):
        super(EmrTerminateJobFlowOperator, self).__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        self.log.info('Terminating JobFlow %s', self.job_flow_id)
        response = emr.terminate_job_flows(JobFlowIds=[self.job_flow_id])

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('JobFlow termination failed: %s' % response)
        else:
            self.log.info('JobFlow with id %s terminated', self.job_flow_id)
