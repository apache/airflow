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
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.utils.decorators import apply_defaults


class EmrCreateJobFlowOperator(BaseOperator):
    """
    Creates an EMR JobFlow, reading the config from the EMR connection.
    A dictionary of JobFlow overrides can be passed that override
    the config from the connection.

    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param emr_conn_id: emr connection to use
    :type emr_conn_id: str
    :param job_flow_overrides: boto3 style arguments to override
       emr_connection extra. (templated)
    :type job_flow_overrides: dict
    """
    template_fields = ['job_flow_overrides']
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='aws_default',
            emr_conn_id='emr_default',
            job_flow_overrides=None,
            region_name=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        if job_flow_overrides is None:
            job_flow_overrides = {}
        self.job_flow_overrides = job_flow_overrides
        self.region_name = region_name

    def execute(self, context):
        emr = EmrHook(aws_conn_id=self.aws_conn_id,
                      emr_conn_id=self.emr_conn_id,
                      region_name=self.region_name)

        self.log.info(
            'Creating JobFlow using aws-conn-id: %s, emr-conn-id: %s',
            self.aws_conn_id, self.emr_conn_id
        )
        response = emr.create_job_flow(self.job_flow_overrides)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('JobFlow creation failed: %s' % response)
        else:
            self.log.info('JobFlow with id %s created', response['JobFlowId'])
            return response['JobFlowId']
