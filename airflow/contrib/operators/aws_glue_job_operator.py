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
from __future__ import unicode_literals

from airflow.contrib.hooks.aws_glue_job_hook import AwsGlueJobHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AWSGlueJobOperator(BaseOperator):
    """
    Creates an AWS Glue Job. AWS Glue is a serverless Spark
    ETL service for running Spark Jobs on the AWS cloud.
    Language support: Python and Scala

    :param job_name: unique job name per AWS Account
    :type str
    :param job_desc: job description details
    :type str
    :param script_args: etl script arguments and AWS Glue arguments
    :type dict
    :param aws_conn_id: aws connection id
    :type aws_conn_id: str
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 job_name='aws_glue_default_job',
                 job_desc='AWS Glue Job with Airflow',
                 script_args={},
                 aws_conn_id='aws_default',
                 *args, **kwargs
                 ):
        super(AWSGlueJobOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.job_desc = job_desc
        self.script_args = script_args
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        """
        Executes AWS Glue Job from Airflow
        :return:
        """
        glue_job = AwsGlueJobHook(
            job_name=self.job_name,
            desc=self.job_desc,
            aws_conn_id=self.aws_conn_id
        )

        self.log.info("Initializing AWS Glue Job: {}".format(self.job_name))
        glue_job_run = glue_job.initialize_job(self.script_args)
        self.log.info('AWS Glue Job: {job_name} status: {job_status}. Run Id: {run_id}'
                      .format(run_id=glue_job_run['JobRunId'],
                              job_name=self.job_name,
                              job_status=glue_job_run['JobRunState'])
                      )

        self.log.info('Done.')
