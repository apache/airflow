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
from __future__ import unicode_literals

from airflow.contrib.hooks.aws_glue_job_hook import AwsGlueJobHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GlueJobOperator(BaseOperator):
    """
    Creates an AWS Glue Job. AWS Glue is a serverless Spark
    ETL service for running Spark Jobs on the AWS cloud.
    Language support: Python and Scala
    :param job_name: unique job name per AWS Account
    :type str
    :param script_location: location of ETL script. Must be a local or S3 path
    :type str
    :param job_desc: job description details
    :type str
    :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
    :type int
    :param script_args: etl script arguments and AWS Glue arguments
    :type dict
    :param connections: AWS Glue connections to be used by the job.
    :type list
    :param retry_limit: The maximum number of times to retry this job if it fails
    :type int
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job.
    :type int
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
    :type str
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 job_name='aws_glue_default_job',
                 job_desc='This is my first AWS Glue Job with Airflow',
                 script_location=None,
                 concurrent_run_limit=None,
                 script_args={},
                 connections=[],
                 retry_limit=None,
                 num_of_dpus=6,
                 region_name=None,
                 s3_bucket=None,
                 *args, **kwargs
                 ):
        super(GlueJobOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.job_desc = job_desc
        self.script_location = script_location
        self.concurrent_run_limit = concurrent_run_limit
        self.script_args = script_args
        self.connections = connections
        self.retry_limit = retry_limit
        self.num_of_dpus = num_of_dpus
        self.region_name = region_name
        self.s3_bucket = s3_bucket

    def execute(self, context):
        """
        Executes AWS Glue Job from Airflow
        :return:
        """
        glue_job = AwsGlueJobHook(job_name=self.job_name,
                                  desc=self.job_desc,
                                  concurrent_run_limit=self.concurrent_run_limit,
                                  script_location=self.script_location,
                                  conns=self.connections,
                                  retry_limit=self.retry_limit,
                                  num_of_dpus=self.num_of_dpus,
                                  region_name=self.region_name,
                                  default_s3_bucket=self.s3_bucket)

        self.log.info("Initializing AWS Glue Job")
        glue_job_status = glue_job.initialize_job(self.script_args)['JobRun']

        self.log.info('AWS Glue Job: {job_name} status: {job_status}. Run Id: {run_id}'
                      .format(run_id=glue_job_status['Id'],
                              job_name=glue_job_status['JobName'],
                              job_status=glue_job_status['JobRunState'])
                      )

        self.log.info('Done.')
