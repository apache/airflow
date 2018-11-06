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


from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook
import time
from botocore.exceptions import ClientError


class AwsGlueJobHook(AwsHook):
    """
    Interact with AWS Glue - create job

    :param job_name: unique job name per AWS account
    :type str
    :param desc: job description
    :type str
    :param aws_conn_id: aws connection id
    :type aws_conn_id: str
    """

    def __init__(self,
                 job_name=None,
                 desc=None,
                 aws_conn_id='aws_default',
                 *args, **kwargs):
        self.job_name = job_name
        self.desc = desc
        self.aws_conn_id = aws_conn_id
        super(AwsGlueJobHook, self).__init__(self.aws_conn_id, *args, **kwargs)

    def get_conn(self):
        return self.get_client_type('glue')

    def list_jobs(self):
        conn = self.get_conn()
        return conn.get_jobs()

    def initialize_job(self, script_arguments=None):
        """
        Initializes connection with AWS Glue
        to run job
        :return:
        """
        glue_client = self.get_conn()

        try:
            job_response = self.get_glue_job()
            job_name = job_response['Name']
            job_run = glue_client.start_job_run(
                JobName=job_name,
                Arguments=script_arguments
            )
            return self.job_completion(job_name, job_run['JobRunId'])
        except ClientError as general_error:
            raise AirflowException(
                'Failed to run aws glue job, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def job_completion(self, job_name=None, run_id=None):
        """
        :param job_name:
        :param run_id:
        :return:
        """
        glue_client = self.get_conn()
        job_status = glue_client.get_job_run(
            JobName=job_name,
            RunId=run_id,
            PredecessorsIncluded=True
        )
        job_run_state = job_status['JobRun']['JobRunState']

        while True:
            if job_run_state in {'FAILED', 'STOPPED', 'SUCCEEDED'}:
                self.log.info("Exiting Job {} Run State: {}"
                              .format(run_id, job_run_state))
                return {'JobRunState': job_run_state, 'JobRunId': run_id}
            else:
                self.log.info("Polling for AWS Glue Job {} current run state"
                              .format(job_name))
                time.sleep(6)

    def get_glue_job(self):
        """
        Function should return glue job details
        :return:
        """
        try:
            self.log.info(
                "Retrieving AWS Glue Job: {job_name}"
                    .format(job_name=self.job_name)
            )
            glue_client = self.get_conn()
            get_job_response = glue_client.get_job(
                JobName=self.job_name
            )['Job']
            self.logger.info("Found AWS Glue Job: {job_name}".format(job_name=self.job_name))
            self.logger.info("Job Creation Time: {}".format(get_job_response['CreatedOn']))
            self.logger.info("Last Job Modification Time: {}".format(get_job_response['LastModifiedOn']))
            return get_job_response
        except ClientError as general_error:
            raise AirflowException(
                'Failed to create aws glue job, error: {error}'.format(
                    error=str(general_error)
                )
            )
