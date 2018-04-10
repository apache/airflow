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


from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.contrib.hooks.aws_hook import AwsHook
import json
import os.path

class AwsGlueJobHook(AwsHook):
    """
    Interact with AWS Glue - create job, trigger, crawler

    :param job_name: unique job name per AWS account
    :type str
    :param desc: job description
    :type str
    :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
    :type int
    :param script_location: path to etl script either on s3 or local
    :type str
    :param conns: A list of connections used by the job
    :type list
    :param retry_limit: The maximum number of times to retry this job if it fails
    :type int
    :param num_of_dpus: The number of AWS Glue data processing units (DPUs) to allocate to this Job
    :type int
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    :param default_s3_bucket: This is your S3 bucket where logs and local AWS Glue etl script will be uploaded to
    :type str
    """

    def __init__(self,
                 job_name=None,
                 desc=None,
                 concurrent_run_limit=None,
                 script_location=None,
                 conns=None,
                 retry_limit=None,
                 num_of_dpus=None,
                 region_name=None,
                 default_s3_bucket=None, *args, **kwargs):
        self.job_name = job_name
        self.desc = desc
        self.concurrent_run_limit = concurrent_run_limit
        self.script_location = script_location
        self.conns = conns
        self.retry_limit = retry_limit
        self.num_of_dpus = num_of_dpus
        self.region_name = region_name
        self.default_s3_bucket = default_s3_bucket
        self.assumed_policy = {
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        }
        self.S3_PROTOCOL = "s3://"
        self.S3_ARTIFACTS_PREFIX = 'artifacts/glue-scripts/'
        self.S3_GLUE_LOGS = 'logs/glue-logs/'
        super(AwsGlueJobHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        self.conn = self.get_client_type('glue', self.region_name)
        return self.conn

    def _create_job_execution_role(self):
        """
        :return: iam role for job execution
        """
        iam_client = self.get_client_type('iam', self.region_name)
        role_name = "{}-ExecutionRole".format(self.job_name.capitalize())

        ## step 1: create job execution
        self.glue_execution_role = iam_client.create_role(
            Path="/",
            RoleName=role_name,
            Description="AWS IAM Execution Role for {}".format(self.job_name.capitalize()),
            AssumeRolePolicyDocument=json.dumps(self.assumed_policy)
        )
        ## step 2: attached aws glue service policy to role role_name
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        )

        return self.glue_execution_role

    def initialize_job(self, script_arguments=None):
        """
        :return:
        """
        glue_client = self.get_conn()

        try:
            job_name = self._get_or_create_glue_job()
            job_run = glue_client.start_job_run(
                JobName=job_name,
                Arguments=script_arguments
            )
            return self._job_completion(job_name, job_run['JobRunId'])
        except Exception as general_error:
            raise AirflowException(
                'Failed to run aws glue job, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def _job_completion(self, job_name=None, run_id=None):
        """
        :param job_name:
        :param run_id:
        :return:
        """
        glue_client = self.get_conn()
        job_status = glue_client.get_job_run(
            JobName=job_name,
            RunId=run_id,
            PredecessorsIncluded=False
        )
        #
        while job_status['JobRun']['JobRunState'] == 'STARTING' or 'RUNNING' or 'STOPPING':
            self.job_completion(job_name, run_id)

        return job_status

    def _get_or_create_glue_job(self):
        glue_client = self.get_conn()
        try:
            job_details = glue_client.get_job(JobName=self.job_name)
            return job_details['Job']['Name']
        except AirflowConfigException:
            ## create job
            execution_role = self._create_job_execution_role()
            script_location = self._check_script_location(self.script_location)
            create_job_response = glue_client.create_job(
                Name=self.job_name,
                Description=self.desc,
                LogUri="s3://{bucket_name}/{logs_path}{job_name}".format(bucket_name=self.default_s3_bucket, logs_path=self.S3_GLUE_LOGS, job_name=self.job_name),
                Role=execution_role['Role']['RoleName'],
                ExecutionProperty={"MaxConcurrentRuns": self.concurrent_run_limit},
                Command={"Name": "glueetl", "ScriptLocation": script_location},
                Connections={"Connections": self.conns},
                MaxRetries=self.retry_limit,
                AllocatedCapacity=self.num_of_dpus
            )
            return create_job_response['Name']
        except Exception as general_error:
            raise AirflowException(
                'Failed to create aws glue job, error: {error}. Check AWS Console for more details'.format(
                    error=str(general_error)
                )
            )

    def _check_script_location(self):
        """
        :return: S3 Script location path
        """
        if self.script_location[:5] == self.S3_PROTOCOL:
            return self.script_location
        elif os.path.isfile(self.script_location):
            s3 = self.get_resource_type('s3', self.region_name)
            script_name = os.path.basename(self.script_location)
            s3.meta.client.upload_file(self.script_location,
                                       self.default_s3_bucket, self.S3_ARTIFACTS_PREFIX + script_name)
            return "s3://{s3_bucket}/{prefix}{script_name}".format(s3_bucket=self.default_s3_bucket,
                                                                   prefix=self.S3_ARTIFACTS_PREFIX,
                                                                   script_name=script_name)
        else:
            return None
