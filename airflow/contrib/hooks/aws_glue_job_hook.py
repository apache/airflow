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


from airflow.exceptions import AirflowException
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
    :param retry_limit: Maximum number of times to retry this job if it fails
    :type int
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job
    :type int
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
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
                 s3_bucket=None, *args, **kwargs):
        self.job_name = job_name
        self.desc = desc
        self.concurrent_run_limit = concurrent_run_limit
        self.script_location = script_location
        self.conns = conns or ["s3"]
        self.retry_limit = retry_limit or 0
        self.num_of_dpus = num_of_dpus or 10
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.assumed_policy = {
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        }
        self.role_name = "{}_ExecutionRole".format(self.job_name.capitalize())
        self.S3_PROTOCOL = "s3://"
        self.S3_ARTIFACTS_PREFIX = 'artifacts/glue-scripts/'
        self.S3_GLUE_LOGS = 'logs/glue-logs/'
        self.GLUE_SERVICE_ROLE = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        super(AwsGlueJobHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        conn = self.get_client_type('glue', self.region_name)
        return conn

    def _create_job_execution_role(self):
        """
        :return: iam role for job execution
        """
        iam_client = self.get_client_type('iam', self.region_name)

        try:
            glue_execution_role = iam_client.get_role(RoleName=self.role_name)
        except Exception:
            glue_execution_role = None

        if glue_execution_role is None:
            self.log.info("Creating IAM Execution role for Glue Job")
            # attach inline policy to access data bucket
            self.log.info("Inline Policy attached: ")
            glue_execution_role = iam_client.create_role(
                Path="/",
                RoleName=self.role_name,
                Description="AWS IAM Execution Role for {}".format(self.role_name),
                AssumeRolePolicyDocument=json.dumps(self.assumed_policy)
            )
        policy = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Sid': 'DataObjectAccess'.format(job_name=self.job_name),
                    'Effect': 'Allow',
                    'Action': ['s3:GetObject', 's3:PutObject'],
                    'Resource': [
                        'arn:aws:s3:::{s3_bucket}'.format(s3_bucket=self.s3_bucket),
                        'arn:aws:s3:::{s3_bucket}/*'.format(s3_bucket=self.s3_bucket)
                    ]
                },
                {
                    'Sid': 'DataBucketAccess'.format(job_name=self.job_name),
                    'Effect': 'Allow',
                    'Action': [
                        's3:ListBucket',
                        's3:ListAllMyBuckets',
                        's3:GetBucketLocation'
                    ],
                    'Resource': [
                        'arn:aws:s3:::{s3_bucket}'.format(s3_bucket=self.s3_bucket)
                    ]
                }
            ]
        }
        self.log.info("Attaching AWS Glue Service Role")
        iam_client.attach_role_policy(
            RoleName=self.role_name,
            PolicyArn=self.GLUE_SERVICE_ROLE
        )
        self.log.info("Attaching Inline Policy for data and script bucket access")
        iam_client.put_role_policy(
            RoleName=self.role_name,
            PolicyName='DataBucketAccessPolicy',
            PolicyDocument=json.dumps(policy)
        )

        return glue_execution_role

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
            PredecessorsIncluded=True
        )
        job_run_state = job_status['JobRun']['JobRunState']
        failed = job_run_state == 'FAILED'
        stopped = job_run_state == 'STOPPED'
        completed = job_run_state == 'SUCCEEDED'

        if failed or stopped or completed:
            self.log.info("Exiting Job {} Run State: {}".format(run_id, job_run_state))
            return job_run_state
        else:
            self.log.info("Polling for AWS Glue job {} status".format(job_name))
            self.log.info("Current AWS Glue Job State: {}".format(job_run_state))
            return self._job_completion(job_name, run_id)

    def _get_or_create_glue_job(self):
        glue_client = self.get_conn()
        try:
            self.log.info("Now creating and running AWS Glue Job")
            s3_log_path = "s3://{bucket_name}/{logs_path}{job_name}"\
                .format(bucket_name=self.s3_bucket,
                        logs_path=self.S3_GLUE_LOGS,
                        job_name=self.job_name)

            execution_role = self._create_job_execution_role()
            script_location = self._check_script_location()
            create_job_response = glue_client.create_job(
                Name=self.job_name,
                Description=self.desc,
                LogUri=s3_log_path,
                Role=execution_role['Role']['RoleName'],
                ExecutionProperty={"MaxConcurrentRuns": self.concurrent_run_limit},
                Command={"Name": "glueetl", "ScriptLocation": script_location},
                MaxRetries=self.retry_limit,
                AllocatedCapacity=self.num_of_dpus
            )
            return create_job_response['Name']
        except Exception as general_error:
            raise AirflowException(
                'Failed to create aws glue job, error: {error}'.format(
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
                                       self.s3_bucket,
                                       self.S3_ARTIFACTS_PREFIX + script_name)

            s3_script_path = "s3://{s3_bucket}/{prefix}{script_name}" \
                .format(s3_bucket=self.s3_bucket,
                        prefix=self.S3_ARTIFACTS_PREFIX,
                        script_name=script_name)
            return s3_script_path
        else:
            return None

    def job_tear_down(self):
        """
        This method tears down a running glue job and
        deletes all attached IAM policies
        :return:
        """
        glue_client = self.get_conn()
        # delete iam role
        iam_client = self.get_client_type('iam', self.region_name)
        iam_client.detach_role_policy(
            RoleName=self.role_name,
            PolicyArn=self.GLUE_SERVICE_ROLE
        )
        iam_client.delete_role(
            RoleName=self.role_name
        )
        # delete aws glue job
        glue_client.delete_job(
            JobName=self.job_name
        )
