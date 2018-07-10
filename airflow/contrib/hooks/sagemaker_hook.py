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
import copy

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook


class SageMakerHook(AwsHook):
    """
    Interact with Amazon SageMaker.
    sagemaker_conn_is is required for using
    the config stored in db for training/tuning
    """

    def __init__(self,
                 sagemaker_conn_id=None,
                 job_name=None,
                 use_db_config=False,
                 *args, **kwargs):
        self.sagemaker_conn_id = sagemaker_conn_id
        self.use_db_config = use_db_config
        self.job_name = job_name
        super(SageMakerHook, self).__init__(*args, **kwargs)
        self.conn = self.get_conn()

    def check_for_url(self, s3url):
        """
        check if the s3url exists
        :param s3url: S3 url
        :type s3url:str
        :return: bool
        """
        bucket, key = S3Hook.parse_s3_url(s3url)
        s3hook = S3Hook(aws_conn_id=self.aws_conn_id)
        if not s3hook.check_for_bucket(bucket_name=bucket):
            raise AirflowException(
                "The input S3 Bucket {} does not exist ".format(bucket))
        if not s3hook.check_for_key(key=key, bucket_name=bucket):
            raise AirflowException("The input S3 Key {} does not exist in the Bucket"
                                   .format(s3url, bucket))
        return True

    def check_valid_training_input(self, training_config):
        """
        Run checks before a training starts
        :param config: training_config
        :type config: dict
        :return: None
        """
        for channel in training_config['InputDataConfig']:
            self.check_for_url(channel['DataSource']
                               ['S3DataSource']['S3Uri'])

    def get_conn(self):
        return self.get_client_type('sagemaker')

    def list_training_job(self, name_contains=None, status_equals=None):
        """
        List the training jobs associated with the given input
        :param name_contains: A string in the training job name
        :type name_contains: str
        :param status_equals: 'InProgress'|'Completed'
        |'Failed'|'Stopping'|'Stopped'
        :return:dict
        """
        return self.conn.list_training_jobs(
            NameContains=name_contains, StatusEquals=status_equals)

    def list_tuning_job(self, name_contains=None, status_equals=None):
        """
        List the tuning jobs associated with the given input
        :param name_contains: A string in the training job name
        :type name_contains: str
        :param status_equals: 'InProgress'|'Completed'
        |'Failed'|'Stopping'|'Stopped'
        :return:dict
        """
        return self.conn.list_hyper_parameter_tuning_job(
            NameContains=name_contains, StatusEquals=status_equals)

    def create_training_job(self, training_job_config):
        """
        Create a training job
        :param training_job_config: the config for training
        :type training_job_config: dict
        :return: A dict that contains ARN of the training job.
        """
        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException("SageMaker connection id must be present to read \
                                        SageMaker training jobs configuration.")
            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = copy.deepcopy(sagemaker_conn.extra_dejson)
            training_job_config.update(config)

        self.check_valid_training_input(training_job_config)

        return self.conn.create_training_job(
            **training_job_config)

    def create_tuning_job(self, tuning_job_config):
        """
        Create a tuning job
        :param tuning_job_config: the config for tuning
        :type tuning_job_config: dict
        :return: A dict that contains ARN of the tuning job.
        """
        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException(
                    "sagemaker connection id must be present to \
                    read sagemaker tunning job configuration.")

            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = sagemaker_conn.extra_dejson.copy()
            tuning_job_config.update(config)

        return self.conn.create_hyper_parameter_tuning_job(
            **tuning_job_config)

    def describe_training_job(self):
        """
        Return the training job info associated with the current job_name
        :return: A dict contains all the training job info
        """
        return self.conn\
                   .describe_training_job(TrainingJobName=self.job_name)

    def describe_tuning_job(self):
        """
        Return the tuning job info associated with the current job_name
        :return: A dict contains all the tuning job info
        """
        return self.conn\
                   .describe_hyper_parameter_tuning_job(TrainingJobName=self.job_name)
