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
from airflow.hooks.S3_hook import S3Hook
from urllib.parse import urlparse


class SageMakerHook(AwsHook):
    """
    Interact with Amazon SageMaker.
    """

    def __init__(self,
                 sagemaker_conn_id='sagemaker_default',
                 job_name=None,
                 use_db_config=False,
                 *args, **kwargs):
        self.sagemaker_conn_id = sagemaker_conn_id
        self.use_db_config = use_db_config
        self.job_name = job_name
        super(SageMakerHook, self).__init__(*args, **kwargs)

    @staticmethod
    def parse_s3_url(s3url):
        parsed_url = urlparse(s3url)
        if parsed_url.scheme != "s3":
            raise AirflowException("Expecting 's3' scheme, got: {} in {}"
                                   .format(parsed_url.scheme, s3url))
        return parsed_url.netloc, parsed_url.path.lstrip('/')

    def check_for_url(self, s3url):
        bucket, key = self.parse_s3_url(s3url)
        s3hook = S3Hook(aws_conn_id=self.aws_conn_id)
        if s3hook.check_for_key(key=key, bucket_name=bucket):
            raise AirflowException("The input S3 Url {} does not exist ".format(s3url))
        return True

    @staticmethod
    def is_key(key, dict, dict_name="configuration"):
        # type: (string, dict, string) -> bool
        if key not in dict:
            raise AirflowException("{} is required, but is missing in the {} provided"
                                   .format(key, dict_name))
        return True

    def check_valid_training_input(self, config):
        InstanceTypes = ['ml.m4.xlarge', 'ml.m4.2xlarge', 'ml.m4.4xlarge',
                         'ml.m4.10xlarge', 'ml.m4.16xlarge', 'ml.m5.large',
                         'ml.m5.xlarge', 'ml.m5.2xlarge', 'ml.m5.4xlarge',
                         'ml.m5.12xlarge', 'ml.m5.24xlarge', 'ml.c4.xlarge',
                         'ml.c4.2xlarge', 'ml.c4.4xlarge', 'ml.c4.8xlarge',
                         'ml.p2.xlarge', 'ml.p2.8xlarge', 'ml.p2.16xlarge',
                         'ml.p3.2xlarge', 'ml.p3.8xlarge', 'ml.p3.16xlarge',
                         'ml.c5.xlarge', 'ml.c5.2xlarge', 'ml.c5.4xlarge',
                         'ml.c5.9xlarge', 'ml.c5.18xlarge']
        self.is_key("TrainingJobName", config)
        self.is_key("AlgorithmSpecification", config)
        self.is_key("TrainingImage", config['AlgorithmSpecification'],
                    dict_name="AlgorithmSpecification")
        self.is_key("TrainingInputMode", config['AlgorithmSpecification'],
                    dict_name="AlgorithmSpecification")
        self.is_key("RoleArn", config)
        self.is_key("InputDataConfig", config)
        for channel in config["InputDataConfig"]:
            self.is_key("ChannelName", channel, dict_name="InputDataConfig")
            self.is_key("DataSource", channel, dict_name="InputDataConfig")
            self.is_key("S3DataSource", channel['DataSource'], dict_name="DataSource")
            self.is_key("S3DataType", channel['DataSource']['S3DataSource'],
                        dict_name="S3DataSource")
            self.is_key("S3Uri", channel['DataSource']['S3DataSource'],
                        dict_name="S3DataSource")
        self.is_key("OutputDataConfig", config)
        self.is_key("S3OutputPath", config['OutputDataConfig'],
                    dict_name="OutputDataConfig")
        self.is_key("ResourceConfig", config)
        self.is_key("InstanceType", config['ResourceConfig'], "ResourceConfig")
        self.is_key("InstanceCount", config['ResourceConfig'], "ResourceConfig")
        self.is_key("VolumeSizeInGB", config['ResourceConfig'], "ResourceConfig")
        if self.is_key("VpcConfig", config):
            self.is_key("SecurityGroupIds", config['VpcConfig'], "VpcConfig")
            self.is_key("Subnets", config['VpcConfig'], "VpcConfig")
        self.is_key("StoppingCondition", config)
        if self.is_key("Tags", config):
            self.is_key("Key", config['Tags'], "Tags")
            self.is_key("Value", config['Tags'], "Tags")
        if not config['AlgorithmSpecification']['TrainingInputMode'] \
                in ('Pipe', 'File'):
            raise AirflowException("TrainingInputMode should either be Pipe or File.")
        if not config['InputDataConfig']['DataSource']['S3DataSource']['S3DataType']\
                in ('ManifestFile', 'S3Prefix'):
            raise AirflowException("S3DataType should either be "
                                   "ManifestFile or S3Prefix.")
        if not config['ResourceConfig']['InstanceType'] in InstanceTypes:
            raise AirflowException("Invalid InstanceType.")
        self.checkt_for_url(config['InputDataConfig']
                                  ['DataSource']['S3DataSource']['S3Uri'])

    def get_conn(self):
        self.conn = self.get_client_type('sagemaker')
        return self.conn

    def list_training_job(self, job_name):
        sagemaker_conn = self.get_conn()
        return sagemaker_conn.list_training_jobs(NameContains=job_name)

    def list_tuning_job(self, job_name):
        sagemaker_conn = self.get_conn()
        return sagemaker_conn.list_hyper_parameter_tuning_job(NameContains=job_name)

    def create_training_job(self, training_job_config):

        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException("SageMaker connection id must be present to read \
                                        SageMaker training jobs configuration.")
            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = sagemaker_conn.extra_dejson.copy()
            tempconfig = config
            training_job_config.update(tempconfig)

        # run checks

        return self.get_conn().create_training_job(
            **training_job_config)

    def describe_training_job(self):
        return self.get_conn()\
                   .describe_training_job(TrainingJobName=self.job_name)

    def describe_tuning_job(self):
        return self.get_conn()\
                   .describe_hyper_parameter_tuning_job(TrainingJobName=self.job_name)

    def create_tuning_job(self, tuning_job_config):

        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException(
                    "sagemaker connection id must be present to \
                    read sagemaker tunning job configuration.")

            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = sagemaker_conn.extra_dejson.copy()
            tuning_job_config.update(config)

        return self.get_conn().create_hyper_parameter_tuning_job(
            **tuning_job_config)
