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

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.sagemaker_base import SageMakerBaseOperator
from airflow.utils.decorators import apply_defaults


class SageMakerTrainingOperator(SageMakerBaseOperator):
    """
    Initiate a SageMaker training job.

    This operator returns The ARN of the training job created in Amazon SageMaker.

    :param config: The configuration necessary to start a training job (templated).

        For details of the configuration parameter see :py:meth:`SageMaker.Client.create_training_job`
    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    :param wait_for_completion: If wait is set to True, the time interval, in seconds,
        that the operation waits to check the status of the training job.
    :type wait_for_completion: bool
    :param print_log: if the operator should print the cloudwatch log during training
    :type print_log: bool
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the training job
    :type check_interval: int
    :param max_ingestion_time: If wait is set to True, the operation fails if the training job
        doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
        the operation does not timeout.
    :type max_ingestion_time: int
    """

    integer_fields = [
        ['ResourceConfig', 'InstanceCount'],
        ['ResourceConfig', 'VolumeSizeInGB'],
        ['StoppingCondition', 'MaxRuntimeInSeconds']
    ]

    @apply_defaults
    def __init__(self,
                 config,
                 wait_for_completion=True,
                 print_log=True,
                 check_interval=30,
                 max_ingestion_time=None,
                 *args, **kwargs):
        super().__init__(config=config,
                         *args, **kwargs)

        self.wait_for_completion = wait_for_completion
        self.print_log = print_log
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time

    def expand_role(self):
        if 'RoleArn' in self.config:
            hook = AwsHook(self.aws_conn_id)
            self.config['RoleArn'] = hook.expand_role(self.config['RoleArn'])

    def execute(self, context):
        self.preprocess_config()

        self.log.info('Creating SageMaker Training Job %s.', self.config['TrainingJobName'])

        response = self.hook.create_training_job(
            self.config,
            wait_for_completion=self.wait_for_completion,
            print_log=self.print_log,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException('Sagemaker Training Job creation failed: %s' % response)
        else:
            return {
                'Training': self.hook.describe_training_job(
                    self.config['TrainingJobName']
                )
            }
