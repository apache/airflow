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
from airflow.contrib.operators.sagemaker_base_operator import SageMakerBaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class SageMakerTuningOperator(SageMakerBaseOperator):
    """
    Initiate a SageMaker hyper-parameter tuning job.

    This operator returns The ARN of the tuning job created in Amazon SageMaker.

    :param config: The configuration necessary to start a tuning job (templated)
    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    :param wait_for_completion: if the operator should block until tuning job finishes
    :type wait_for_completion: bool
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the tuning job
    :type check_interval: int
    :param max_ingestion_time: if wait is set to be true, the operator will fail
        if the tuning job hasn't finish within the max_ingestion_time in seconds
        (Caution: be careful to set this parameters because tuning can take very long)
    :type max_ingestion_time: int
    """

    integer_fields = [
        ['HyperParameterTuningJobConfig', 'ResourceLimits', 'MaxNumberOfTrainingJobs'],
        ['HyperParameterTuningJobConfig', 'ResourceLimits', 'MaxParallelTrainingJobs'],
        ['TrainingJobDefinition', 'ResourceConfig', 'InstanceCount'],
        ['TrainingJobDefinition', 'ResourceConfig', 'VolumeSizeInGB'],
        ['TrainingJobDefinition', 'StoppingCondition', 'MaxRuntimeInSeconds']
    ]

    @apply_defaults
    def __init__(self,
                 config,
                 wait_for_completion=True,
                 check_interval=30,
                 max_ingestion_time=None,
                 *args, **kwargs):
        super(SageMakerTuningOperator, self).__init__(config=config,
                                                      *args, **kwargs)
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time

    def expand_role(self):
        if 'TrainingJobDefinition' in self.config:
            config = self.config['TrainingJobDefinition']
            if 'RoleArn' in config:
                hook = AwsHook(self.aws_conn_id)
                config['RoleArn'] = hook.expand_role(config['RoleArn'])

    def execute(self, context):
        self.preprocess_config()

        self.log.info(
            'Creating SageMaker Hyper-Parameter Tuning Job %s', self.config['HyperParameterTuningJobName']
        )

        response = self.hook.create_tuning_job(
            self.config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(
                'Sagemaker Tuning Job creation failed: %s' % response)
        else:
            return {
                'Tuning': self.hook.describe_tuning_job(
                    self.config['HyperParameterTuningJobName']
                )
            }
