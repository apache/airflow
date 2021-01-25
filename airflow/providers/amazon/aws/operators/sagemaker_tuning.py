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
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.sagemaker_base import SageMakerBaseOperator
from airflow.utils.decorators import apply_defaults


class SageMakerTuningOperator(SageMakerBaseOperator):
    """
    Initiate a SageMaker hyperparameter tuning job.

    This operator returns The ARN of the tuning job created in Amazon SageMaker.

    :param config: The configuration necessary to start a tuning job (templated).

        For details of the configuration parameter see
        :py:meth:`SageMaker.Client.create_hyper_parameter_tuning_job`
    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    :param wait_for_completion: Set to True to wait until the tuning job finishes.
    :type wait_for_completion: bool
    :param check_interval: If wait is set to True, the time interval, in seconds,
        that this operation waits to check the status of the tuning job.
    :type check_interval: int
    :param max_ingestion_time: If wait is set to True, the operation fails
        if the tuning job doesn't finish within max_ingestion_time seconds. If you
        set this parameter to None, the operation does not timeout.
    :type max_ingestion_time: int
    """

    integer_fields = [
        ['HyperParameterTuningJobConfig', 'ResourceLimits', 'MaxNumberOfTrainingJobs'],
        ['HyperParameterTuningJobConfig', 'ResourceLimits', 'MaxParallelTrainingJobs'],
        ['TrainingJobDefinition', 'ResourceConfig', 'InstanceCount'],
        ['TrainingJobDefinition', 'ResourceConfig', 'VolumeSizeInGB'],
        ['TrainingJobDefinition', 'StoppingCondition', 'MaxRuntimeInSeconds'],
    ]

    @apply_defaults
    def __init__(
        self,
        *,
        config: dict,
        wait_for_completion: bool = True,
        check_interval: int = 30,
        max_ingestion_time: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(config=config, **kwargs)
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time

    def expand_role(self) -> None:
        if 'TrainingJobDefinition' in self.config:
            config = self.config['TrainingJobDefinition']
            if 'RoleArn' in config:
                hook = AwsBaseHook(self.aws_conn_id, client_type='iam')
                config['RoleArn'] = hook.expand_role(config['RoleArn'])

    def execute(self, context) -> dict:
        self.preprocess_config()

        self.log.info(
            'Creating SageMaker Hyper-Parameter Tuning Job %s', self.config['HyperParameterTuningJobName']
        )

        response = self.hook.create_tuning_job(
            self.config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(f'Sagemaker Tuning Job creation failed: {response}')
        else:
            return {'Tuning': self.hook.describe_tuning_job(self.config['HyperParameterTuningJobName'])}
