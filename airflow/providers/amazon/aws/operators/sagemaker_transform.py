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
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.sagemaker_base import SageMakerBaseOperator
from airflow.utils.decorators import apply_defaults


class SageMakerTransformOperator(SageMakerBaseOperator):
    """
    Initiate a SageMaker transform job.

    This operator returns The ARN of the model created in Amazon SageMaker.

    :param config: The configuration necessary to start a transform job (templated).

        If you need to create a SageMaker transform job based on an existed SageMaker model::

            config = transform_config

        If you need to create both SageMaker model and SageMaker Transform job::

            config = {
                'Model': model_config,
                'Transform': transform_config
            }

        For details of the configuration parameter of transform_config see
        :py:meth:`SageMaker.Client.create_transform_job`

        For details of the configuration parameter of model_config, See:
        :py:meth:`SageMaker.Client.create_model`

    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    :param wait_for_completion: Set to True to wait until the transform job finishes.
    :type wait_for_completion: bool
    :param check_interval: If wait is set to True, the time interval, in seconds,
        that this operation waits to check the status of the transform job.
    :type check_interval: int
    :param max_ingestion_time: If wait is set to True, the operation fails
        if the transform job doesn't finish within max_ingestion_time seconds. If you
        set this parameter to None, the operation does not timeout.
    :type max_ingestion_time: int
    """

    @apply_defaults
    def __init__(self,
                 config,
                 wait_for_completion=True,
                 check_interval=30,
                 max_ingestion_time=None,
                 **kwargs):
        super().__init__(config=config,
                         **kwargs)
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.create_integer_fields()

    def create_integer_fields(self):
        """Set fields which should be casted to integers."""
        self.integer_fields = [
            ['Transform', 'TransformResources', 'InstanceCount'],
            ['Transform', 'MaxConcurrentTransforms'],
            ['Transform', 'MaxPayloadInMB']
        ]
        if 'Transform' not in self.config:
            for field in self.integer_fields:
                field.pop(0)

    def expand_role(self):
        if 'Model' not in self.config:
            return
        config = self.config['Model']
        if 'ExecutionRoleArn' in config:
            hook = AwsBaseHook(self.aws_conn_id, client_type='iam')
            config['ExecutionRoleArn'] = hook.expand_role(config['ExecutionRoleArn'])

    def execute(self, context):
        self.preprocess_config()

        model_config = self.config.get('Model')
        transform_config = self.config.get('Transform', self.config)

        if model_config:
            self.log.info('Creating SageMaker Model %s for transform job', model_config['ModelName'])
            self.hook.create_model(model_config)

        self.log.info('Creating SageMaker transform Job %s.', transform_config['TransformJobName'])
        response = self.hook.create_transform_job(
            transform_config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException('Sagemaker transform Job creation failed: %s' % response)
        else:
            return {
                'Model': self.hook.describe_model(
                    transform_config['ModelName']
                ),
                'Transform': self.hook.describe_transform_job(
                    transform_config['TransformJobName']
                )
            }
