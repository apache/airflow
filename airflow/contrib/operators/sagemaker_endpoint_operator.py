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


class SageMakerEndpointOperator(SageMakerBaseOperator):

    """
    Create a SageMaker endpoint.

    This operator returns The ARN of the endpoint created in Amazon SageMaker

    :param config:
        The configuration necessary to create an endpoint.

        If you need to create a SageMaker endpoint based on an existed
        SageMaker model and an existed SageMaker endpoint config::

            config = endpoint_configuration;

        If you need to create all of SageMaker model, SageMaker endpoint-config and SageMaker endpoint::

            config = {
                'Model': model_configuration,
                'EndpointConfig': endpoint_config_configuration,
                'Endpoint': endpoint_configuration
            }

        For details of the configuration parameter of model_configuration see
        :py:meth:`SageMaker.Client.create_model`

        For details of the configuration parameter of endpoint_config_configuration see
        :py:meth:`SageMaker.Client.create_endpoint_config`

        For details of the configuration parameter of endpoint_configuration see
        :py:meth:`SageMaker.Client.create_endpoint`

    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    :param wait_for_completion: Whether the operator should wait until the endpoint creation finishes.
    :type wait_for_completion: bool
    :param check_interval: If wait is set to True, this is the time interval, in seconds, that this operation
        waits before polling the status of the endpoint creation.
    :type check_interval: int
    :param max_ingestion_time: If wait is set to True, this operation fails if the endpoint creation doesn't
        finish within max_ingestion_time seconds. If you set this parameter to None it never times out.
    :type max_ingestion_time: int
    :param operation: Whether to create an endpoint or update an endpoint. Must be either 'create or 'update'.
    :type operation: str
    """

    @apply_defaults
    def __init__(self,
                 config,
                 wait_for_completion=True,
                 check_interval=30,
                 max_ingestion_time=None,
                 operation='create',
                 *args, **kwargs):
        super().__init__(config=config,
                         *args, **kwargs)

        self.config = config
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.operation = operation.lower()
        if self.operation not in ['create', 'update']:
            raise ValueError('Invalid value! Argument operation has to be one of "create" and "update"')
        self.create_integer_fields()

    def create_integer_fields(self):
        if 'EndpointConfig' in self.config:
            self.integer_fields = [
                ['EndpointConfig', 'ProductionVariants', 'InitialInstanceCount']
            ]

    def expand_role(self):
        if 'Model' not in self.config:
            return
        hook = AwsHook(self.aws_conn_id)
        config = self.config['Model']
        if 'ExecutionRoleArn' in config:
            config['ExecutionRoleArn'] = hook.expand_role(config['ExecutionRoleArn'])

    def execute(self, context):
        self.preprocess_config()

        model_info = self.config.get('Model')
        endpoint_config_info = self.config.get('EndpointConfig')
        endpoint_info = self.config.get('Endpoint', self.config)

        if model_info:
            self.log.info('Creating SageMaker model %s.', model_info['ModelName'])
            self.hook.create_model(model_info)

        if endpoint_config_info:
            self.log.info('Creating endpoint config %s.', endpoint_config_info['EndpointConfigName'])
            self.hook.create_endpoint_config(endpoint_config_info)

        if self.operation == 'create':
            sagemaker_operation = self.hook.create_endpoint
            log_str = 'Creating'
        elif self.operation == 'update':
            sagemaker_operation = self.hook.update_endpoint
            log_str = 'Updating'
        else:
            raise ValueError('Invalid value! Argument operation has to be one of "create" and "update"')

        self.log.info('%s SageMaker endpoint %s.', log_str, endpoint_info['EndpointName'])

        response = sagemaker_operation(
            endpoint_info,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(
                'Sagemaker endpoint creation failed: %s' % response)
        else:
            return {
                'EndpointConfig': self.hook.describe_endpoint_config(
                    endpoint_info['EndpointConfigName']
                ),
                'Endpoint': self.hook.describe_endpoint(
                    endpoint_info['EndpointName']
                )
            }
