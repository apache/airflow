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


class SageMakerModelOperator(SageMakerBaseOperator):

    """
    Create a SageMaker model.

    This operator returns The ARN of the model created in Amazon SageMaker

    :param config: The configuration necessary to create a model.

        For details of the configuration parameter see :py:meth:`SageMaker.Client.create_model`
    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    """

    @apply_defaults
    def __init__(self,
                 config,
                 *args, **kwargs):
        super().__init__(config=config,
                         *args, **kwargs)

        self.config = config

    def expand_role(self):
        if 'ExecutionRoleArn' in self.config:
            hook = AwsHook(self.aws_conn_id)
            self.config['ExecutionRoleArn'] = hook.expand_role(self.config['ExecutionRoleArn'])

    def execute(self, context):
        self.preprocess_config()

        self.log.info('Creating SageMaker Model %s.', self.config['ModelName'])
        response = self.hook.create_model(self.config)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException('Sagemaker model creation failed: %s' % response)
        else:
            return {
                'Model': self.hook.describe_model(
                    self.config['ModelName']
                )
            }
