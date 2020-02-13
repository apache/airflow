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

"""
This module contains AWS CloudFormation Hook
"""
from botocore.exceptions import ClientError

from airflow.contrib.hooks.aws_hook import AwsHook


class AWSCloudFormationHook(AwsHook):
    """
    Interact with AWS CloudFormation.
    """

    def __init__(self, region_name=None, *args, **kwargs):
        self.region_name = region_name
        self.conn = None
        super().__init__(*args, **kwargs)

    def get_conn(self):
        if not self.conn:
            self.conn = self.get_client_type('cloudformation', self.region_name)
        return self.conn

    def get_stack_status(self, stack_name):
        """
        Get stack status from CloudFormation.
        """
        cloudformation = self.get_conn()

        self.log.info('Poking for stack %s', stack_name)

        try:
            stacks = cloudformation.describe_stacks(StackName=stack_name)['Stacks']
            return stacks[0]['StackStatus']
        except ClientError as e:
            if 'does not exist' in str(e):
                return None
            else:
                raise e

    def create_stack(self, stack_name, params):
        """
        Create stack in CloudFormation.

        :param stack_name: stack_name.
        :type stack_name: str
        :param params: parameters to be passed to CloudFormation.
        :type params: dict
        """

        if 'StackName' not in params:
            params['StackName'] = stack_name
        self.get_conn().create_stack(**params)

    def delete_stack(self, stack_name, params=None):
        """
        Delete stack in CloudFormation.

        :param stack_name: stack_name.
        :type stack_name: str
        :param params: parameters to be passed to CloudFormation (optional).
        :type params: dict
        """

        params = params or {}
        if 'StackName' not in params:
            params['StackName'] = stack_name
        self.get_conn().delete_stack(**params)
