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

"""This module contains AWS CloudFormation Hook"""
from typing import Optional, Union

from boto3 import client, resource
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AWSCloudFormationHook(AwsBaseHook):
    """
    Interact with AWS CloudFormation.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type='cloudformation', *args, **kwargs)

    def get_stack_status(self, stack_name: Union[client, resource]) -> Optional[dict]:
        """Get stack status from CloudFormation."""
        self.log.info('Poking for stack %s', stack_name)

        try:
            stacks = self.get_conn().describe_stacks(StackName=stack_name)['Stacks']
            return stacks[0]['StackStatus']
        except ClientError as e:
            if 'does not exist' in str(e):
                return None
            else:
                raise e

    def create_stack(self, stack_name: str, params: dict) -> None:
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

    def delete_stack(self, stack_name: str, params: Optional[dict] = None) -> None:
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
