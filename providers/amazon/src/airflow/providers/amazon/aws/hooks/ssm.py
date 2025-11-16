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

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.version_compat import NOTSET, ArgNotSet, is_arg_set

if TYPE_CHECKING:
    from airflow.sdk.execution_time.secrets_masker import mask_secret
else:
    try:
        from airflow.sdk.log import mask_secret
    except ImportError:
        try:
            from airflow.sdk.execution_time.secrets_masker import mask_secret
        except ImportError:
            from airflow.utils.log.secrets_masker import mask_secret


class SsmHook(AwsBaseHook):
    """
    Interact with Amazon Systems Manager (SSM).

    Provide thin wrapper around
    :external+boto3:py:class:`boto3.client("ssm") <SSM.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "ssm"
        super().__init__(*args, **kwargs)

    def get_parameter_value(self, parameter: str, default: str | ArgNotSet = NOTSET) -> str:
        """
        Return the provided Parameter or an optional default.

        If it is encrypted, then decrypt and mask.

        .. seealso::
            - :external+boto3:py:meth:`SSM.Client.get_parameter`

        :param parameter: The SSM Parameter name to return the value for.
        :param default: Optional default value to return if none is found.
        """
        try:
            param = self.conn.get_parameter(Name=parameter, WithDecryption=True)["Parameter"]
            value = param["Value"]
            if param["Type"] == "SecureString":
                mask_secret(value)
            return value
        except self.conn.exceptions.ParameterNotFound:
            if is_arg_set(default):
                return default
            raise

    def get_command_invocation(self, command_id: str, instance_id: str) -> dict:
        """
        Get the output of a command invocation for a specific instance.

        .. seealso::
            - :external+boto3:py:meth:`SSM.Client.get_command_invocation`

        :param command_id: The ID of the command.
        :param instance_id: The ID of the instance.
        :return: The command invocation details including output.
        """
        return self.conn.get_command_invocation(CommandId=command_id, InstanceId=instance_id)

    def list_command_invocations(self, command_id: str) -> dict:
        """
        List all command invocations for a given command ID.

        .. seealso::
            - :external+boto3:py:meth:`SSM.Client.list_command_invocations`

        :param command_id: The ID of the command.
        :return: Response from SSM list_command_invocations API.
        """
        return self.conn.list_command_invocations(CommandId=command_id)
