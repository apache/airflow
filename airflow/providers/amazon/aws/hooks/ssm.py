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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.types import NOTSET, ArgNotSet


class SsmHook(AwsBaseHook):
    """
    Interact with Amazon Systems Manager (SSM).
    Provide thin wrapper around :external+boto3:py:class:`boto3.client("ssm") <SSM.Client>`.

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
        Returns the value of the provided Parameter or an optional default.
        If value exists, and it is encrypted, then decrypt and mask them for loggers.

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
            if isinstance(default, ArgNotSet):
                raise
            return default
