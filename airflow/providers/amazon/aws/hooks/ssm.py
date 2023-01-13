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
from airflow.utils.types import NOTSET, ArgNotSet


class SsmHook(AwsBaseHook):
    """
    Interact with Amazon Systems Manager (SSM) using the boto3 library.
    All API calls available through the Boto API are also available here.
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#client

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "ssm"
        super().__init__(*args, **kwargs)

    def get_parameter_value(self, parameter: str, default: str | ArgNotSet = NOTSET) -> str:
        """
        Returns the value of the provided Parameter or an optional default.

        :param parameter: The SSM Parameter name to return the value for.
        :param default: Optional default value to return if none is found.
        """
        try:
            return self.conn.get_parameter(Name=parameter)["Parameter"]["Value"]
        except self.conn.exceptions.ParameterNotFound:
            if isinstance(default, ArgNotSet):
                raise
            return default
