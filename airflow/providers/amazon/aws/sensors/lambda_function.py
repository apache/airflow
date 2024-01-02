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
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils import trim_none_values
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LambdaFunctionStateSensor(AwsBaseSensor[LambdaHook]):
    """
    Poll the deployment state of the AWS Lambda function until it reaches a target state.

    Fails if the query fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:LambdaFunctionStateSensor`

    :param function_name: The name of the AWS Lambda function, version, or alias.
    :param qualifier: Specify a version or alias to get details about a published version of the function.
    :param target_states: The Lambda states desired.
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    """

    FAILURE_STATES = ("Failed",)

    aws_hook_class = LambdaHook
    template_fields: Sequence[str] = aws_template_fields(
        "function_name",
        "qualifier",
    )

    def __init__(
        self,
        *,
        function_name: str,
        qualifier: str | None = None,
        target_states: list = ["Active"],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.function_name = function_name
        self.qualifier = qualifier
        self.target_states = target_states

    def poke(self, context: Context) -> bool:
        get_function_args = {
            "FunctionName": self.function_name,
            "Qualifier": self.qualifier,
        }
        state = self.hook.conn.get_function(**trim_none_values(get_function_args))["Configuration"]["State"]

        if state in self.FAILURE_STATES:
            message = "Lambda function state sensor failed because the Lambda is in a failed state"
            # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)

        return state in self.target_states
