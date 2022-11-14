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

from typing import TYPE_CHECKING, Any, Callable, Sequence
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class Boto3BaseOperator(BaseOperator):
    """Base boto3 operator for interating with AWS via boto3"""

    template_fields: Sequence[str] = ("boto3_kwargs",)

    def __init__(
        self,
        *args,
        aws_conn_id: str = "aws_conn_id",
        hook_params: dict = None,
        boto3_callable: str = None,
        boto3_kwargs: dict = None,
        **kwargs,
    ):
        """
        Args:
            aws_conn_id (str, optional): AWS connection id. Defaults to "aws_conn_id".
            hook_params (dict, optional): Parameters to pass to AwsBaseHook. Defaults to None.
            boto3_callable (str, optional): boto3 function that the operator should call. Format: <client>.<function>. Example: "rds.describe_db_snapshots". Defaults to None.
            boto3_kwargs (dict, optional): Parameters to pass to the boto3_callable. Example: {"DBInstanceIdentifier": "my-database"} Defaults to None.
        """
        assert (
            boto3_callable and len(boto3_callable.split(".")) == 2
        ), f"Wrong boto3_callable: Got '{boto3_callable}' but expected format: '<boto3_client_type>.<function_to_call>'"

        client_type, self.client_callable = boto3_callable.split(".")
        self.boto3_kwargs = boto3_kwargs or {}

        hook_params = hook_params or {}
        self.hook = AwsBaseHook(aws_conn_id=aws_conn_id, **hook_params, client_type=client_type)
        super().__init__(*args, **kwargs)

    @property
    def boto3_action(
        self,
    ):
        return getattr(self.hook.conn, self.client_callable)


class Boto3Operator(Boto3BaseOperator):
    """boto3 operator that implements interacting with boto3 client"""

    template_fields: Sequence[str] = ("boto3_kwargs",)

    def __init__(self, *args, result_handler: Callable = None, **kwargs):
        """
        Args:
            result_handler (Callable, optional): python function that accepts boto3 call result and does some data transformation. Return value from that function saved as xcom. Defaults to None.
        """
        self.handle_result = result_handler
        super().__init__(*args, **kwargs)

    def execute(self, context: Context) -> Any:
        result = self.boto3_action(**self.boto3_kwargs)

        if self.handle_result is not None:
            result = self.handle_result(result)

        if not isinstance(result, str):
            import json

            return json.dumps(result, default=str)

        return result
