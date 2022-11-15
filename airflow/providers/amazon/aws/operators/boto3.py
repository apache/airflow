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
from airflow.compat.functools import cached_property
from airflow.exceptions import ParamValidationError
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class Boto3BaseOperator(BaseOperator):
    """Base boto3 operator for interating with AWS via boto3"""

    template_fields: Sequence[str] = (
        "client_type",
        "client_method",
        "method_kwargs",
    )

    def __init__(
        self,
        aws_conn_id: str | None = AwsBaseHook.default_conn_name,
        hook_params: dict | None = None,
        client_type: str | None = None,
        client_method: str | None = None,
        method_kwargs: dict | None = None,
        **kwargs,
    ):
        """
        Args:
            aws_conn_id (str, optional): AWS connection id. Defaults to "aws_conn_id".
            hook_params (dict, optional): Parameters to pass to AwsBaseHook. Defaults to None.
            client_type (str, optional): boto3 client. Ex: "rds", "ec2", "s3", etc. Defaults to None.
            client_method (str, optional): boto3 function that the operator should call on the client. Example: "describe_db_snapshots" (with `client_type`="rds"). Defaults to None.
            method_kwargs (dict, optional): Parameters to pass to the client_method. Example: {"DBInstanceIdentifier": "my-database"} Defaults to None.
        """
        if not client_method.isidentifier():
            raise ParamValidationError(f'"{client_method}" is not valid client_method')

        self.client_type = client_type
        self.client_method = client_method
        self.method_kwargs = method_kwargs or {}

        self.hook_params = hook_params or {}

        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> AwsBaseHook:
        return AwsBaseHook(aws_conn_id=self.aws_conn_id, **self.hook_params, client_type=self.client_type)

    @property
    def boto3_action(
        self,
    ):
        method = getattr(self.hook.conn, self.client_method)
        if not callable(method):
            raise ParamValidationError(
                f'Method "{self.client_method}" does not exist on boto3 client "{self.client_type}"'
            )
        return method


class Boto3Operator(Boto3BaseOperator):
    """boto3 operator that implements interacting with boto3 client"""

    def __init__(self, result_handler: Callable = None, **kwargs):
        """
        Args:
            result_handler (Callable, optional): python function that accepts boto3 call result and does some data transformation. Return value from that function saved as xcom. Defaults to None.
        """
        self.handle_result = result_handler
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        result = self.boto3_action(**self.method_kwargs)

        if self.handle_result is not None:
            result = self.handle_result(result)

        return result
