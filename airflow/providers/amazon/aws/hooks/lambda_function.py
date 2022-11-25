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
"""This module contains AWS Lambda hook"""
from __future__ import annotations

from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class LambdaHook(AwsBaseHook):
    """
    Interact with AWS Lambda

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param function_name: AWS Lambda Function Name
    :param log_type: Tail Invocation Request
    :param qualifier: AWS Lambda Function Version or Alias Name
    :param invocation_type: AWS Lambda Invocation Type (RequestResponse, Event etc)
    """

    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        kwargs["client_type"] = "lambda"
        super().__init__(*args, **kwargs)

    def invoke_lambda(
        self,
        *,
        function_name: str,
        invocation_type: str | None = None,
        log_type: str | None = None,
        client_context: str | None = None,
        payload: str | None = None,
        qualifier: str | None = None,
    ):
        """Invoke Lambda Function. Refer to the boto3 documentation for more info."""
        invoke_args = {
            "FunctionName": function_name,
            "InvocationType": invocation_type,
            "LogType": log_type,
            "ClientContext": client_context,
            "Payload": payload,
            "Qualifier": qualifier,
        }
        return self.conn.invoke(**{k: v for k, v in invoke_args.items() if v is not None})

    def create_lambda(
        self,
        *,
        function_name: str,
        runtime: str | None = None,
        role: str,
        handler: str | None = None,
        code: dict,
        description: str | None = None,
        timeout: int | None = None,
        memory_size: int | None = None,
        publish: bool | None = None,
        vpc_config: Any | None = None,
        package_type: str | None = None,
        dead_letter_config: Any | None = None,
        environment: Any | None = None,
        kms_key_arn: str | None = None,
        tracing_config: Any | None = None,
        tags: Any | None = None,
        layers: list | None = None,
        file_system_configs: list[Any] | None = None,
        image_config: Any | None = None,
        code_signing_config_arn: str | None = None,
        architectures: list[str] | None = None,
    ) -> dict:
        if package_type == "Zip":
            if handler is None:
                raise TypeError("Parameter 'handler' is required if 'package_type' is 'Zip'")
            if runtime is None:
                raise TypeError("Parameter 'runtime' is required if 'package_type' is 'Zip'")

        """Create a Lambda Function"""
        create_function_args = {
            "FunctionName": function_name,
            "Runtime": runtime,
            "Role": role,
            "Handler": handler,
            "Code": code,
            "Description": description,
            "Timeout": timeout,
            "MemorySize": memory_size,
            "Publish": publish,
            "VpcConfig": vpc_config,
            "PackageType": package_type,
            "DeadLetterConfig": dead_letter_config,
            "Environment": environment,
            "KMSKeyArn": kms_key_arn,
            "TracingConfig": tracing_config,
            "Tags": tags,
            "Layers": layers,
            "FileSystemConfigs": file_system_configs,
            "ImageConfig": image_config,
            "CodeSigningConfigArn": code_signing_config_arn,
            "Architectures": architectures,
        }
        return self.conn.create_function(
            **{k: v for k, v in create_function_args.items() if v is not None},
        )
