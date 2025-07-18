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


class BedrockHook(AwsBaseHook):
    """
    Interact with Amazon Bedrock.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock") <Bedrock.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)


class BedrockRuntimeHook(AwsBaseHook):
    """
    Interact with the Amazon Bedrock Runtime.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-runtime") <BedrockRuntime.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-runtime"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)


class BedrockAgentHook(AwsBaseHook):
    """
    Interact with the Amazon Agents for Bedrock API.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-agent") <AgentsforBedrock.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-agent"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)


class BedrockAgentRuntimeHook(AwsBaseHook):
    """
    Interact with the Amazon Agents for Bedrock API.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("bedrock-agent-runtime") <AgentsforBedrockRuntime.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = "bedrock-agent-runtime"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)
