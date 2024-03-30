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

import json
from typing import TYPE_CHECKING, Any, Sequence

from airflow.providers.amazon.aws.hooks.bedrock import BedrockRuntimeHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BedrockInvokeModelOperator(AwsBaseOperator[BedrockRuntimeHook]):
    """
    Invoke the specified Bedrock model to run inference using the input provided.

    Use InvokeModel to run inference for text models, image models, and embedding models.
    To see the format and content of the input_data field for different models, refer to
    `Inference parameters docs <https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters.html>`_.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockInvokeModelOperator`

    :param model_id: The ID of the Bedrock model. (templated)
    :param input_data: Input data in the format specified in the content-type request header. (templated)
    :param content_type: The MIME type of the input data in the request. (templated) Default: application/json
    :param accept: The desired MIME type of the inference body in the response.
        (templated) Default: application/json

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = BedrockRuntimeHook
    template_fields: Sequence[str] = aws_template_fields(
        "model_id", "input_data", "content_type", "accept_type"
    )

    def __init__(
        self,
        model_id: str,
        input_data: dict[str, Any],
        content_type: str | None = None,
        accept_type: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.model_id = model_id
        self.input_data = input_data
        self.content_type = content_type
        self.accept_type = accept_type

    def execute(self, context: Context) -> dict[str, str | int]:
        # These are optional values which the API defaults to "application/json" if not provided here.
        invoke_kwargs = prune_dict({"contentType": self.content_type, "accept": self.accept_type})

        response = self.hook.conn.invoke_model(
            body=json.dumps(self.input_data),
            modelId=self.model_id,
            **invoke_kwargs,
        )

        response_body = json.loads(response["body"].read())
        self.log.info("Bedrock %s prompt: %s", self.model_id, self.input_data)
        self.log.info("Bedrock model response: %s", response_body)
        return response_body
