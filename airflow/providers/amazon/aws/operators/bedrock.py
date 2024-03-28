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
from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.bedrock import BedrockRuntimeHook
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.utils.context import Context


DEFAULT_CONN_ID = "aws_default"


class BedrockInvokeModelOperator(BaseOperator):
    """
    Invoke the specified Bedrock model to run inference using the input provided.

    Use InvokeModel to run inference for text models, image models, and embedding models.
    To see the format and content of the input_data field for different models, refer to
    `Inference parameters docs <https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters.html>`_.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BedrockInvokeModelOperator`

    :param model_id: The ID of the Bedrock model. (templated)
    :param prompt: The text prompt to provide to the model.
        Alternatively, this may be embedded in input_data. (templated)
    :param input_data: Input data in the format specified in the content-type request header. (templated)
    :param content_type: The MIME type of the input data in the request. (templated) Default: application/json
    :param accept: The desired MIME type of the inference body in the response.
        (templated) Default: application/json

    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param region: Which AWS region the connection should use. (templated)
         If this is None or empty then the default boto3 behaviour is used.
    """

    template_fields: Sequence[str] = (
        "model_id",
        "prompt",
        "input_data",
        "content_type",
        "accept_type",
        "aws_conn_id",
        "region",
    )

    def __init__(
        self,
        model_id: str,
        prompt: str | None = None,
        input_data: dict[str, str | int] | None = None,
        content_type: str | None = None,
        accept_type: str | None = None,
        aws_conn_id: str | None = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ):
        self.model_id = model_id
        self.prompt = prompt
        self.input_data = input_data or {}
        self.content_type = content_type
        self.accept_type = accept_type

        if self.prompt and self.input_data and "prompt" in self.input_data.keys():
            msg = "Either provide the prompt as a standalone parameter or include it in input_data."
            self.log.error(msg)
            raise ValueError(msg)

        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> BedrockRuntimeHook:
        return BedrockRuntimeHook(aws_conn_id=self.aws_conn_id, region_name=self.region)

    def execute(self, context: Context) -> dict[str, str | int]:
        # These are optional values which the API defaults to "application/json" if not provided here.
        invoke_kwargs = prune_dict({"contentType": self.content_type, "accept": self.accept_type})

        request_body = prune_dict({"prompt": self.prompt, **self.input_data})

        response = self.hook.conn.invoke_model(
            body=json.dumps(request_body),
            modelId=self.model_id,
            **invoke_kwargs,
        )

        response_body = json.loads(response["body"].read())
        self.log.info("Bedrock prompt: %s", self.prompt)
        self.log.info("Bedrock model response: %s", response_body["generation"].replace("\n", " "))
        return response_body
