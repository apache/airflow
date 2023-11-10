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

from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class WeaviateIngestOperator(BaseOperator):
    """
    Operator that store vector in the Weaviate class.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WeaviateIngestOperator`

    Operator that accepts input json to generate embeddings on or accepting provided custom vectors
    and store them in the Weaviate class.

    :param conn_id: The Weaviate connection.
    :param class_name: The Weaviate class to be used for storing the data objects into.
    :param input_json: The JSON representing Weaviate data objects to generate embeddings on (or provides
        custom vectors) and store them in the Weaviate class. Either input_json or input_callable should be
        provided.
    """

    template_fields: Sequence[str] = ("input_json",)

    def __init__(
        self,
        conn_id: str,
        class_name: str,
        input_json: list[dict[str, Any]],
        **kwargs: Any,
    ) -> None:
        self.batch_params = kwargs.pop("batch_params", {})
        self.hook_params = kwargs.pop("hook_params", {})
        super().__init__(**kwargs)
        self.class_name = class_name
        self.conn_id = conn_id
        self.input_json = input_json

    @cached_property
    def hook(self) -> WeaviateHook:
        """Return an instance of the WeaviateHook."""
        return WeaviateHook(conn_id=self.conn_id, **self.hook_params)

    def execute(self, context: Context) -> None:
        self.log.debug("Input json: %s", self.input_json)
        self.hook.batch_data(self.class_name, self.input_json, **self.batch_params)
