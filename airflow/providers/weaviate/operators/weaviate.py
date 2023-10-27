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
from typing import TYPE_CHECKING, Any, Callable, Collection, Mapping, Sequence

from airflow.exceptions import AirflowException
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
    :param class: The Weaviate class to be used for storing the data objects into.
    :param input_json: The JSON representing Weaviate data objects to generate embeddings on (or provides
        custom vectors) and store them in the Weaviate class. Either input_json or input_callable should be
        provided.
    :param input_callable: The callable that provides the input json to generate embeddings on
        (or provides custom vectors) and store them in the Weaviate class. Either input_text or
        input_callable should be provided.
    """

    template_fields: Sequence[str] = ("input_json",)

    def __init__(
        self,
        conn_id: str,
        class_name: str,
        input_json: dict[str, Any] | None = None,
        input_callable: Callable[[Any], Any] | None = None,
        input_callable_args: Collection[Any] | None = None,
        input_callable_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.batch_params = kwargs.pop("batch_params", {})
        self.hook_params = kwargs.pop("hook_params", {})
        super().__init__(**kwargs)
        self.class_name = class_name
        self.conn_id = conn_id
        self.input_json = input_json
        self.input_callable = input_callable
        self.input_callable_args = input_callable_args or ()
        self.input_callable_kwargs = input_callable_kwargs or {}

    @cached_property
    def hook(self) -> WeaviateHook:
        """Return an instance of the WeaviateHook."""
        return WeaviateHook(conn_id=self.conn_id, **self.hook_params)

    def execute(self, context: Context) -> None:
        if self.input_json and self.input_callable:
            raise RuntimeError("Only one of 'input_text' and 'input_callable' is allowed")
        if self.input_callable:
            if not callable(self.input_callable):
                raise AirflowException("`input_callable` param must be callable")
            input_json = self.input_callable(*self.input_callable_args, **self.input_callable_kwargs)
        elif self.input_json:
            input_json = self.input_json
        else:
            raise RuntimeError("Either one of 'input_json' and 'input_callable' must be provided")
        self.log.debug("Input json: %s", input_json)
        self.hook.batch_data(self.class_name, input_json, **self.batch_params)
