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

import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

if TYPE_CHECKING:
    import pandas as pd

    from airflow.utils.context import Context


class WeaviateIngestOperator(BaseOperator):
    """
    Operator that store vector in the Weaviate class.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WeaviateIngestOperator`

    Operator that accepts input json or pandas dataframe to generate embeddings on or accepting provided
    custom vectors and store them in the Weaviate class.

    :param conn_id: The Weaviate connection.
    :param class_name: The Weaviate class to be used for storing the data objects into.
    :param input_data: The list of dicts or pandas dataframe representing Weaviate data objects to generate
        embeddings on (or provides custom vectors) and store them in the Weaviate class.
    :param input_json: (Deprecated) The JSON representing Weaviate data objects to generate embeddings on (or provides
        custom vectors) and store them in the Weaviate class.
    :param vector_col: key/column name in which the vectors are stored.
    """

    template_fields: Sequence[str] = ("input_json", "input_data")

    def __init__(
        self,
        conn_id: str,
        class_name: str,
        input_json: list[dict[str, Any]] | pd.DataFrame | None = None,
        input_data: list[dict[str, Any]] | pd.DataFrame | None = None,
        vector_col: str = "Vector",
        uuid_column: str = "id",
        tenant: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.batch_params = kwargs.pop("batch_params", {})
        self.hook_params = kwargs.pop("hook_params", {})

        super().__init__(**kwargs)
        self.class_name = class_name
        self.conn_id = conn_id
        self.vector_col = vector_col
        self.input_json = input_json
        self.uuid_column = uuid_column
        self.tenant = tenant
        if input_data is not None:
            self.input_data = input_data
        elif input_json is not None:
            warnings.warn(
                "Passing 'input_json' to WeaviateIngestOperator is deprecated and"
                " you should use 'input_data' instead",
                AirflowProviderDeprecationWarning,
            )
            self.input_data = input_json
        else:
            raise TypeError("Either input_json or input_data is required")

    @cached_property
    def hook(self) -> WeaviateHook:
        """Return an instance of the WeaviateHook."""
        return WeaviateHook(conn_id=self.conn_id, **self.hook_params)

    def execute(self, context: Context):
        self.log.debug("Input data: %s", self.input_data)
        insertion_errors: list = []
        self.hook.batch_data(
            class_name=self.class_name,
            data=self.input_data,
            batch_config_params=self.batch_params,
            vector_col=self.vector_col,
            insertion_errors=insertion_errors,
            uuid_col=self.uuid_column,
            tenant=self.tenant,
        )
        return insertion_errors
