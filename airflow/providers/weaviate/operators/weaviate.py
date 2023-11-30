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
<<<<<<< HEAD
    :param input_data: The list of dicts or pandas dataframe representing Weaviate data objects to generate
        embeddings on (or provides custom vectors) and store them in the Weaviate class.
    :param input_json: (Deprecated) The JSON representing Weaviate data objects to generate embeddings on (or provides
        custom vectors) and store them in the Weaviate class.
=======
    :param input_data: The list of dicts or pandas dataframe representing Weaviate data objects to generate\
        embeddings on (or provides custom vectors) and store them in the Weaviate class. Either input_json
        or input_callable should be provided.
>>>>>>> e9508bb3b4 (Resolve conflicts)
    :param vector_col: key/column name in which the vectors are stored.
    """

    template_fields: Sequence[str] = ("input_json",)

    def __init__(
        self,
        conn_id: str,
        class_name: str,
<<<<<<< HEAD
        input_json: list[dict[str, Any]] | pd.DataFrame | None = None,
        input_data: list[dict[str, Any]] | pd.DataFrame | None = None,
=======
        input_data: list[dict[str, Any]] | pd.DataFrame,
>>>>>>> e9508bb3b4 (Resolve conflicts)
        vector_col: str = "Vector",
        **kwargs: Any,
    ) -> None:
        self.batch_params = kwargs.pop("batch_params", {})
        self.hook_params = kwargs.pop("hook_params", {})

        super().__init__(**kwargs)
        self.class_name = class_name
        self.conn_id = conn_id
<<<<<<< HEAD
        self.vector_col = vector_col

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
=======
        self.input_data = input_data
        self.vector_col = vector_col
>>>>>>> e9508bb3b4 (Resolve conflicts)

    @cached_property
    def hook(self) -> WeaviateHook:
        """Return an instance of the WeaviateHook."""
        return WeaviateHook(conn_id=self.conn_id, **self.hook_params)

    def execute(self, context: Context) -> None:
        self.log.debug("Input data: %s", self.input_data)
        self.hook.batch_data(
<<<<<<< HEAD
            self.class_name,
            self.input_data,
            **self.batch_params,
            vector_col=self.vector_col,
=======
            self.class_name, self.input_data, vector_col=self.vector_col, **self.batch_params
>>>>>>> e9508bb3b4 (Resolve conflicts)
        )
