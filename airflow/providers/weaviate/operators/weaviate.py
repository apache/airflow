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
    :param vector_col: key/column name in which the vectors are stored.
    :param batch_params: Additional parameters for Weaviate batch configuration.
    :param hook_params: Optional config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    :param input_json: (Deprecated) The JSON representing Weaviate data objects to generate embeddings on
        (or provides custom vectors) and store them in the Weaviate class.
    """

    template_fields: Sequence[str] = ("input_json", "input_data")

    def __init__(
        self,
        conn_id: str,
        class_name: str,
        input_data: list[dict[str, Any]] | pd.DataFrame | None = None,
        vector_col: str = "Vector",
        uuid_column: str = "id",
        tenant: str | None = None,
        batch_params: dict | None = None,
        hook_params: dict | None = None,
        input_json: list[dict[str, Any]] | pd.DataFrame | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.class_name = class_name
        self.conn_id = conn_id
        self.vector_col = vector_col
        self.input_json = input_json
        self.uuid_column = uuid_column
        self.tenant = tenant
        self.input_data = input_data
        self.batch_params = batch_params or {}
        self.hook_params = hook_params or {}

        if (self.input_data is None) and (input_json is not None):
            warnings.warn(
                "Passing 'input_json' to WeaviateIngestOperator is deprecated and"
                " you should use 'input_data' instead",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            self.input_data = input_json
        elif self.input_data is None and input_json is None:
            raise TypeError("Either input_json or input_data is required")

    @cached_property
    def hook(self) -> WeaviateHook:
        """Return an instance of the WeaviateHook."""
        return WeaviateHook(conn_id=self.conn_id, **self.hook_params)

    def execute(self, context: Context) -> list:
        self.log.debug("Input data: %s", self.input_data)
        insertion_errors: list = []
        self.hook.batch_data(
            class_name=self.class_name,
            data=self.input_data,
            batch_config_params=self.batch_params,
            vector_col=self.vector_col,
            uuid_col=self.uuid_column,
            tenant=self.tenant,
        )
        return insertion_errors


class WeaviateDocumentIngestOperator(BaseOperator):
    """
    Create or replace objects belonging to documents.

    In real-world scenarios, information sources like Airflow docs, Stack Overflow, or other issues
    are considered 'documents' here. It's crucial to keep the database objects in sync with these sources.
    If any changes occur in these documents, this function aims to reflect those changes in the database.

    .. note::

        This function assumes responsibility for identifying changes in documents, dropping relevant
        database objects, and recreating them based on updated information. It's crucial to handle this
        process with care, ensuring backups and validation are in place to prevent data loss or
        inconsistencies.

    Provides users with multiple ways of dealing with existing values.
    replace: replace the existing objects with new objects. This option requires to identify the
    objects belonging to a document. which by default is done by using document_column field.
    skip: skip the existing objects and only add the missing objects of a document.
    error: raise an error if an object belonging to a existing document is tried to be created.

    :param data: A single pandas DataFrame or a list of dicts to be ingested.
    :param class_name: Name of the class in Weaviate schema where data is to be ingested.
    :param existing: Strategy for handling existing data: 'skip', or 'replace'. Default is 'skip'.
    :param document_column: Column in DataFrame that identifying source document.
    :param uuid_column: Column with pre-generated UUIDs. If not provided, UUIDs will be generated.
    :param vector_column: Column with embedding vectors for pre-embedded data.
    :param batch_config_params: Additional parameters for Weaviate batch configuration.
    :param tenant: The tenant to which the object will be added.
    :param verbose: Flag to enable verbose output during the ingestion process.
    :param hook_params: Optional config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    """

    template_fields: Sequence[str] = ("input_data",)

    def __init__(
        self,
        conn_id: str,
        input_data: pd.DataFrame | list[dict[str, Any]] | list[pd.DataFrame],
        class_name: str,
        document_column: str,
        existing: str = "skip",
        uuid_column: str = "id",
        vector_col: str = "Vector",
        batch_config_params: dict | None = None,
        tenant: str | None = None,
        verbose: bool = False,
        hook_params: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_data = input_data
        self.class_name = class_name
        self.document_column = document_column
        self.existing = existing
        self.uuid_column = uuid_column
        self.vector_col = vector_col
        self.batch_config_params = batch_config_params
        self.tenant = tenant
        self.verbose = verbose
        self.hook_params = hook_params or {}

    @cached_property
    def hook(self) -> WeaviateHook:
        """Return an instance of the WeaviateHook."""
        return WeaviateHook(conn_id=self.conn_id, **self.hook_params)

    def execute(self, context: Context) -> list:
        """
        Create or replace objects belonging to documents.

        :return: List of UUID which failed to create
        """
        self.log.debug("Total input objects : %s", len(self.input_data))
        insertion_errors = self.hook.create_or_replace_document_objects(
            data=self.input_data,
            class_name=self.class_name,
            document_column=self.document_column,
            existing=self.existing,
            uuid_column=self.uuid_column,
            vector_column=self.vector_col,
            batch_config_params=self.batch_config_params,
            tenant=self.tenant,
            verbose=self.verbose,
        )
        return insertion_errors
