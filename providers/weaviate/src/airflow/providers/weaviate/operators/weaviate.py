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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

if TYPE_CHECKING:
    import pandas as pd
    from weaviate.types import UUID

    from airflow.providers.common.compat.sdk import Context


class WeaviateIngestOperator(BaseOperator):
    """
    Operator that store vector in the Weaviate class.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WeaviateIngestOperator`

    Operator that accepts input json or pandas dataframe to generate embeddings on or accepting provided
    custom vectors and store them in the Weaviate class.

    :param conn_id: The Weaviate connection.
    :param collection: The Weaviate collection to be used for storing the data objects into.
    :param input_data: The list of dicts or pandas dataframe representing Weaviate data objects to generate
        embeddings on (or provides custom vectors) and store them in the Weaviate class.
    :param vector_col: key/column name in which the vectors are stored.
    :param hook_params: Optional config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    """

    template_fields: Sequence[str] = ("input_data",)

    def __init__(
        self,
        conn_id: str,
        collection_name: str,
        input_data: list[dict[str, Any]] | pd.DataFrame | None = None,
        vector_col: str = "Vector",
        uuid_column: str = "id",
        tenant: str | None = None,
        hook_params: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.conn_id = conn_id
        self.vector_col = vector_col
        self.uuid_column = uuid_column
        self.tenant = tenant
        self.input_data = input_data
        self.hook_params = hook_params or {}

        if self.input_data is None:
            raise TypeError("input_data is required")

    @cached_property
    def hook(self) -> WeaviateHook:
        """Return an instance of the WeaviateHook."""
        return WeaviateHook(conn_id=self.conn_id, **self.hook_params)

    def execute(self, context: Context) -> None:
        self.log.debug("Input data: %s", self.input_data)
        self.hook.batch_data(
            collection_name=self.collection_name,
            data=self.input_data,
            vector_col=self.vector_col,
            uuid_col=self.uuid_column,
        )


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
    :param collection_name: Name of the collection in Weaviate schema where data is to be ingested.
    :param existing: Strategy for handling existing data: 'skip', or 'replace'. Default is 'skip'.
    :param document_column: Column in DataFrame that identifying source document.
    :param uuid_column: Column with pre-generated UUIDs. If not provided, UUIDs will be generated.
    :param vector_column: Column with embedding vectors for pre-embedded data.
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
        collection_name: str,
        document_column: str,
        existing: str = "skip",
        uuid_column: str = "id",
        vector_col: str = "Vector",
        tenant: str | None = None,
        verbose: bool = False,
        hook_params: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_data = input_data
        self.collection_name = collection_name
        self.document_column = document_column
        self.existing = existing
        self.uuid_column = uuid_column
        self.vector_col = vector_col
        self.tenant = tenant
        self.verbose = verbose
        self.hook_params = hook_params or {}

    @cached_property
    def hook(self) -> WeaviateHook:
        """Return an instance of the WeaviateHook."""
        return WeaviateHook(conn_id=self.conn_id, **self.hook_params)

    def execute(self, context: Context) -> Sequence[dict[str, UUID | str] | None]:
        """
        Create or replace objects belonging to documents.

        :return: List of UUID which failed to create
        """
        self.log.debug("Total input objects : %s", len(self.input_data))
        batch_delete_error = self.hook.create_or_replace_document_objects(
            data=self.input_data,
            collection_name=self.collection_name,
            document_column=self.document_column,
            existing=self.existing,
            uuid_column=self.uuid_column,
            vector_column=self.vector_col,
            verbose=self.verbose,
        )
        return batch_delete_error
