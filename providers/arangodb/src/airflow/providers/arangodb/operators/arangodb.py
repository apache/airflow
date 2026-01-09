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
from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.arangodb.hooks.arangodb import ArangoDBHook
from airflow.providers.common.compat.sdk import AirflowException, BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class AQLOperator(BaseOperator):
    """
    Executes AQL query in a ArangoDB database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AQLOperator`

    :param query: the AQL query to be executed. Can receive a str representing a
        AQL statement, or you can provide .sql file having the query
    :param result_processor: function to further process the Result from ArangoDB
    :param arangodb_conn_id: Reference to :ref:`ArangoDB connection id <howto/connection:arangodb>`.
    """

    template_fields: Sequence[str] = ("query",)
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"query": "sql"}

    def __init__(
        self,
        *,
        query: str,
        arangodb_conn_id: str = "arangodb_default",
        result_processor: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.arangodb_conn_id = arangodb_conn_id
        self.query = query
        self.result_processor = result_processor

    def execute(self, context: Context):
        self.log.info("Executing: %s", self.query)
        hook = ArangoDBHook(arangodb_conn_id=self.arangodb_conn_id)
        result = hook.query(self.query)
        if self.result_processor:
            self.result_processor(result)


class ArangoDBCollectionOperator(BaseOperator):
    """
    Executes collection operations in a ArangoDB database.

    :param arangodb_conn_id: Connection ID for ArangoDB, defaults to "arangodb_default".
    :param collection_name: The name of the collection to be operated on.
    :param documents_to_insert: A list of python dictionaries to insert into the collection.
    :param documents_to_update: A list of python dictionaries to update in the collection.
    :param documents_to_replace: A list of python dictionaries to replace in the collection.
    :param documents_to_delete: A list of python dictionaries to delete from the collection.
    :param delete_collection: If True, the specified collection will be deleted.
    """

    def __init__(
        self,
        *,
        arangodb_conn_id: str = "arangodb_default",
        collection_name: str,
        documents_to_insert: list[dict[str, Any]] | None = None,
        documents_to_update: list[dict[str, Any]] | None = None,
        documents_to_replace: list[dict[str, Any]] | None = None,
        documents_to_delete: list[dict[str, Any]] | None = None,
        delete_collection: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.arangodb_conn_id = arangodb_conn_id
        self.collection_name = collection_name
        self.documents_to_insert = documents_to_insert or []
        self.documents_to_update = documents_to_update or []
        self.documents_to_replace = documents_to_replace or []
        self.documents_to_delete = documents_to_delete or []
        self.delete_collection = delete_collection

    def execute(self, context: Context):
        hook = ArangoDBHook(arangodb_conn_id=self.arangodb_conn_id)

        if not any(
            [
                self.documents_to_insert,
                self.documents_to_update,
                self.documents_to_replace,
                self.documents_to_delete,
                self.delete_collection,
            ]
        ):
            raise AirflowException("At least one operation must be specified.")

        if self.documents_to_insert:
            self.log.info(
                "Inserting %d documents into collection '%s'.",
                len(self.documents_to_insert),
                self.collection_name,
            )
            hook.insert_documents(self.collection_name, self.documents_to_insert)

        if self.documents_to_update:
            self.log.info(
                "Updating %d documents in collection '%s'.",
                len(self.documents_to_update),
                self.collection_name,
            )
            hook.update_documents(self.collection_name, self.documents_to_update)

        if self.documents_to_replace:
            self.log.info(
                "Replacing %d documents in collection '%s'.",
                len(self.documents_to_replace),
                self.collection_name,
            )
            hook.replace_documents(self.collection_name, self.documents_to_replace)

        if self.documents_to_delete:
            self.log.info(
                "Deleting %d documents from collection '%s'.",
                len(self.documents_to_delete),
                self.collection_name,
            )
            hook.delete_documents(self.collection_name, self.documents_to_delete)

        if self.delete_collection:
            self.log.info("Deleting collection '%s'.", self.collection_name)
            hook.delete_collection(self.collection_name)
