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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseSensorOperator
from airflow.providers.microsoft.azure.hooks.cosmos import AzureCosmosDBHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureCosmosDocumentSensor(BaseSensorOperator):
    """
    Checks for the existence of a document which matches the given query in CosmosDB.

    .. code-block:: python

        azure_cosmos_sensor = AzureCosmosDocumentSensor(
            database_name="somedatabase_name",
            collection_name="somecollection_name",
            document_id="unique-doc-id",
            azure_cosmos_conn_id="azure_cosmos_default",
            task_id="azure_cosmos_sensor",
        )

    :param database_name: Target CosmosDB database_name.
    :param collection_name: Target CosmosDB collection_name.
    :param document_id: The ID of the target document.
    :param azure_cosmos_conn_id: Reference to the
        :ref:`Azure CosmosDB connection<howto/connection:azure_cosmos>`.
    """

    template_fields: Sequence[str] = ("database_name", "collection_name", "document_id")

    def __init__(
        self,
        *,
        database_name: str,
        collection_name: str,
        document_id: str,
        azure_cosmos_conn_id: str = "azure_cosmos_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.azure_cosmos_conn_id = azure_cosmos_conn_id
        self.database_name = database_name
        self.collection_name = collection_name
        self.document_id = document_id

    def poke(self, context: Context) -> bool:
        self.log.info("*** Entering poke")
        hook = AzureCosmosDBHook(self.azure_cosmos_conn_id)
        return hook.get_document(self.document_id, self.database_name, self.collection_name) is not None
