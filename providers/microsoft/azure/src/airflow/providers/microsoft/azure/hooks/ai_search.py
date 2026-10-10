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

from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.identity.aio import ClientSecretCredential
from azure.search.documents.aio import SearchClient

from airflow.providers.common.compat.connection import get_async_connection, get_async_extra_dejson
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.exceptions import AzureAISearchIndexingError
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_async_default_azure_credential,
    get_field,
)

if TYPE_CHECKING:
    from azure.core.credentials_async import AsyncTokenCredential

    from airflow.sdk import Connection


class AzureAISearchHook(BaseHook):
    """
    Query and write the documents of one Azure AI Search index, natively async.

    A thin layer over :class:`azure.search.documents.aio.SearchClient`. The SDK pages the search
    results and retries throttled requests (429 and 503) with their ``Retry-After``; the hook binds
    the client to one index and adds what every Dag would otherwise repeat:

    * :meth:`search` is an async generator over every matching document, without the ``@search.*``
      metadata.
    * :meth:`count` and :meth:`get_document` (``None`` when the index has no such document).
    * :meth:`upload`, :meth:`merge`, :meth:`merge_or_upload` and :meth:`delete` write in batches of
      ``batch_size`` documents and raise
      :class:`~airflow.providers.microsoft.azure.exceptions.AzureAISearchIndexingError` when the
      service rejects any document, naming the keys and reasons. The SDK itself returns the rejected
      documents of a partially failed write without raising.

    Other errors are the SDK's: :class:`azure.core.exceptions.HttpResponseError` with its
    ``status_code``, and ``ServiceRequestError`` or ``ServiceResponseError`` when the connection fails.

    Every method is a coroutine, so the hook is meant for async tasks and triggers. The credential is
    chosen in this order:

    1. Login, password and ``tenantId``: a service principal.
    2. Password alone: an API key (a query key is enough to read, writing needs an admin key).
    3. Neither: ``DefaultAzureCredential``, i.e. a managed or workload identity, or the environment.

    With Microsoft Entra ID the identity needs the *Search Index Data Reader* role, or
    *Search Index Data Contributor* to write.

    .. seealso::
        https://learn.microsoft.com/en-us/azure/search/

    :param azure_ai_search_conn_id: The :ref:`Azure AI Search connection id<howto/connection:azure_ai_search>`.
        Default is ``azure_ai_search_default``.
    :param index: The index this hook works on. Overrides the ``index`` connection extra.
    :param batch_size: Documents per write request, at most 1000 (the limit of the service). Overrides
        the ``batch_size`` connection extra. Lower it for large documents: the service also refuses a
        request over about 16 MB.
    """

    conn_name_attr = "azure_ai_search_conn_id"
    default_conn_name = "azure_ai_search_default"
    conn_type = "azure_ai_search"
    hook_name = "Azure AI Search"

    # The service accepts at most 1000 documents per write request.
    MAX_BATCH_SIZE = 1000

    def __init__(
        self,
        azure_ai_search_conn_id: str = default_conn_name,
        index: str | None = None,
        batch_size: int | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = azure_ai_search_conn_id
        self.index = index
        # Checked here, so that a wrong value fails when the Dag builds the hook.
        self.batch_size = None if batch_size is None else self._valid_batch_size(batch_size, "batch_size")
        # Resolved from the hook or the connection each time a client is built.
        self._index: str | None = index
        self._batch_size = self.batch_size or self.MAX_BATCH_SIZE

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import IntegerField, StringField
        from wtforms.validators import NumberRange, Optional

        return {
            "tenantId": StringField(lazy_gettext("Azure Tenant ID"), widget=BS3TextFieldWidget()),
            "index": StringField(lazy_gettext("Default Index"), widget=BS3TextFieldWidget()),
            "batch_size": IntegerField(
                lazy_gettext("Batch Size"),
                widget=BS3TextFieldWidget(),
                validators=[Optional(), NumberRange(min=1, max=cls.MAX_BATCH_SIZE)],
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {
                "host": "Search Service Endpoint",
                "login": "Azure Client ID",
                "password": "Azure Secret or API Key",
            },
            "placeholders": {
                "host": "https://<service>.search.windows.net",
                "login": "client_id (service principal only)",
                "password": "secret (service principal), or an API key when no client_id is set",
                "tenantId": "tenantId (service principal only)",
                "index": "index used when the hook is given none",
                "batch_size": "documents per write request, 1 to 1000 (default 1000)",
            },
        }

    def _get_field(self, extras: dict[str, Any], field_name: str) -> Any:
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=field_name,
        )

    def _get_credential(
        self, conn: Connection, extras: dict[str, Any]
    ) -> AzureKeyCredential | AsyncTokenCredential:
        tenant = self._get_field(extras, "tenantId")
        if conn.login:
            if not (conn.password and tenant):
                raise ValueError(
                    "Azure Client ID, Azure Secret, and Azure Tenant ID must all be provided when "
                    "authenticating with a service principal."
                )
            self.log.info("Getting connection using specific credentials.")
            return ClientSecretCredential(tenant_id=tenant, client_id=conn.login, client_secret=conn.password)
        if conn.password:
            self.log.info("Getting connection using an API key.")
            return AzureKeyCredential(conn.password)
        self.log.info("Using DefaultAzureCredential as credential.")
        return get_async_default_azure_credential(
            managed_identity_client_id=self._get_field(extras, "managed_identity_client_id"),
            workload_identity_tenant_id=self._get_field(extras, "workload_identity_tenant_id"),
        )

    @classmethod
    def _valid_batch_size(cls, value: Any, source: str) -> int:
        """Return ``value`` as a batch size, or raise naming where it came from."""
        # bool is a subclass of int: refuse it, True would silently mean 1.
        if isinstance(value, bool):
            raise ValueError(f"{source} must be an integer, got {value!r}")
        if isinstance(value, str) and value.strip().isdigit():
            value = int(value)
        if not isinstance(value, int):
            raise ValueError(f"{source} must be an integer, got {value!r}")
        if not 1 <= value <= cls.MAX_BATCH_SIZE:
            raise ValueError(f"{source} must be between 1 and {cls.MAX_BATCH_SIZE}, got {value}")
        return value

    @asynccontextmanager
    async def get_async_conn(self) -> AsyncGenerator[SearchClient, None]:
        """Yield the async ``SearchClient`` of the index, closed with its credential on exit."""
        conn = await get_async_connection(self.conn_id)
        # Masks the extra's secrets without a synchronous call to the Task SDK on the event loop.
        extras = await get_async_extra_dejson(conn)

        self._index = self.index or self._get_field(extras, "index")
        if not self._index:
            raise ValueError(
                f"No index for connection {self.conn_id!r}: pass index to the hook or set it in the connection."
            )
        host = (conn.host or "").rstrip("/")
        if not host:
            raise ValueError(f"Connection {self.conn_id!r} has no search service endpoint.")
        endpoint = host if "://" in host else f"https://{host}"

        if self.batch_size is not None:
            self._batch_size = self.batch_size
        elif (batch_size := self._get_field(extras, "batch_size")) not in (None, ""):
            self._batch_size = self._valid_batch_size(
                batch_size, f"extra['batch_size'] of connection {self.conn_id!r}"
            )
        else:
            self._batch_size = self.MAX_BATCH_SIZE

        credential = self._get_credential(conn, extras)
        try:
            async with SearchClient(endpoint, self._index, credential) as client:
                yield client
        finally:
            # The token credentials hold their own HTTP session, the key credential has none.
            if not isinstance(credential, AzureKeyCredential):
                await credential.close()

    async def search(
        self,
        search: str = "*",
        *,
        select: Iterable[str] | None = None,
        filter: str | None = None,
        order_by: Iterable[str] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Yield every document of the index matching ``search`` and ``filter``.

        :param search: Full-text query, ``*`` for every document.
        :param select: Fields to return, all retrievable fields when omitted.
        :param filter: OData filter, e.g. ``"category eq 'report'"``.
        :param order_by: OData sort expressions, e.g. ``["modified desc"]``.
        """
        async with self.get_async_conn() as client:
            results = await client.search(
                search_text=search,
                select=list(select) if select else None,
                filter=filter,
                order_by=list(order_by) if order_by else None,
            )
            async for document in results:
                yield {key: value for key, value in document.items() if not key.startswith("@search.")}

    async def count(self, *, filter: str | None = None) -> int:
        """
        Return the number of documents of the index, or of those matching ``filter``.

        :param filter: OData filter, e.g. ``"category eq 'report'"``.
        """
        async with self.get_async_conn() as client:
            if not filter:
                return await client.get_document_count()
            results = await client.search(search_text="*", filter=filter, include_total_count=True, top=0)
            return await results.get_count()

    async def get_document(self, key: str, *, select: Iterable[str] | None = None) -> dict[str, Any] | None:
        """
        Return the document with this key, or ``None`` when the index has none.

        :param key: The key of the document.
        :param select: Fields to return, all retrievable fields when omitted.
        """
        async with self.get_async_conn() as client:
            try:
                document = await client.get_document(key, selected_fields=list(select) if select else None)
            except ResourceNotFoundError:
                return None
        return dict(document)

    async def upload(self, documents: Iterable[dict[str, Any]]) -> int:
        """
        Insert documents, or replace the ones whose key exists (every field is replaced).

        :param documents: The documents to write.
        :return: The number of documents written.
        """
        return await self._write("upload_documents", documents)

    async def merge(self, documents: Iterable[dict[str, Any]]) -> int:
        """
        Update the given fields of existing documents; a document whose key is missing is rejected.

        :param documents: The key and the fields to update of each document.
        :return: The number of documents written.
        """
        return await self._write("merge_documents", documents)

    async def merge_or_upload(self, documents: Iterable[dict[str, Any]]) -> int:
        """
        Update the given fields of existing documents, or insert the document when its key is new.

        :param documents: The key and the fields to write of each document.
        :return: The number of documents written.
        """
        return await self._write("merge_or_upload_documents", documents)

    async def delete(self, documents: Iterable[dict[str, Any]]) -> int:
        """
        Delete documents.

        :param documents: The documents to delete; each needs at least its key field.
        :return: The number of documents deleted.
        """
        return await self._write("delete_documents", documents)

    async def _write(self, method: str, documents: Iterable[dict[str, Any]]) -> int:
        written = 0
        batch: list[dict[str, Any]] = []
        async with self.get_async_conn() as client:
            for document in documents:
                batch.append(document)
                if len(batch) == self._batch_size:
                    written += await self._write_batch(client, method, batch)
                    batch = []
            if batch:
                written += await self._write_batch(client, method, batch)
        return written

    async def _write_batch(self, client: SearchClient, method: str, batch: list[dict[str, Any]]) -> int:
        # The SDK returns one result per document and does not raise when the service rejected
        # some of them (HTTP 207).
        results = await getattr(client, method)(documents=batch)
        failed = [result for result in results if not result.succeeded]
        if failed:
            details = "; ".join(
                f"{result.key}: {result.status_code} {result.error_message}" for result in failed[:10]
            )
            raise AzureAISearchIndexingError(
                f"Index {self._index!r} rejected {len(failed)} of {len(batch)} documents ({details})"
            )
        return len(batch)
