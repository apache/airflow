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

from types import SimpleNamespace
from unittest import mock

import pytest
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ResourceNotFoundError

from airflow.models import Connection
from airflow.providers.microsoft.azure.exceptions import AzureAISearchIndexingError
from airflow.providers.microsoft.azure.hooks.ai_search import AzureAISearchHook

MODULE = "airflow.providers.microsoft.azure.hooks.ai_search"
CONN_ID = "azure_ai_search_test"
HOST = "test.search.windows.net"
ENDPOINT = f"https://{HOST}"


class FakeResults:
    """Stands for the SDK's ``AsyncSearchItemPaged``: async iteration and ``get_count()``."""

    def __init__(self, documents: list[dict]) -> None:
        self.documents = documents

    def __aiter__(self):
        async def generate():
            for document in self.documents:
                yield {"@search.score": 1.0, **document}

        return generate()

    async def get_count(self) -> int:
        return len(self.documents)


class FakeSearchClient:
    """Stands for the SDK's async ``SearchClient`` and records how the hook builds and uses it."""

    def __init__(self, endpoint, index_name, credential, documents=(), reject=()) -> None:
        self.endpoint = endpoint
        self.index_name = index_name
        self.credential = credential
        self.documents = list(documents)
        self.reject = set(reject)
        self.searches: list[dict] = []
        self.writes: list[tuple[str, list[dict]]] = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True

    async def search(self, search_text=None, **kwargs):
        self.searches.append({"search_text": search_text, **kwargs})
        documents = self.documents
        if kwargs.get("filter") == "category eq 'report'":
            documents = [d for d in documents if d.get("category") == "report"]
        if kwargs.get("select"):
            documents = [{field: d.get(field) for field in kwargs["select"]} for d in documents]
        return FakeResults(documents)

    async def get_document_count(self) -> int:
        return len(self.documents)

    async def get_document(self, key, selected_fields=None):
        for document in self.documents:
            if document["id"] == key:
                return document
        raise ResourceNotFoundError("not found")

    def __getattr__(self, name):
        if not name.endswith("_documents"):
            raise AttributeError(name)

        async def write(documents):
            self.writes.append((name, list(documents)))
            return [
                SimpleNamespace(
                    key=d["id"],
                    succeeded=d["id"] not in self.reject,
                    status_code=400 if d["id"] in self.reject else 200,
                    error_message="bad field" if d["id"] in self.reject else None,
                )
                for d in documents
            ]

        return write


def docs(count: int) -> list[dict]:
    return [{"id": str(i), "title": f"title {i}", "category": None} for i in range(count)]


@pytest.fixture
def search_service():
    """Patch the connection and the SDK client; returns a namespace to set them up and inspect them."""
    service = SimpleNamespace(documents=[], reject=set(), clients=[])

    def set_connection(**kwargs):
        kwargs.setdefault("host", HOST)
        kwargs.setdefault("extra", {"index": "idx"})
        mock_get_connection.return_value = Connection(conn_id=CONN_ID, conn_type="azure_ai_search", **kwargs)

    def client(*args):
        fake = FakeSearchClient(*args, documents=service.documents, reject=service.reject)
        service.clients.append(fake)
        return fake

    service.set_connection = set_connection
    with (
        mock.patch(f"{MODULE}.get_async_connection", autospec=True) as mock_get_connection,
        mock.patch(f"{MODULE}.SearchClient", side_effect=client),
    ):
        set_connection(password="api-key")
        yield service


class TestAzureAISearchHook:
    def test_connection_form_widgets(self):
        pytest.importorskip("flask_appbuilder")
        widgets = AzureAISearchHook.get_connection_form_widgets()

        assert set(widgets) == {
            "tenantId",
            "index",
            "batch_size",
            "managed_identity_client_id",
            "workload_identity_tenant_id",
        }

    def test_ui_field_behaviour(self):
        behaviour = AzureAISearchHook.get_ui_field_behaviour()

        assert behaviour["hidden_fields"] == ["schema", "port"]
        assert behaviour["relabeling"] == {
            "host": "Search Service Endpoint",
            "login": "Azure Client ID",
            "password": "Azure Secret or API Key",
        }

    @pytest.mark.parametrize("batch_size", [0, 1001, -5, True, 2.5, "abc"])
    def test_batch_size_is_refused_when_the_hook_is_built(self, batch_size):
        with pytest.raises(ValueError, match="batch_size must be"):
            AzureAISearchHook(CONN_ID, index="idx", batch_size=batch_size)


@pytest.mark.asyncio
class TestAzureAISearchHookAsync:
    async def test_client_is_bound_to_the_index_with_an_api_key(self, search_service):
        hook = AzureAISearchHook(CONN_ID, index="my-index")

        async with hook.get_async_conn() as client:
            assert not client.closed

        assert client.endpoint == ENDPOINT
        assert client.index_name == "my-index"
        assert isinstance(client.credential, AzureKeyCredential)
        assert client.credential.key == "api-key"
        assert client.closed

    @pytest.mark.parametrize("host", [ENDPOINT, f"{ENDPOINT}/", HOST])
    async def test_endpoint_from_the_host(self, search_service, host):
        search_service.set_connection(host=host, password="api-key")

        await AzureAISearchHook(CONN_ID).count()

        assert search_service.clients[0].endpoint == ENDPOINT

    async def test_index_from_the_extra(self, search_service):
        await AzureAISearchHook(CONN_ID).count()

        assert search_service.clients[0].index_name == "idx"

    async def test_index_from_the_prefixed_extra(self, search_service):
        search_service.set_connection(password="api-key", extra={"extra__azure_ai_search__index": "legacy"})

        await AzureAISearchHook(CONN_ID).count()

        assert search_service.clients[0].index_name == "legacy"

    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    async def test_service_principal_with_login_password_and_tenant(self, mock_credential, search_service):
        search_service.set_connection(
            login="client-id", password="secret", extra={"index": "idx", "tenantId": "tenant"}
        )

        await AzureAISearchHook(CONN_ID).count()

        mock_credential.assert_called_once_with(
            tenant_id="tenant", client_id="client-id", client_secret="secret"
        )
        assert search_service.clients[0].credential is mock_credential.return_value
        mock_credential.return_value.close.assert_awaited_once()

    @pytest.mark.parametrize(
        ("password", "extra"),
        [
            pytest.param("secret", {"index": "idx"}, id="no-tenant"),
            pytest.param(None, {"index": "idx", "tenantId": "tenant"}, id="no-secret"),
        ],
    )
    async def test_incomplete_service_principal(self, search_service, password, extra):
        search_service.set_connection(login="client-id", password=password, extra=extra)

        with pytest.raises(ValueError, match="must all be provided"):
            await AzureAISearchHook(CONN_ID).count()

    async def test_password_without_client_id_is_an_api_key(self, search_service):
        search_service.set_connection(password="api-key", extra={"index": "idx", "tenantId": "tenant"})

        await AzureAISearchHook(CONN_ID).count()

        assert isinstance(search_service.clients[0].credential, AzureKeyCredential)

    @mock.patch(f"{MODULE}.get_async_default_azure_credential", autospec=True)
    async def test_default_azure_credential_without_secret(self, mock_default, search_service):
        mock_default.return_value.close = mock.AsyncMock()
        search_service.set_connection(
            extra={
                "index": "idx",
                "managed_identity_client_id": "client-id",
                "workload_identity_tenant_id": "tenant",
            }
        )

        await AzureAISearchHook(CONN_ID).count()

        mock_default.assert_called_once_with(
            managed_identity_client_id="client-id", workload_identity_tenant_id="tenant"
        )
        assert search_service.clients[0].credential is mock_default.return_value
        mock_default.return_value.close.assert_awaited_once()

    async def test_missing_index(self, search_service):
        search_service.set_connection(password="api-key", extra={})

        with pytest.raises(ValueError, match="No index"):
            await AzureAISearchHook(CONN_ID).count()

    async def test_missing_endpoint(self, search_service):
        search_service.set_connection(host="", password="api-key")

        with pytest.raises(ValueError, match="no search service endpoint"):
            await AzureAISearchHook(CONN_ID).count()

    @mock.patch(f"{MODULE}.get_async_extra_dejson", autospec=True)
    async def test_extra_is_read_through_the_async_helper(self, mock_extra, search_service):
        # Connection.extra_dejson masks through a synchronous call to the Task SDK, which is not
        # safe on the event loop of an async task.
        mock_extra.return_value = {"index": "from-helper"}

        await AzureAISearchHook(CONN_ID).count()

        mock_extra.assert_awaited_once()
        assert search_service.clients[0].index_name == "from-helper"

    async def test_search_yields_every_document_without_search_metadata(self, search_service):
        search_service.documents = docs(2500)

        found = [document async for document in AzureAISearchHook(CONN_ID).search(select=["id", "title"])]

        assert [document["id"] for document in found] == [str(i) for i in range(2500)]
        assert found[0] == {"id": "0", "title": "title 0"}
        assert search_service.clients[0].searches == [
            {"search_text": "*", "select": ["id", "title"], "filter": None, "order_by": None}
        ]
        assert search_service.clients[0].closed

    async def test_search_with_a_filter_and_an_order(self, search_service):
        search_service.documents = [{"id": "1", "category": "report"}, {"id": "2", "category": None}]

        found = [
            document
            async for document in AzureAISearchHook(CONN_ID).search(
                "annual", filter="category eq 'report'", order_by=("id desc",)
            )
        ]

        assert [document["id"] for document in found] == ["1"]
        assert search_service.clients[0].searches == [
            {
                "search_text": "annual",
                "select": None,
                "filter": "category eq 'report'",
                "order_by": ["id desc"],
            }
        ]

    async def test_count(self, search_service):
        search_service.documents = docs(42)

        assert await AzureAISearchHook(CONN_ID).count() == 42
        assert search_service.clients[0].searches == []

    async def test_count_with_a_filter(self, search_service):
        search_service.documents = [{"id": "1", "category": "report"}, {"id": "2", "category": None}]

        assert await AzureAISearchHook(CONN_ID).count(filter="category eq 'report'") == 1
        assert search_service.clients[0].searches == [
            {"search_text": "*", "filter": "category eq 'report'", "include_total_count": True, "top": 0}
        ]

    async def test_get_document(self, search_service):
        search_service.documents = docs(3)
        hook = AzureAISearchHook(CONN_ID)

        assert await hook.get_document("2") == {"id": "2", "title": "title 2", "category": None}
        assert await hook.get_document("9") is None

    @pytest.mark.parametrize(
        ("method", "sdk_method"),
        [
            ("upload", "upload_documents"),
            ("merge", "merge_documents"),
            ("merge_or_upload", "merge_or_upload_documents"),
            ("delete", "delete_documents"),
        ],
    )
    async def test_writes_in_batches_of_1000(self, search_service, method, sdk_method):
        written = await getattr(AzureAISearchHook(CONN_ID), method)(docs(2001))

        assert written == 2001
        assert len(search_service.clients) == 1
        assert [(name, len(batch)) for name, batch in search_service.clients[0].writes] == [
            (sdk_method, 1000),
            (sdk_method, 1000),
            (sdk_method, 1),
        ]

    async def test_writes_a_generator(self, search_service):
        written = await AzureAISearchHook(CONN_ID, batch_size=2).upload(document for document in docs(3))

        assert written == 3
        assert [len(batch) for _, batch in search_service.clients[0].writes] == [2, 1]

    async def test_rejected_documents_raise(self, search_service):
        search_service.reject = {"1"}

        with pytest.raises(
            AzureAISearchIndexingError,
            match=r"Index 'idx' rejected 1 of 3 documents \(1: 400 bad field\)",
        ):
            await AzureAISearchHook(CONN_ID).merge(docs(3))

    async def test_batch_size_from_the_hook(self, search_service):
        search_service.set_connection(password="api-key", extra={"index": "idx", "batch_size": 3})

        await AzureAISearchHook(CONN_ID, batch_size=2).upload(docs(5))

        assert [len(batch) for _, batch in search_service.clients[0].writes] == [2, 2, 1]

    @pytest.mark.parametrize("batch_size", [3, "3"])
    async def test_batch_size_from_the_extra(self, search_service, batch_size):
        search_service.set_connection(password="api-key", extra={"index": "idx", "batch_size": batch_size})

        await AzureAISearchHook(CONN_ID).upload(docs(5))

        assert [len(batch) for _, batch in search_service.clients[0].writes] == [3, 2]

    @pytest.mark.parametrize("batch_size", ["abc", True, 2.5, 0, 5000])
    async def test_batch_size_in_the_extra_is_validated_with_its_source(self, search_service, batch_size):
        search_service.set_connection(password="api-key", extra={"index": "idx", "batch_size": batch_size})

        with pytest.raises(ValueError, match=rf"extra\['batch_size'\] of connection '{CONN_ID}' must be"):
            await AzureAISearchHook(CONN_ID).upload(docs(1))
