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

import json
from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from functools import cached_property
from typing import Any
from urllib.parse import urlsplit

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.search.client import SearchClient, build_ssl_context, parse_cloud_id

SQL_ENDPOINTS = {"elasticsearch": "/_sql", "opensearch": "/_plugins/_sql"}


def _to_bool(value: Any) -> bool:
    return value if isinstance(value, bool) else str(value).strip().lower() in ("true", "1", "yes", "on")


class SearchSQLCursor:
    """
    DB-API style cursor over the SQL endpoint of Elasticsearch (``_sql``) or OpenSearch (``_plugins/_sql``).

    Rows are fetched ``fetch_size`` at a time; the server-side cursor is followed as rows are consumed.
    """

    arraysize = 1000

    def __init__(self, client: SearchClient, engine: str, fetch_size: int = 1000) -> None:
        self.client = client
        self.engine = engine
        self.endpoint = SQL_ENDPOINTS[engine]
        self.fetch_size = fetch_size
        self.description: list[tuple[str, str, None, None, None, None, None]] | None = None
        self.rowcount = -1
        self._rows: deque[list[Any]] = deque()
        self._cursor_id: str | None = None

    def execute(self, statement: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        self.close()
        body: dict[str, Any] = {"query": statement, "fetch_size": self.fetch_size}
        if parameters:
            if self.engine != "elasticsearch" or isinstance(parameters, Mapping):
                raise ValueError("Only Elasticsearch SQL takes parameters, and only as a sequence for '?'.")
            body["params"] = list(parameters)
        response = self._post(body)
        columns = response.get("columns") or response.get("schema") or []
        self.description = [
            (column["name"], column["type"], None, None, None, None, None) for column in columns
        ]
        self.rowcount = response.get("total", -1)
        self._load(response)

    def _post(self, body: dict[str, Any]) -> dict[str, Any]:
        params = {"format": "json"} if self.engine == "elasticsearch" else None
        return self.client.request("POST", self.endpoint, params=params, body=body)

    def _load(self, response: dict[str, Any]) -> None:
        self._rows.extend(response.get("rows") or response.get("datarows") or [])
        self._cursor_id = response.get("cursor")

    def _fetch_next_page(self) -> bool:
        if not self._cursor_id:
            return False
        self._load(self._post({"cursor": self._cursor_id}))
        return True

    def fetchone(self) -> list[Any] | None:
        while not self._rows:
            if not self._fetch_next_page():
                return None
        return self._rows.popleft()

    def fetchmany(self, size: int | None = None) -> list[list[Any]]:
        size = size or self.arraysize
        rows: list[list[Any]] = []
        while len(rows) < size and (row := self.fetchone()) is not None:
            rows.append(row)
        return rows

    def fetchall(self) -> list[list[Any]]:
        rows = list(self._rows)
        self._rows.clear()
        while self._fetch_next_page():
            rows.extend(self._rows)
            self._rows.clear()
        return rows

    def close(self) -> None:
        """Release the server-side cursor if the rows were not read to the end."""
        if self._cursor_id:
            self.client.request("POST", f"{self.endpoint}/close", body={"cursor": self._cursor_id})
        self._cursor_id = None
        self._rows.clear()


class SearchSQLConnection:
    """DB-API style connection; SQL over these endpoints is read-only, so ``commit`` does nothing."""

    def __init__(self, client: SearchClient, engine: str, fetch_size: int = 1000) -> None:
        self.client = client
        self.engine = engine
        self.fetch_size = fetch_size

    def cursor(self) -> SearchSQLCursor:
        return SearchSQLCursor(self.client, self.engine, self.fetch_size)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


class SearchHook(DbApiHook):
    """
    Interact with Elasticsearch or OpenSearch over their REST API.

    ``client`` gives the REST calls (search, documents, indices, bulk). ``get_conn`` returns a DB-API
    connection over the engine's SQL endpoint, so the hook also works with ``SQLExecuteQueryOperator``.

    The connection host may be a full URL. Otherwise the scheme comes from the ``scheme`` extra
    (default ``http``) and the port from the port field (default 9200). Extras:

    * ``api_key``: Elasticsearch API key, instead of login and password.
    * ``cloud_id``: Elastic Cloud deployment id, instead of host.
    * ``verify_certs`` (default true), ``ca_certs``, ``client_cert``, ``client_key``: TLS.
    * ``request_timeout`` (default 10), ``max_retries`` (default 3), ``http_compress``, ``headers``.
    * ``url_prefix``: path the cluster is served under, behind a proxy.
    * ``fetch_size`` (default 1000): rows per SQL page.

    :param search_conn_id: The :ref:`search connection id <howto/connection:search>`.
    :param log_query: Log queries before running them.
    """

    conn_name_attr = "search_conn_id"
    default_conn_name = "search_default"
    conn_type = "search"
    hook_name = "Elasticsearch / OpenSearch"
    supports_autocommit = False

    def __init__(self, *args: Any, log_query: bool = False, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.log_query = log_query

    @cached_property
    def client(self) -> SearchClient:
        """REST client for the connection."""
        conn = self.connection
        extra = conn.extra_dejson
        if extra.get("cloud_id"):
            host = parse_cloud_id(extra["cloud_id"])
        else:
            host = conn.host or "localhost"
            if "://" not in host:
                host = f"{extra.get('scheme', 'http')}://{host}"
            parts = urlsplit(host)
            if parts.port is None and (conn.port or "://" not in (conn.host or "")):
                parts = parts._replace(netloc=f"{parts.netloc}:{conn.port or 9200}")
            host = parts.geturl().rstrip("/") + "/" + str(extra.get("url_prefix", "")).strip("/")

        headers = extra.get("headers") or {}
        return SearchClient(
            host,
            basic_auth=(conn.login, conn.password or "") if conn.login else None,
            api_key=extra.get("api_key"),
            headers=json.loads(headers) if isinstance(headers, str) else headers,
            ssl_context=build_ssl_context(
                verify_certs=_to_bool(extra.get("verify_certs", True)),
                ca_certs=extra.get("ca_certs"),
                client_cert=extra.get("client_cert"),
                client_key=extra.get("client_key"),
            ),
            request_timeout=float(extra.get("request_timeout", 10)),
            max_retries=int(extra.get("max_retries", 3)),
            http_compress=_to_bool(extra.get("http_compress", False)),
        )

    @cached_property
    def engine(self) -> str:
        """``elasticsearch`` or ``opensearch``, as reported by the cluster."""
        return self.client.get_engine()

    def get_conn(self) -> SearchSQLConnection:
        """Return a DB-API connection over the SQL endpoint of the cluster."""
        fetch_size = int(self.connection.extra_dejson.get("fetch_size", 1000))
        return SearchSQLConnection(self.client, self.engine, fetch_size)

    def get_uri(self) -> str:
        raise NotImplementedError("Elasticsearch and OpenSearch have no SQLAlchemy dialect.")

    def search(self, query: dict[str, Any], index: str | Sequence[str], **params: Any) -> dict[str, Any]:
        """
        Run a search and return the full response.

        :param query: Search request body, e.g. ``{"query": {"match_all": {}}}``.
        :param index: Index, pattern or list of them.
        :param params: Query string parameters such as ``size`` or ``scroll``.
        """
        if self.log_query:
            self.log.info("Searching %s with query: %s", index, query)
        return self.client.search(index, query, **params)

    def count(self, query: dict[str, Any] | None, index: str | Sequence[str]) -> int:
        """Count the documents that match ``query`` (``{"query": ...}``), or all when ``None``."""
        return self.client.count(index, query)

    def index_document(
        self, document: dict[str, Any], index: str, doc_id: str | int | None = None, **params: Any
    ) -> dict[str, Any]:
        """Create or replace a document; the cluster assigns an id when ``doc_id`` is not given."""
        return self.client.index_document(index, document, doc_id=doc_id, **params)

    def delete(
        self, index: str, query: dict[str, Any] | None = None, doc_id: str | int | None = None
    ) -> dict[str, Any]:
        """Delete the document with ``doc_id``, or every document matching ``query``."""
        if query is not None:
            if self.log_query:
                self.log.info("Deleting from %s with query: %s", index, query)
            return self.client.delete_by_query(index, query)
        if doc_id is not None:
            return self.client.delete_document(index, doc_id)
        raise ValueError("Pass either query or doc_id to delete documents.")

    def create_index(self, index: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        """Create an index with optional ``settings``, ``mappings`` and ``aliases``."""
        return self.client.create_index(index, body)

    def bulk(self, actions: Iterable[Mapping[str, Any]], **kwargs: Any) -> int:
        """Send actions through ``_bulk``; see :meth:`SearchClient.bulk`."""
        return self.client.bulk(actions, **kwargs)

    def test_connection(self):
        """Check the cluster answers over REST, which does not need the SQL endpoint to be enabled."""
        try:
            info = self.client.info()
        except Exception as err:
            return False, str(err)
        return True, f"Connected to {self.engine} {info['version']['number']}"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour for the connection form."""
        return {
            "hidden_fields": ["schema"],
            "relabeling": {"host": "Host or URL", "extra": "Options"},
            "placeholders": {
                "host": "https://search.example.com",
                "port": "9200",
                "extra": json.dumps({"verify_certs": True, "api_key": "<encoded key>"}, indent=2),
            },
        }
