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
"""HTTP client for the REST API shared by Elasticsearch and OpenSearch."""

from __future__ import annotations

import base64
import binascii
import gzip
import json
import logging
import ssl
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx2

log = logging.getLogger(__name__)

RETRY_ON_STATUS = frozenset({429, 502, 503, 504})
DEFAULT_BULK_CHUNK_SIZE = 500
DEFAULT_BULK_MAX_CHUNK_BYTES = 100 * 1024 * 1024


class SearchClientError(Exception):
    """Base class for errors raised by :class:`SearchClient`."""


class SearchConnectionError(SearchClientError):
    """No configured host could be reached."""


class SearchHTTPError(SearchClientError):
    """The cluster answered with an error status."""

    def __init__(self, status_code: int, body: Any, method: str, url: str) -> None:
        self.status_code = status_code
        self.body = body
        self.method = method
        self.url = url
        super().__init__(f"{method} {url} returned {status_code}: {body}")


class NotFoundError(SearchHTTPError):
    """The index or document does not exist (HTTP 404)."""


class BulkIndexError(SearchClientError):
    """One or more documents in a ``_bulk`` request failed."""

    def __init__(self, errors: list[dict[str, Any]]) -> None:
        self.errors = errors
        super().__init__(f"{len(errors)} document(s) failed to index")


def parse_cloud_id(cloud_id: str) -> str:
    """
    Return the Elasticsearch URL encoded in an Elastic Cloud ``cloud_id``.

    A cloud id is ``<name>:<base64("<domain>[:port]$<es uuid>$<kibana uuid>")>``.
    """
    _, _, encoded = cloud_id.rpartition(":")
    try:
        decoded = base64.b64decode(encoded.encode()).decode()
    except (binascii.Error, UnicodeDecodeError) as err:
        raise ValueError(f"Invalid cloud_id {cloud_id!r}") from err
    parts = decoded.split("$")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid cloud_id {cloud_id!r}")
    domain, _, port = parts[0].partition(":")
    return f"https://{parts[1]}.{domain}:{port or 443}"


def build_ssl_context(
    *,
    verify_certs: bool = True,
    ca_certs: str | None = None,
    client_cert: str | None = None,
    client_key: str | None = None,
) -> ssl.SSLContext:
    """Build the TLS context from the options the Elasticsearch and OpenSearch clients accept."""
    context = ssl.create_default_context(cafile=ca_certs or None)
    if client_cert:
        context.load_cert_chain(client_cert, client_key or None)
    if not verify_certs:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def _split_userinfo(url: str) -> tuple[str, tuple[str, str] | None]:
    """Remove ``user:password@`` from ``url`` and return it separately as basic-auth credentials."""
    parts = urlsplit(url)
    if parts.username is None:
        return url, None
    netloc = parts.hostname or ""
    if parts.port is not None:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit(parts._replace(netloc=netloc)), (parts.username, parts.password or "")


def normalize_host(host: str) -> str:
    """Add ``http://`` when ``host`` has no scheme and reject values that are not URLs."""
    host = host.strip()
    if urlsplit(host).scheme not in ("http", "https"):
        host = f"http://{host}"
    if not urlsplit(host).netloc:
        raise ValueError(f"{host!r} is not a valid URL.")
    return host.rstrip("/")


def _encode_index(index: str | Sequence[str]) -> str:
    names = [index] if isinstance(index, str) else list(index)
    return ",".join(quote(name, safe="*") for name in names)


def _encode_bulk_action(action: Mapping[str, Any]) -> bytes:
    """Encode one action in the ``elasticsearch.helpers.bulk`` shape as ``_bulk`` NDJSON lines."""
    action = dict(action)
    op_type = action.pop("_op_type", "index")
    source = action.pop("_source", None)
    metadata: dict[str, Any] = {key: action.pop(key) for key in ("_index", "_id") if key in action}
    if "_routing" in action:
        metadata["routing"] = action.pop("_routing")
    lines = [{op_type: metadata}]
    if op_type != "delete":
        lines.append(action if source is None else source)
    return b"".join(json.dumps(line).encode() + b"\n" for line in lines)


def _iter_bulk_chunks(
    actions: Iterable[Mapping[str, Any]], chunk_size: int, max_chunk_bytes: int
) -> Iterator[list[bytes]]:
    """Group encoded bulk actions by count and size, like ``elasticsearch.helpers.bulk``."""
    chunk: list[bytes] = []
    chunk_bytes = 0
    for action in actions:
        encoded = _encode_bulk_action(action)
        if chunk and (len(chunk) >= chunk_size or chunk_bytes + len(encoded) > max_chunk_bytes):
            yield chunk
            chunk, chunk_bytes = [], 0
        chunk.append(encoded)
        chunk_bytes += len(encoded)
    if chunk:
        yield chunk


class SearchClient:
    """
    Minimal client for the Elasticsearch and OpenSearch REST API.

    Both engines share the document, search, count and bulk endpoints, so one client serves both
    without the vendor libraries. Requests go to the first host and move on to the next one only
    when a host cannot be reached.

    :param hosts: One URL or a list of URLs. ``user:password@`` in a URL is used as basic auth.
    :param basic_auth: ``(username, password)`` for HTTP basic auth.
    :param api_key: Elasticsearch API key, either the encoded string or an ``(id, key)`` pair.
    :param headers: Extra headers sent with every request.
    :param ssl_context: TLS settings, see :func:`build_ssl_context`.
    :param request_timeout: Seconds to wait for each request.
    :param max_retries: Extra attempts on connection errors and on 429, 502, 503 and 504 responses.
    :param retry_backoff: Base delay in seconds between retries; doubles on each attempt.
    :param http_compress: Gzip request bodies.
    :param transport: Custom ``httpx2`` transport, for tests.
    """

    def __init__(
        self,
        hosts: str | Sequence[str],
        *,
        basic_auth: tuple[str, str] | None = None,
        api_key: str | tuple[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        ssl_context: ssl.SSLContext | None = None,
        request_timeout: float = 10.0,
        max_retries: int = 3,
        retry_backoff: float = 0.5,
        http_compress: bool = False,
        transport: httpx2.BaseTransport | None = None,
    ) -> None:
        host_list = [hosts] if isinstance(hosts, str) else list(hosts)
        if not host_list:
            raise ValueError("At least one host is required.")
        self.hosts: list[str] = []
        for host in host_list:
            url, userinfo = _split_userinfo(normalize_host(host))
            self.hosts.append(url)
            basic_auth = basic_auth or userinfo
        if basic_auth is not None and api_key is not None:
            raise ValueError("Set either basic_auth or api_key, not both.")

        # httpx2 also offers zstd when the zstandard package is installed, and OpenSearch 2.19 then
        # never finishes the response. Both engines support gzip.
        request_headers = {"Accept": "application/json", "Accept-Encoding": "gzip", **(headers or {})}
        if api_key is not None:
            if not isinstance(api_key, str):
                api_key = base64.b64encode(f"{api_key[0]}:{api_key[1]}".encode()).decode()
            request_headers["Authorization"] = f"ApiKey {api_key}"

        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.http_compress = http_compress
        # The Elasticsearch and OpenSearch log handlers ignored HTTP(S)_PROXY and friends; keep that so
        # task log traffic is not rerouted for deployments that move over from them.
        self._client = httpx2.Client(
            auth=httpx2.BasicAuth(*basic_auth) if basic_auth else None,
            headers=request_headers,
            verify=ssl_context or True,
            timeout=request_timeout,
            transport=transport,
            trust_env=False,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> SearchClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Any = None,
        ndjson: bytes | None = None,
    ) -> Any:
        """
        Send a request and return the decoded JSON response.

        :param method: HTTP method.
        :param path: Path below the host, e.g. ``/my-index/_search``.
        :param params: Query string parameters. Lists are joined with commas and booleans lowered.
        :param body: JSON request body.
        :param ndjson: Pre-encoded newline-delimited JSON body, for ``_bulk``.
        """
        headers: dict[str, str] = {}
        content: bytes | None = None
        if ndjson is not None:
            content = ndjson
            headers["Content-Type"] = "application/x-ndjson"
        elif body is not None:
            content = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        if content is not None and self.http_compress:
            content = gzip.compress(content)
            headers["Content-Encoding"] = "gzip"
        query = {key: _encode_param(value) for key, value in (params or {}).items() if value is not None}

        attempt = 0
        host_index = 0
        while True:
            url = self.hosts[host_index] + path
            try:
                response = self._client.request(method, url, params=query, content=content, headers=headers)
            except (httpx2.ConnectError, httpx2.ConnectTimeout) as err:
                # Nothing reached the server, so the request is safe to send to the next host.
                if attempt >= self.max_retries:
                    raise SearchConnectionError(f"{method} {url} failed: {err}") from err
                log.warning("Could not connect to %s, retrying: %s", self.hosts[host_index], err)
                host_index = (host_index + 1) % len(self.hosts)
            except httpx2.TransportError as err:
                # The server may have acted on the request, so it is not repeated.
                raise SearchConnectionError(f"{method} {url} failed: {err}") from err
            else:
                if response.status_code < 300:
                    return response.json() if response.content else None
                if response.status_code not in RETRY_ON_STATUS or attempt >= self.max_retries:
                    raise _http_error(response, method, url)
                log.warning("%s %s returned %s, retrying", method, url, response.status_code)
            time.sleep(self.retry_backoff * 2**attempt)
            attempt += 1

    def info(self) -> dict[str, Any]:
        """Return the cluster name and version (``GET /``)."""
        return self.request("GET", "/")

    def search(self, index: str | Sequence[str], body: Mapping[str, Any], **params: Any) -> dict[str, Any]:
        """Run a search (``POST /<index>/_search``); ``params`` go to the query string."""
        return self.request("POST", f"/{_encode_index(index)}/_search", params=params, body=body)

    def count(self, index: str | Sequence[str], body: Mapping[str, Any] | None = None) -> int:
        """Count matching documents (``POST /<index>/_count``)."""
        return self.request("POST", f"/{_encode_index(index)}/_count", body=body)["count"]

    def get_engine(self) -> str:
        """Return ``"opensearch"`` or ``"elasticsearch"``, from the version the cluster reports."""
        return "opensearch" if self.info()["version"].get("distribution") == "opensearch" else "elasticsearch"

    def index_document(
        self,
        index: str,
        document: Mapping[str, Any],
        *,
        doc_id: str | int | None = None,
        refresh: bool | str | None = None,
    ) -> dict[str, Any]:
        """Create or replace a document; the cluster assigns an id when ``doc_id`` is not given."""
        params = {"refresh": refresh}
        if doc_id is None:
            return self.request("POST", f"/{quote(index)}/_doc", params=params, body=document)
        path = f"/{quote(index)}/_doc/{quote(str(doc_id), safe='')}"
        return self.request("PUT", path, params=params, body=document)

    def delete_document(
        self, index: str, doc_id: str | int, *, refresh: bool | str | None = None
    ) -> dict[str, Any]:
        """Delete one document by id."""
        path = f"/{quote(index)}/_doc/{quote(str(doc_id), safe='')}"
        return self.request("DELETE", path, params={"refresh": refresh})

    def delete_by_query(self, index: str | Sequence[str], body: Mapping[str, Any]) -> dict[str, Any]:
        """Delete every document that matches the query in ``body``."""
        return self.request("POST", f"/{_encode_index(index)}/_delete_by_query", body=body)

    def create_index(self, index: str, body: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Create an index with optional ``settings``, ``mappings`` and ``aliases`` in ``body``."""
        return self.request("PUT", f"/{quote(index)}", body=body)

    def bulk(
        self,
        actions: Iterable[Mapping[str, Any]],
        *,
        chunk_size: int = DEFAULT_BULK_CHUNK_SIZE,
        max_chunk_bytes: int = DEFAULT_BULK_MAX_CHUNK_BYTES,
        refresh: bool | str | None = None,
    ) -> int:
        """
        Send actions through ``_bulk`` and return the number of successful ones.

        Each action takes the same shape as in ``elasticsearch.helpers.bulk``: ``_op_type``
        (default ``index``), ``_index``, optional ``_id`` and ``_routing``, and the document in
        ``_source`` or as the remaining keys.

        :raises BulkIndexError: after all chunks are sent, if any action failed.
        """
        succeeded = 0
        errors: list[dict[str, Any]] = []
        for chunk in _iter_bulk_chunks(actions, chunk_size, max_chunk_bytes):
            response = self.request("POST", "/_bulk", params={"refresh": refresh}, ndjson=b"".join(chunk))
            for item in response["items"]:
                result = next(iter(item.values()))
                if "error" in result or result.get("status", 200) >= 300:
                    errors.append(item)
                else:
                    succeeded += 1
        if errors:
            raise BulkIndexError(errors)
        return succeeded


def _encode_param(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value)
    return str(value)


def _http_error(response: httpx2.Response, method: str, url: str) -> SearchHTTPError:
    try:
        body = response.json()
    except ValueError:
        body = response.text
    error_class = NotFoundError if response.status_code == 404 else SearchHTTPError
    return error_class(response.status_code, body, method, url)
