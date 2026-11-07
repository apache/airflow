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
"""
Stateless ``pagination_function`` factories for ``HttpOperator``.

Each factory returns a callback ``(response) -> dict | None`` compatible with
``airflow.providers.http.operators.http.HttpOperator``. The next page is
derived only from that response (JSON body and the query string of
``response.url``). Deferrable runs rebuild the task between pages, so a counter
closed over by the callback would reset and request the same page again.

The factories do not apply a page cap. Pass ``max_pages`` to ``HttpOperator``
so the limit also covers a hand-written callback.

Offset, page, and cursor values are read from the URL query. ``HttpHook`` sends
``data`` as query parameters for GET and as a body otherwise, so these helpers
target GET listings. A cursor that exists only in a POST body needs a custom
callback.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qsl, urljoin, urlparse

from requests import Response

log = logging.getLogger(__name__)

PaginationCallback = Callable[[Response], dict[str, Any] | None]

__all__ = [
    "cursor_pagination",
    "next_link_pagination",
    "offset_pagination",
    "page_number_pagination",
]


def offset_pagination(
    *,
    offset_param: str = "offset",
    limit_param: str = "limit",
    page_size: int = 100,
    records_key: str = "",
    has_more_key: str = "",
) -> PaginationCallback:
    """
    Return a callback that advances an offset query parameter.

    The next offset is the offset on ``response.url`` plus ``page_size``.
    The callback returns ``None`` when the page has fewer than ``page_size``
    records, or when ``has_more_key`` is present on the JSON object and false.

    :param offset_param: Query parameter that carries the offset.
    :param limit_param: Query parameter that carries the page size.
    :param page_size: Number of records requested per page. The offset advances by this amount.
    :param records_key: Key of the record list when the body is an object.
        Empty when the body itself is the list.
    :param has_more_key: Optional boolean field that is false on the last page.
    """
    offset_param = _require_name("offset_param", offset_param)
    limit_param = _require_name("limit_param", limit_param)
    page_size = _require_page_size(page_size)

    def next_page(response: Response) -> dict[str, Any] | None:
        payload = response.json()
        records = _records(payload, records_key)
        if _short_page(payload, records, page_size=page_size, has_more_key=has_more_key):
            return None
        current = _int_query(response.url, offset_param, default=0)
        return {"data": {offset_param: current + page_size, limit_param: page_size}}

    return next_page


def page_number_pagination(
    *,
    page_param: str = "page",
    limit_param: str = "page_size",
    page_size: int = 100,
    records_key: str = "",
    has_more_key: str = "",
    first_page: int = 1,
) -> PaginationCallback:
    """
    Return a callback that advances a page-number query parameter.

    When ``page_param`` is absent from ``response.url``, the current page is
    ``first_page``. The callback returns ``None`` on a short page or when
    ``has_more_key`` is present and false.

    :param page_param: Query parameter that carries the page number.
    :param limit_param: Query parameter that carries the page size.
    :param page_size: Number of records requested per page.
    :param records_key: Key of the record list when the body is an object.
        Empty when the body itself is the list.
    :param has_more_key: Optional boolean field that is false on the last page.
    :param first_page: Page number used when the request URL has no page parameter.
    """
    page_param = _require_name("page_param", page_param)
    limit_param = _require_name("limit_param", limit_param)
    page_size = _require_page_size(page_size)
    first_page = _require_int("first_page", first_page)

    def next_page(response: Response) -> dict[str, Any] | None:
        payload = response.json()
        records = _records(payload, records_key)
        if _short_page(payload, records, page_size=page_size, has_more_key=has_more_key):
            return None
        current = _int_query(response.url, page_param, default=first_page)
        return {"data": {page_param: current + 1, limit_param: page_size}}

    return next_page


def cursor_pagination(
    *,
    param: str,
    field: str,
    limit_param: str = "limit",
    page_size: int = 100,
    records_key: str = "",
    has_more_key: str = "",
    numeric: bool = False,
) -> PaginationCallback:
    """
    Return a callback that sets a cursor query parameter to the max record field.

    The walk stops when the cursor does not advance, the page is short, or
    ``has_more_key`` is present and false. An empty cursor is never sent:
    ``?after=`` is how some APIs mean "no filter" and would repeat the listing.

    ``numeric=False`` compares values with ``max()`` on their original type.
    Decimal strings do not sort numerically, so ``"999"`` is greater than
    ``"1000"`` and the cursor stops moving. Set ``numeric=True`` to compare
    them as integers.

    :param param: Query parameter that carries the cursor.
    :param field: Record field the cursor is taken from.
    :param limit_param: Query parameter that carries the page size.
    :param page_size: Number of records requested per page.
    :param records_key: Key of the record list when the body is an object.
        Empty when the body itself is the list.
    :param has_more_key: Optional boolean field that is false on the last page.
    :param numeric: Compare cursor values as integers instead of as strings.
    """
    param = _require_name("param", param)
    field = _require_name("field", field)
    limit_param = _require_name("limit_param", limit_param)
    page_size = _require_page_size(page_size)
    if not isinstance(numeric, bool):
        raise ValueError("numeric must be a bool")

    def next_page(response: Response) -> dict[str, Any] | None:
        payload = response.json()
        records = _records(payload, records_key)
        if _short_page(payload, records, page_size=page_size, has_more_key=has_more_key):
            return None
        nxt = _max_field(records, field, numeric=numeric)
        if nxt is None or str(nxt) == "":
            return None
        if _query_value(response.url, param) == str(nxt):
            return None
        return {"data": {param: nxt, limit_param: page_size}}

    return next_page


def next_link_pagination(
    *,
    path: str,
    base_path: str = "",
    allow_other_hosts: bool = False,
) -> PaginationCallback:
    """
    Return a callback that follows a next-page URL stored in the JSON body.

    ``path`` is a dotted path (``page.next``). The callback returns
    ``{"endpoint": "<path>?<query>"}`` because ``HttpHook`` joins ``endpoint``
    onto the connection's base URL. An absolute next URL is reduced to that
    path and query. The request stays on the connection host either way, so
    connection credentials are not sent to another host.

    A next URL whose host differs from ``response.url`` is ignored unless
    ``allow_other_hosts`` is true. When it is true, only the path and query are
    used, still on the connection host. This matches the guard added for
    Microsoft Graph next links: a payload must not redirect a credentialed
    client at an arbitrary host.

    If the connection host already includes a path (``https://api.example.com/v2``)
    and the next URL repeats it (``/v2/feeds``), pass ``base_path="/v2"``.
    Otherwise the hook appends ``/v2`` again and calls ``/v2/v2/feeds``.

    :param path: Dotted path of the next-page URL in the JSON body.
    :param base_path: Path prefix already present on the connection host, stripped
        from the next URL so the endpoint is not doubled.
    :param allow_other_hosts: Accept a next URL on a different host and use its
        path on the connection host. The default refuses that URL.
    """
    path = _require_name("path", path)
    if not isinstance(base_path, str):
        raise ValueError("base_path must be a string")
    if not isinstance(allow_other_hosts, bool):
        raise ValueError("allow_other_hosts must be a bool")
    stripped = base_path.strip().strip("/")
    prefix = f"/{stripped}" if stripped else ""

    def next_page(response: Response) -> dict[str, Any] | None:
        nxt = _dig(response.json(), path)
        if not isinstance(nxt, str) or not nxt.strip():
            return None
        current = urlparse(response.url)
        resolved = urljoin(response.url, nxt.strip())
        if resolved == response.url:
            return None
        target = urlparse(resolved)
        if _netloc(target.netloc) != _netloc(current.netloc) and not allow_other_hosts:
            log.warning(
                "Ignoring next-page URL on a different host (%s). The response host is %s. "
                "Set allow_other_hosts=True to use that link's path on the connection host.",
                target.netloc,
                current.netloc,
            )
            return None
        endpoint = target.path or "/"
        if prefix and (endpoint == prefix or endpoint.startswith(f"{prefix}/")):
            endpoint = endpoint[len(prefix) :] or "/"
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        if target.query:
            endpoint = f"{endpoint}?{target.query}"
        return {"endpoint": endpoint}

    return next_page


def _require_name(name: str, value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _require_page_size(page_size: int) -> int:
    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1:
        raise ValueError("page_size must be a positive integer")
    return page_size


def _require_int(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _netloc(netloc: str) -> str:
    return netloc.casefold()


def _records(payload: Any, records_key: str) -> list[Any]:
    if records_key:
        rows = payload.get(records_key, []) if isinstance(payload, dict) else []
        return rows if isinstance(rows, list) else []
    return payload if isinstance(payload, list) else []


def _short_page(payload: Any, records: list[Any], *, page_size: int, has_more_key: str) -> bool:
    if has_more_key and isinstance(payload, dict) and not bool(payload.get(has_more_key, True)):
        return True
    return len(records) < page_size


def _dig(payload: Any, path: str) -> Any:
    node = payload
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _max_field(records: list[Any], field: str, *, numeric: bool) -> Any:
    values: list[Any] = []
    for item in records:
        if not isinstance(item, dict) or item.get(field) is None:
            continue
        value = item[field]
        if numeric:
            try:
                value = int(value)
            except (TypeError, ValueError):
                continue
        values.append(value)
    return max(values) if values else None


def _query_value(url: str, name: str) -> str | None:
    for key, value in parse_qsl(urlparse(url).query, keep_blank_values=True):
        if key == name:
            return value
    return None


def _int_query(url: str, name: str, *, default: int) -> int:
    raw = _query_value(url, name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        return default
