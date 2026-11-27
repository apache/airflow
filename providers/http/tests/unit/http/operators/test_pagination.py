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
import logging
import re
from collections.abc import Callable
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import tenacity
from requests import Response

from airflow.models import Connection
from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.operators.pagination import (
    cursor_pagination,
    next_link_pagination,
    offset_pagination,
    page_number_pagination,
)
from airflow.providers.http.triggers.http import HttpResponseSerializer

BASE = "https://api.example.com"
EVENTS = ("997", "998", "999", "1000", "1001", "1002")
ORDERS = list(range(1, 8))


def _query(request) -> dict[str, list[str]]:
    return parse_qs(urlparse(request.url).query, keep_blank_values=True)


def _query_int(request, name: str, default: int) -> int:
    raw = _query(request).get(name)
    if not raw or raw[0] == "":
        return default
    return int(raw[0])


def _json_body(payload_for: Callable[..., Any]) -> Callable[..., str]:
    def _respond(request, context):
        context.status_code = 200
        context.headers["Content-Type"] = "application/json"
        return json.dumps(payload_for(request))

    return _respond


def _orders_payload(request) -> dict[str, Any]:
    offset = _query_int(request, "offset", 0)
    limit = _query_int(request, "limit", 3)
    if offset > 30:
        raise AssertionError(f"offset pagination did not advance (offset={offset})")
    window = ORDERS[offset : offset + limit]
    return {"orders": window, "has_more": offset + limit < len(ORDERS)}


def _events_payload(request) -> dict[str, Any]:
    limit = _query_int(request, "limit", 2)
    after = _query(request).get("after")
    calls = getattr(_events_payload, "calls", 0) + 1
    _events_payload.calls = calls
    if calls > 12:
        raise AssertionError("cursor pagination did not stop")
    if after is not None and after[0] == "":
        matched = list(EVENTS)
        unfiltered = True
    elif after is not None:
        cursor = int(after[0])
        matched = [event_id for event_id in EVENTS if int(event_id) >= cursor]
        unfiltered = False
    else:
        matched = list(EVENTS)
        unfiltered = False
    return {"events": [{"id": event_id} for event_id in matched[:limit]], "unfiltered": unfiltered}


def _walk(first_url: str, pager, *, limit: int = 10) -> list[Response]:
    """Follow a pagination callback the way HttpOperator merges ``data`` or ``endpoint``."""
    url = first_url
    responses: list[Response] = []
    for _ in range(limit):
        response = requests.get(url, timeout=5)
        responses.append(response)
        nxt = pager(response)
        if not nxt:
            return responses
        if "endpoint" in nxt:
            parsed = urlparse(url)
            url = f"{parsed.scheme}://{parsed.netloc}{nxt['endpoint']}"
        else:
            url = requests.Request("GET", url.split("?")[0], params=nxt["data"]).prepare().url
    raise AssertionError(f"pagination did not stop after {limit} requests")


def _distinct_ids(responses: list[Response], key: str) -> list[Any]:
    seen: list[Any] = []
    for response in responses:
        for row in response.json()[key]:
            row_id = row["id"] if isinstance(row, dict) else row
            if row_id not in seen:
                seen.append(row_id)
    return seen


def _serialized_page(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = Response()
    response.status_code = 200
    response.url = url
    response.encoding = "utf-8"
    response.headers["Content-Type"] = "application/json"
    response._content = json.dumps(payload).encode()
    return {
        "context": {},
        "event": {"status": "success", "response": HttpResponseSerializer.serialize(response)},
    }


class TestOffsetPagination:
    def test_walks_until_has_more_is_false(self, requests_mock):
        requests_mock.get(re.compile(rf"{BASE}/orders"), text=_json_body(_orders_payload))
        pager = offset_pagination(page_size=3, records_key="orders", has_more_key="has_more")
        responses = _walk(f"{BASE}/orders?offset=0&limit=3", pager)
        assert _distinct_ids(responses, "orders") == ORDERS
        assert len(responses) == 3

    def test_same_response_yields_the_same_next_page(self, requests_mock):
        requests_mock.get(f"{BASE}/orders?offset=4&limit=2", json={"orders": [5, 6]})
        response = requests.get(f"{BASE}/orders?offset=4&limit=2", timeout=5)
        first = offset_pagination(page_size=2, records_key="orders")
        second = offset_pagination(page_size=2, records_key="orders")
        assert first(response) == {"data": {"offset": 6, "limit": 2}}
        assert second(response) == first(response)

    def test_short_page_stops(self, requests_mock):
        requests_mock.get(f"{BASE}/orders?offset=0&limit=3", json={"orders": [1]})
        response = requests.get(f"{BASE}/orders?offset=0&limit=3", timeout=5)
        assert offset_pagination(page_size=3, records_key="orders")(response) is None

    def test_missing_offset_starts_at_zero(self, requests_mock):
        requests_mock.get(f"{BASE}/orders", json=[1, 2])
        response = requests.get(f"{BASE}/orders", timeout=5)
        assert offset_pagination(page_size=2)(response) == {"data": {"offset": 2, "limit": 2}}

    def test_page_size_must_be_positive(self):
        with pytest.raises(ValueError, match="page_size"):
            offset_pagination(page_size=0)


class TestPageNumberPagination:
    def test_reads_the_page_from_the_response_url(self, requests_mock):
        requests_mock.get(f"{BASE}/customers?page=3&page_size=2", json={"customers": [1, 2]})
        response = requests.get(f"{BASE}/customers?page=3&page_size=2", timeout=5)
        nxt = page_number_pagination(page_size=2, records_key="customers")(response)
        assert nxt == {"data": {"page": 4, "page_size": 2}}

    def test_missing_page_param_uses_first_page(self, requests_mock):
        requests_mock.get(f"{BASE}/customers", json=[1, 2])
        response = requests.get(f"{BASE}/customers", timeout=5)
        assert page_number_pagination(page_size=2)(response)["data"]["page"] == 2

    def test_walks_a_short_last_page(self, requests_mock):
        customers = list(range(1, 6))

        def payload(request):
            page = _query_int(request, "page", 1)
            page_size = _query_int(request, "page_size", 2)
            start = (page - 1) * page_size
            return {"customers": customers[start : start + page_size]}

        requests_mock.get(re.compile(rf"{BASE}/customers"), text=_json_body(payload))
        pager = page_number_pagination(page_size=2, records_key="customers")
        responses = _walk(f"{BASE}/customers?page=1&page_size=2", pager)
        assert _distinct_ids(responses, "customers") == customers
        assert len(responses) == 3


class TestCursorPagination:
    @pytest.mark.parametrize(("numeric", "expected"), [(True, 1000), (False, "999")])
    def test_numeric_comparison_does_not_stop_on_string_order(self, requests_mock, numeric, expected):
        # "999" > "1000" as text, so a string max never moves past the page that contains both.
        requests_mock.get(
            f"{BASE}/events",
            json={"events": [{"id": "999"}, {"id": "1000"}]},
        )
        response = requests.get(f"{BASE}/events", timeout=5)
        pager = cursor_pagination(
            param="after", field="id", page_size=2, records_key="events", numeric=numeric
        )
        assert pager(response)["data"]["after"] == expected

    def test_stops_when_the_cursor_does_not_advance(self, requests_mock):
        requests_mock.get(
            f"{BASE}/events?after=1000",
            json={"events": [{"id": "999"}, {"id": "1000"}]},
        )
        response = requests.get(f"{BASE}/events?after=1000", timeout=5)
        pager = cursor_pagination(param="after", field="id", page_size=2, records_key="events", numeric=True)
        assert pager(response) is None

    def test_never_sends_an_empty_cursor(self, requests_mock):
        requests_mock.get(f"{BASE}/events", json={"events": [{"id": ""}, {"id": ""}]})
        response = requests.get(f"{BASE}/events", timeout=5)
        pager = cursor_pagination(param="after", field="id", page_size=2, records_key="events")
        assert pager(response) is None

    @pytest.mark.parametrize(
        ("numeric", "expected_ids"),
        [
            (False, ["997", "998", "999", "1000"]),
            (True, ["997", "998", "999", "1000", "1001", "1002"]),
        ],
    )
    def test_walk_string_cursor_stops_before_1001(self, requests_mock, numeric, expected_ids):
        _events_payload.calls = 0
        requests_mock.get(re.compile(rf"{BASE}/events"), text=_json_body(_events_payload))
        pager = cursor_pagination(
            param="after", field="id", page_size=2, records_key="events", numeric=numeric
        )
        responses = _walk(f"{BASE}/events?limit=2", pager)
        assert _distinct_ids(responses, "events") == expected_ids
        if not numeric:
            assert all("1001" not in response.url and "1002" not in response.url for response in responses)


class TestNextLinkPagination:
    def test_relative_link_becomes_an_endpoint(self, requests_mock):
        requests_mock.get(f"{BASE}/feeds", json={"page": {"next": "/feeds?cursor=2"}})
        response = requests.get(f"{BASE}/feeds", timeout=5)
        assert next_link_pagination(path="page.next")(response) == {"endpoint": "/feeds?cursor=2"}

    def test_other_host_is_refused(self, requests_mock, caplog):
        requests_mock.get(f"{BASE}/feeds", json={"page": {"next": "https://evil.invalid/steal"}})
        response = requests.get(f"{BASE}/feeds", timeout=5)
        with caplog.at_level(logging.WARNING):
            assert next_link_pagination(path="page.next")(response) is None
        assert "different host" in caplog.text
        assert "evil.invalid" in caplog.text

    def test_protocol_relative_other_host_is_refused(self, requests_mock):
        requests_mock.get(f"{BASE}/feeds", json={"page": {"next": "//evil.invalid/steal"}})
        response = requests.get(f"{BASE}/feeds", timeout=5)
        assert next_link_pagination(path="page.next")(response) is None

    def test_other_host_path_is_used_when_opted_in(self, requests_mock):
        requests_mock.get(f"{BASE}/feeds", json={"page": {"next": "https://cdn.example.com/feeds?c=2"}})
        response = requests.get(f"{BASE}/feeds", timeout=5)
        pager = next_link_pagination(path="page.next", allow_other_hosts=True)
        assert pager(response) == {"endpoint": "/feeds?c=2"}

    def test_base_path_is_not_doubled(self, requests_mock):
        requests_mock.get(f"{BASE}/v2/feeds", json={"page": {"next": "/v2/feeds?cursor=2"}})
        response = requests.get(f"{BASE}/v2/feeds", timeout=5)
        pager = next_link_pagination(path="page.next", base_path="/v2")
        assert pager(response) == {"endpoint": "/feeds?cursor=2"}

    def test_base_path_does_not_strip_a_longer_segment(self, requests_mock):
        requests_mock.get(f"{BASE}/v20/feeds", json={"page": {"next": "/v20/feeds?cursor=2"}})
        response = requests.get(f"{BASE}/v20/feeds", timeout=5)
        pager = next_link_pagination(path="page.next", base_path="/v2")
        assert pager(response) == {"endpoint": "/v20/feeds?cursor=2"}

    def test_self_link_stops(self, requests_mock):
        requests_mock.get(f"{BASE}/feeds?cursor=2", json={"page": {"next": "/feeds?cursor=2"}})
        response = requests.get(f"{BASE}/feeds?cursor=2", timeout=5)
        assert next_link_pagination(path="page.next")(response) is None

    def test_null_next_link_stops(self, requests_mock):
        requests_mock.get(f"{BASE}/feeds", json={"page": {"next": None}})
        response = requests.get(f"{BASE}/feeds", timeout=5)
        assert next_link_pagination(path="page.next")(response) is None


@mock.patch.dict("os.environ", AIRFLOW_CONN_HTTP_EXAMPLE="http://www.example.com")
class TestHttpOperatorMaxPages:
    @pytest.fixture(autouse=True)
    def _connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="http_default", conn_type="http", host="test:8080/", extra='{"bearer": "test"}'
            )
        )
        create_connection_without_db(
            Connection(conn_id="http_with_base_path", conn_type="http", host="http://api.example.com/v2")
        )

    def test_sync_max_pages_stops_and_warns(self, requests_mock, caplog):
        def payload(request):
            offset = _query_int(request, "offset", 0)
            if offset > 20:
                raise AssertionError("max_pages did not stop the sync loop")
            return {"rows": [offset, offset + 1], "has_more": True}

        requests_mock.get(re.compile(r"http://www\.example\.com/rows"), text=_json_body(payload))
        operator = HttpOperator(
            task_id="capped",
            method="GET",
            endpoint="/rows",
            data={"offset": 0, "limit": 2},
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=offset_pagination(page_size=2, records_key="rows", has_more_key="has_more"),
            max_pages=2,
            response_filter=lambda responses: [
                row for response in responses for row in response.json()["rows"]
            ],
        )
        with caplog.at_level(logging.WARNING):
            result = operator.execute({})

        assert result == [0, 1, 2, 3]
        assert len(requests_mock.request_history) == 2
        assert "max_pages=2" in caplog.text

    def test_sync_does_not_warn_when_the_listing_ends_on_the_cap(self, requests_mock, caplog):
        requests_mock.get(
            re.compile(r"http://www\.example\.com/rows"),
            text=_json_body(lambda request: {"rows": [1], "has_more": False}),
        )
        operator = HttpOperator(
            task_id="exact_cap",
            method="GET",
            endpoint="/rows",
            data={"offset": 0, "limit": 2},
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=offset_pagination(page_size=2, records_key="rows", has_more_key="has_more"),
            max_pages=1,
        )
        with caplog.at_level(logging.WARNING):
            result = operator.execute({})
        assert json.loads(result[0]) == {"rows": [1], "has_more": False}
        assert "max_pages=" not in caplog.text

    def test_fresh_callback_each_page_keeps_advancing(self, requests_mock):
        """A new factory per page must read the offset from the response, not from a counter."""
        requests_mock.get(re.compile(r"http://www\.example\.com/orders"), text=_json_body(_orders_payload))

        def pagination_function(response):
            return offset_pagination(page_size=3, records_key="orders", has_more_key="has_more")(response)

        operator = HttpOperator(
            task_id="fresh",
            method="GET",
            endpoint="/orders",
            data={"offset": 0, "limit": 3},
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=pagination_function,
            response_filter=lambda responses: _distinct_ids(responses, "orders"),
        )
        assert operator.execute({}) == ORDERS
        offsets = [_query_int(request, "offset", 0) for request in requests_mock.request_history]
        assert offsets == [0, 3, 6]

    @pytest.mark.parametrize(
        ("numeric", "expected_ids"),
        [
            (False, ["997", "998", "999", "1000"]),
            (True, ["997", "998", "999", "1000", "1001", "1002"]),
        ],
    )
    def test_string_cursor_stops_early_and_numeric_cursor_does_not(
        self, requests_mock, numeric, expected_ids
    ):
        _events_payload.calls = 0
        requests_mock.get(re.compile(r"http://www\.example\.com/events"), text=_json_body(_events_payload))
        operator = HttpOperator(
            task_id="events",
            method="GET",
            endpoint="/events",
            data={"limit": 2},
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=cursor_pagination(
                param="after", field="id", page_size=2, records_key="events", numeric=numeric
            ),
            response_filter=lambda responses: _distinct_ids(responses, "events"),
        )
        assert operator.execute({}) == expected_ids

    def test_other_host_next_link_does_not_leave_the_connection(self, requests_mock, caplog):
        requests_mock.get(
            re.compile(r"http://www\.example\.com/feeds"),
            text=_json_body(
                lambda request: {"items": [{"id": "t1"}], "page": {"next": "http://evil.invalid/steal"}}
            ),
        )
        operator = HttpOperator(
            task_id="trap",
            method="GET",
            endpoint="/feeds",
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=next_link_pagination(path="page.next"),
            response_filter=lambda responses: _distinct_ids(responses, "items"),
        )
        with caplog.at_level(logging.WARNING):
            assert operator.execute({}) == ["t1"]
        assert len(requests_mock.request_history) == 1
        assert "different host" in caplog.text

    def test_connection_base_path_is_not_doubled(self, requests_mock):
        def payload(request):
            if urlparse(request.url).path != "/v2/feeds":
                raise AssertionError(f"endpoint was doubled: {request.url}")
            if _query(request).get("cursor") == ["2"]:
                return {"items": [{"id": "f3"}], "page": {"next": None}}
            return {"items": [{"id": "f1"}, {"id": "f2"}], "page": {"next": "/v2/feeds?cursor=2"}}

        requests_mock.get(re.compile(r"http://api\.example\.com/"), text=_json_body(payload))
        operator = HttpOperator(
            task_id="base_path",
            method="GET",
            endpoint="/feeds",
            http_conn_id="http_with_base_path",
            pagination_function=next_link_pagination(path="page.next", base_path="/v2"),
            response_filter=lambda responses: _distinct_ids(responses, "items"),
        )
        assert operator.execute({}) == ["f1", "f2", "f3"]
        assert [urlparse(request.url).path for request in requests_mock.request_history] == [
            "/v2/feeds",
            "/v2/feeds",
        ]

    @pytest.mark.parametrize("max_pages", [0, -1, True, 1.5, "2"])
    def test_max_pages_rejects_non_positive_integers(self, max_pages):
        with pytest.raises(ValueError, match="max_pages"):
            HttpOperator(task_id="bad_cap", max_pages=max_pages)

    @mock.patch.object(HttpHook, "run_with_advanced_retry")
    def test_max_pages_bounds_the_retry_loop(self, mock_run):
        response = Response()
        response.status_code = 200
        response._content = b"{}"
        mock_run.return_value = response

        def pagination_function(response: Response) -> dict:
            return {"endpoint": "/more"}

        operator = HttpOperator(
            task_id="retry_cap",
            method="GET",
            endpoint="/",
            http_conn_id="HTTP_EXAMPLE",
            pagination_function=pagination_function,
            max_pages=2,
            retry_args={
                "wait": tenacity.wait_none(),
                "stop": tenacity.stop_after_attempt(2),
                "retry": tenacity.retry_if_exception_type(Exception),
            },
        )
        result = operator.execute({})
        assert mock_run.call_count == 2
        assert result == ["{}", "{}"]

    def test_deferrable_max_pages_returns_every_accumulated_page(self, caplog):
        """The resume that hits the cap must return, and the assertion must see every page.

        ``execute_complete`` used to ignore the list built by ``paginate_async`` and return
        only the latest response (#72388). A test that only catches ``TaskDeferred`` never
        runs that assertion.
        """

        def pagination_function(response: Response) -> dict:
            return {"endpoint": "/page"}

        operator = HttpOperator(
            task_id="defer_cap",
            method="GET",
            endpoint="/page",
            deferrable=True,
            pagination_function=pagination_function,
            max_pages=2,
            response_filter=lambda responses: [entry.json()["n"] for entry in responses],
        )

        with caplog.at_level(logging.WARNING), pytest.raises(TaskDeferred) as first:
            operator.execute_complete(**_serialized_page("http://test:8080/page", {"n": 1}))
        assert len(first.value.kwargs["paginated_responses"]) == 1

        result = operator.execute_complete(
            **_serialized_page("http://test:8080/page", {"n": 2}),
            paginated_responses=first.value.kwargs["paginated_responses"],
        )
        assert result == [1, 2]
        assert "max_pages=2" in caplog.text

    def test_deferrable_fresh_callback_advances_from_the_response_url(self):
        """Each resume builds a new callback, so the next offset has to come from ``response.url``."""

        def pagination_function(response: Response):
            return offset_pagination(page_size=2, records_key="orders", has_more_key="has_more")(response)

        def resume(operator: HttpOperator, url: str, orders: list[int], has_more: bool, previous=None):
            kwargs = _serialized_page(url, {"orders": orders, "has_more": has_more})
            if previous is not None:
                kwargs["paginated_responses"] = previous
            try:
                result = operator.execute_complete(**kwargs)
            except TaskDeferred as deferred:
                return deferred, None
            return None, result

        first = HttpOperator(
            task_id="defer_fresh",
            method="GET",
            endpoint="/orders",
            data={"offset": 0, "limit": 2},
            deferrable=True,
            pagination_function=pagination_function,
            response_filter=lambda responses: _distinct_ids(responses, "orders"),
        )
        deferred, result = resume(first, "http://www.example.com/orders?offset=0&limit=2", [1, 2], True)
        assert result is None
        assert deferred.trigger.data["offset"] == 2
        previous = deferred.kwargs["paginated_responses"]

        # A rebuilt operator does not keep a counter from the previous attempt.
        second = HttpOperator(
            task_id="defer_fresh",
            method="GET",
            endpoint="/orders",
            data={"offset": 0, "limit": 2},
            deferrable=True,
            pagination_function=pagination_function,
            response_filter=lambda responses: _distinct_ids(responses, "orders"),
        )
        deferred, result = resume(
            second, "http://www.example.com/orders?offset=2&limit=2", [3, 4], True, previous
        )
        assert result is None
        assert deferred.trigger.data["offset"] == 4
        previous = deferred.kwargs["paginated_responses"]

        third = HttpOperator(
            task_id="defer_fresh",
            method="GET",
            endpoint="/orders",
            data={"offset": 0, "limit": 2},
            deferrable=True,
            pagination_function=pagination_function,
            response_filter=lambda responses: _distinct_ids(responses, "orders"),
        )
        deferred, result = resume(
            third, "http://www.example.com/orders?offset=4&limit=2", [5], False, previous
        )
        assert deferred is None
        assert result == [1, 2, 3, 4, 5]
