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

import base64
import gzip
import json
import ssl

import httpx2
import pytest

from airflow.providers.search.client import (
    BulkIndexError,
    NotFoundError,
    SearchClient,
    SearchConnectionError,
    SearchHTTPError,
    build_ssl_context,
    parse_cloud_id,
)


class Recorder:
    """Transport handler that records requests and replies with queued responses."""

    def __init__(self, *responses: httpx2.Response | Exception):
        self.responses = list(responses)
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def make_client(recorder: Recorder, hosts="http://node-a:9200", **kwargs) -> SearchClient:
    return SearchClient(hosts, transport=httpx2.MockTransport(recorder), retry_backoff=0, **kwargs)


def ndjson_lines(request: httpx2.Request) -> list[dict]:
    return [json.loads(line) for line in request.content.decode().splitlines()]


class TestParseCloudId:
    def test_decodes_es_url(self):
        encoded = base64.b64encode(b"us-east-1.aws.found.io$abc123$kibana456").decode()
        assert parse_cloud_id(f"my-deployment:{encoded}") == "https://abc123.us-east-1.aws.found.io:443"

    def test_keeps_port(self):
        encoded = base64.b64encode(b"example.com:9243$abc123$kibana456").decode()
        assert parse_cloud_id(f"name:{encoded}") == "https://abc123.example.com:9243"

    @pytest.mark.parametrize(
        "cloud_id", ["name:not-base64!", "name:" + base64.b64encode(b"only-domain").decode()]
    )
    def test_rejects_invalid(self, cloud_id):
        with pytest.raises(ValueError, match="Invalid cloud_id"):
            parse_cloud_id(cloud_id)


def test_build_ssl_context_without_verification():
    context = build_ssl_context(verify_certs=False)
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False


class TestAuth:
    def test_userinfo_in_host_becomes_basic_auth(self):
        recorder = Recorder(httpx2.Response(200, json={}))
        client = make_client(recorder, hosts="https://user:secret@node-a:9200")

        client.info()

        request = recorder.requests[0]
        assert str(request.url) == "https://node-a:9200/"
        assert request.headers["Authorization"] == "Basic " + base64.b64encode(b"user:secret").decode()

    def test_host_without_scheme_defaults_to_http(self):
        recorder = Recorder(httpx2.Response(200, json={}))
        make_client(recorder, hosts="node-a:9200").info()
        assert str(recorder.requests[0].url) == "http://node-a:9200/"

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"api_key": "encoded"}, "ApiKey encoded"),
            ({"api_key": ("id", "key")}, "ApiKey " + base64.b64encode(b"id:key").decode()),
        ],
    )
    def test_authorization_header(self, kwargs, expected):
        recorder = Recorder(httpx2.Response(200, json={}))
        make_client(recorder, **kwargs).info()
        assert recorder.requests[0].headers["Authorization"] == expected

    def test_accepts_only_gzip_encoding(self):
        recorder = Recorder(httpx2.Response(200, json={}))
        make_client(recorder).info()
        assert recorder.requests[0].headers["Accept-Encoding"] == "gzip"

    def test_rejects_more_than_one_auth(self):
        with pytest.raises(ValueError, match="either basic_auth or api_key"):
            SearchClient("http://node-a:9200", basic_auth=("u", "p"), api_key="key")


class TestRequest:
    def test_encodes_params_and_index(self):
        recorder = Recorder(httpx2.Response(200, json={"hits": {"hits": []}}))

        make_client(recorder).search(["logs-*", "other index"], {"query": {}}, track_total_hits=True, x=None)

        request = recorder.requests[0]
        assert request.url.raw_path.split(b"?")[0] == b"/logs-*,other%20index/_search"
        assert request.url.params["track_total_hits"] == "true"
        assert "x" not in request.url.params
        assert json.loads(request.content) == {"query": {}}

    def test_count_returns_number(self):
        recorder = Recorder(httpx2.Response(200, json={"count": 7}))
        assert make_client(recorder).count("logs", {"query": {"match_all": {}}}) == 7
        assert recorder.requests[0].url.path == "/logs/_count"

    def test_compresses_body(self):
        recorder = Recorder(httpx2.Response(200, json={"count": 0}))

        make_client(recorder, http_compress=True).count("logs", {"query": {}})

        request = recorder.requests[0]
        assert request.headers["Content-Encoding"] == "gzip"
        assert json.loads(gzip.decompress(request.content)) == {"query": {}}

    def test_404_raises_not_found(self):
        recorder = Recorder(httpx2.Response(404, json={"error": {"type": "index_not_found_exception"}}))

        with pytest.raises(NotFoundError) as err:
            make_client(recorder).count("missing")

        assert err.value.status_code == 404
        assert err.value.body["error"]["type"] == "index_not_found_exception"

    def test_client_error_is_not_retried(self):
        recorder = Recorder(httpx2.Response(400, text="bad query"))

        with pytest.raises(SearchHTTPError, match="bad query"):
            make_client(recorder).count("logs")

        assert len(recorder.requests) == 1

    def test_retries_unavailable_status(self):
        recorder = Recorder(
            httpx2.Response(503), httpx2.Response(429), httpx2.Response(200, json={"count": 1})
        )
        assert make_client(recorder).count("logs") == 1
        assert len(recorder.requests) == 3

    def test_gives_up_after_max_retries(self):
        recorder = Recorder(httpx2.Response(503), httpx2.Response(503))

        with pytest.raises(SearchHTTPError) as err:
            make_client(recorder, max_retries=1).count("logs")

        assert err.value.status_code == 503

    def test_connect_error_moves_to_next_host(self):
        recorder = Recorder(httpx2.ConnectError("refused"), httpx2.Response(200, json={"count": 2}))

        assert make_client(recorder, hosts=["http://node-a:9200", "http://node-b:9200"]).count("logs") == 2

        assert [r.url.host for r in recorder.requests] == ["node-a", "node-b"]

    def test_connect_error_after_max_retries(self):
        recorder = Recorder(httpx2.ConnectError("refused"), httpx2.ConnectError("refused"))

        with pytest.raises(SearchConnectionError, match="refused"):
            make_client(recorder, max_retries=1).count("logs")

    def test_read_timeout_is_not_retried(self):
        recorder = Recorder(httpx2.ReadTimeout("slow"))

        with pytest.raises(SearchConnectionError, match="slow"):
            make_client(recorder).count("logs")

        assert len(recorder.requests) == 1


class TestBulk:
    def test_sends_ndjson(self):
        recorder = Recorder(httpx2.Response(200, json={"items": [{"index": {"status": 201}}] * 3}))

        succeeded = make_client(recorder).bulk(
            [
                {"_index": "logs", "_source": {"event": "a"}},
                {"_index": "logs", "_id": "2", "_routing": "r", "event": "b"},
                {"_op_type": "delete", "_index": "logs", "_id": "3"},
            ],
            refresh="wait_for",
        )

        assert succeeded == 3
        request = recorder.requests[0]
        assert request.url.path == "/_bulk"
        assert request.url.params["refresh"] == "wait_for"
        assert request.headers["Content-Type"] == "application/x-ndjson"
        assert ndjson_lines(request) == [
            {"index": {"_index": "logs"}},
            {"event": "a"},
            {"index": {"_index": "logs", "_id": "2", "routing": "r"}},
            {"event": "b"},
            {"delete": {"_index": "logs", "_id": "3"}},
        ]

    def test_splits_chunks(self):
        recorder = Recorder(
            httpx2.Response(200, json={"items": [{"index": {"status": 201}}] * 2}),
            httpx2.Response(200, json={"items": [{"index": {"status": 201}}]}),
        )

        succeeded = make_client(recorder).bulk(
            ({"_index": "logs", "_source": {"n": n}} for n in range(3)), chunk_size=2
        )

        assert succeeded == 3
        assert [len(ndjson_lines(r)) for r in recorder.requests] == [4, 2]

    def test_raises_with_failed_items_after_all_chunks(self):
        failure = {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}}
        recorder = Recorder(
            httpx2.Response(200, json={"errors": True, "items": [failure]}),
            httpx2.Response(200, json={"items": [{"index": {"status": 201}}]}),
        )

        with pytest.raises(BulkIndexError) as err:
            make_client(recorder).bulk(
                ({"_index": "logs", "_source": {"n": n}} for n in range(2)), chunk_size=1
            )

        assert err.value.errors == [failure]
        assert len(recorder.requests) == 2
