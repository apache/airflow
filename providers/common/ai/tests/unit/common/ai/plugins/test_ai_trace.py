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

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("AI Trace plugin is only compatible with Airflow >= 3.1.0", allow_module_level=True)

import datetime
import json
from typing import TYPE_CHECKING
from unittest import mock

import time_machine
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app, purge_cached_app
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.providers.common.ai.plugins.ai_trace import (
    AITracePlugin,
    _estimate_generation_cost,
    _get_map_index,
    _is_safe_segment,
    _latest_try_file,
    _normalize_store_trace,
    _otlp_attrs,
    _parse_otlp_lines,
    _store_span_node,
    _trace_id_from_context_carrier,
)
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager

BASE_URL = "http://testserver"
VALID_TRACE_ID = "1685a5e38f935f8c261c31575ee15666"


# ---------------------------------------------------------------------------
# OTLP span dict builders (a store file is newline-delimited TracesData JSON)
# ---------------------------------------------------------------------------
def _kv(k, v):
    if isinstance(v, bool):
        val = {"boolValue": v}
    elif isinstance(v, int):
        val = {"intValue": str(v)}
    else:
        val = {"stringValue": str(v)}
    return {"key": k, "value": val}


def _span(span_id, parent, name, op, *, start_ns=1_000_000_000, dur_ns=1_000_000, attrs=None, error=None):
    a = [_kv("gen_ai.operation.name", op)] + [_kv(k, v) for k, v in (attrs or {}).items()]
    s = {
        "traceId": VALID_TRACE_ID,
        "spanId": span_id,
        "name": name,
        "kind": 1,
        "startTimeUnixNano": str(start_ns),
        "endTimeUnixNano": str(start_ns + dur_ns),
        "attributes": a,
        "status": {"code": 2, "message": error} if error else {},
    }
    if parent:
        s["parentSpanId"] = parent
    return s


def _to_jsonl(spans) -> str:
    return "\n".join(
        json.dumps({"resourceSpans": [{"scopeSpans": [{"scope": {"name": "pydantic-ai"}, "spans": [s]}]}]})
        for s in spans
    )


def _agent_trace_spans():
    """A realistic tree: invoke_agent root (carries totals), 2 chats, 1 tool."""
    return [
        _span(
            "aaaa000000000001",
            None,
            "invoke_agent agent",
            "invoke_agent",
            attrs={"gen_ai.usage.input_tokens": 134, "gen_ai.usage.output_tokens": 24},
        ),
        _span(
            "aaaa000000000002",
            "aaaa000000000001",
            "chat",
            "chat",
            start_ns=1_100_000_000,
            attrs={
                "gen_ai.request.model": "claude-opus-4-8",
                "gen_ai.response.model": "claude-opus-4-8",
                "gen_ai.provider.name": "anthropic",
                "gen_ai.usage.input_tokens": 64,
                "gen_ai.usage.output_tokens": 7,
                "gen_ai.input.messages": json.dumps(
                    [{"role": "user", "parts": [{"type": "text", "content": "Roll a die"}]}]
                ),
            },
        ),
        _span(
            "aaaa000000000003",
            "aaaa000000000001",
            "execute_tool roll_dice",
            "execute_tool",
            start_ns=1_200_000_000,
            attrs={"gen_ai.tool.call.arguments": json.dumps({"sides": 6}), "gen_ai.tool.call.result": "4"},
        ),
        _span(
            "aaaa000000000004",
            "aaaa000000000001",
            "chat",
            "chat",
            start_ns=1_300_000_000,
            attrs={
                "gen_ai.request.model": "claude-opus-4-8",
                "gen_ai.response.model": "claude-opus-4-8",
                "gen_ai.provider.name": "anthropic",
                "gen_ai.usage.input_tokens": 70,
                "gen_ai.usage.output_tokens": 17,
                "gen_ai.output.messages": json.dumps(
                    [{"role": "assistant", "parts": [{"type": "text", "content": "You rolled a 4"}]}]
                ),
            },
        ),
    ]


# ---------------------------------------------------------------------------
# Security fixes (path traversal + trace-id validation) -- pure logic
# ---------------------------------------------------------------------------
class TestSafeSegment:
    @pytest.mark.parametrize(
        "seg",
        ["my_dag", "manual__2026-07-03T22:12:52.056569+00:00", "task.with.dots", "-1", "a b"],
    )
    def test_accepts_real_identifiers(self, seg):
        assert _is_safe_segment(seg) is True

    @pytest.mark.parametrize("seg", ["", ".", "..", "../etc", "a/b", "a\\b", "/etc", "a\x00b"])
    def test_rejects_traversal_and_separators(self, seg):
        assert _is_safe_segment(seg) is False


class TestLatestTryFileGuards:
    def test_traversal_cannot_reach_a_real_file_outside_root(self, tmp_path):
        from airflow.sdk import ObjectStoragePath

        root = tmp_path / "store"
        root.mkdir()
        # Plant a real jsonl at EXACTLY the path a "../" coordinate resolves to:
        # store/../outside/t/-1 == tmp_path/outside/t/-1. Without the guard,
        # _latest_try_file would iterdir that dir and return the file (escape);
        # with it, dag_id=".." is rejected and it returns None. Fails loudly if
        # the guard is removed, rather than passing on a missing directory.
        escaped = tmp_path / "outside" / "t" / "-1"
        escaped.mkdir(parents=True)
        (escaped / "1.jsonl").write_text('{"resourceSpans": []}')

        store = ObjectStoragePath(f"file://{root}")
        assert _latest_try_file(store, "..", "outside", "t", -1) is None

        # Safe control: a legitimate in-root coordinate still resolves.
        safe = root / "d" / "r" / "t" / "-1"
        safe.mkdir(parents=True)
        (safe / "3.jsonl").write_text('{"resourceSpans": []}')
        assert _latest_try_file(store, "d", "r", "t", -1).name == "3.jsonl"


# ---------------------------------------------------------------------------
# Store reader / normalizer
# ---------------------------------------------------------------------------
class TestStoreSpanNode:
    @pytest.mark.parametrize(
        ("op", "expected"),
        [
            ("chat", "GENERATION"),
            ("text_completion", "GENERATION"),
            ("generate_content", "GENERATION"),
            ("execute_tool", "TOOL"),
            ("invoke_agent", "SPAN"),
            ("something_else", "SPAN"),
        ],
    )
    def test_type_mapping(self, op, expected):
        node = _store_span_node(_span("s1", None, "n", op))
        assert node["type"] == expected

    def test_generation_inlines_message_io(self):
        node = _store_span_node(
            _span(
                "s1",
                None,
                "chat",
                "chat",
                attrs={"gen_ai.input.messages": json.dumps([{"role": "user", "parts": []}])},
            )
        )
        # Store nodes always carry input/output keys (possibly null) -- the
        # frontend uses key-presence to skip its lazy fetch.
        assert "input" in node
        assert "output" in node
        assert node["input"] == [{"role": "user", "parts": []}]

    def test_tool_inlines_arguments_and_result(self):
        node = _store_span_node(
            _span(
                "s1",
                None,
                "execute_tool x",
                "execute_tool",
                attrs={"gen_ai.tool.call.arguments": json.dumps({"a": 1}), "gen_ai.tool.call.result": "ok"},
            )
        )
        assert node["input"] == {"a": 1}
        assert node["output"] == "ok"

    def test_error_span_surfaces_level_and_message(self):
        node = _store_span_node(_span("s1", None, "chat", "chat", error="kaboom"))
        assert node["level"] == "ERROR"
        assert node["status_message"] == "kaboom"


class TestNormalizeStoreTrace:
    def test_tokens_sum_generations_only(self):
        # invoke_agent root carries totals (134/24) AND the two chats carry
        # their own (64/7 + 70/17 = 134/24). Counting the root too would double.
        trace = _normalize_store_trace(_agent_trace_spans())
        assert trace["total_tokens"] == 134 + 24

    def test_reported_shape_is_store_mode(self):
        trace = _normalize_store_trace(_agent_trace_spans())
        assert trace["langfuse_url"] is None
        assert trace["metadata"] is None
        assert trace["trace_id"] == VALID_TRACE_ID
        assert trace["observation_count"] == 4
        assert trace["model"] == "claude-opus-4-8"

    def test_conversation_built_from_first_generation(self):
        trace = _normalize_store_trace(_agent_trace_spans())
        roles = [m["role"] for m in trace["conversation"]]
        assert "user" in roles

    def test_orphan_parent_treated_as_root(self):
        # A span whose parent is not in the file must still render (as a root),
        # never silently vanish.
        spans = [_span("child", "missingparent", "chat", "chat")]
        trace = _normalize_store_trace(spans)
        assert trace["observation_count"] == 1

    def test_error_surfaced_at_trace_level(self):
        spans = [_span("s1", None, "chat", "chat", error="model failed")]
        trace = _normalize_store_trace(spans)
        assert trace["error"] == "model failed"


class TestParseOtlpLines:
    def test_blank_and_invalid_lines_skipped(self):
        text = "\n".join(["", "not json", _to_jsonl([_span("s1", None, "chat", "chat")]), "  "])
        spans = _parse_otlp_lines(text)
        assert len(spans) == 1
        assert spans[0]["spanId"] == "s1"

    def test_empty_text_yields_no_spans(self):
        assert _parse_otlp_lines("") == []


class TestOtlpAttrs:
    def test_flattens_value_types(self):
        kvs = [
            {"key": "s", "value": {"stringValue": "x"}},
            {"key": "i", "value": {"intValue": "5"}},
            {"key": "b", "value": {"boolValue": True}},
            {"key": "arr", "value": {"arrayValue": {"values": [{"intValue": "1"}]}}},
            {"key": "kv", "value": {"kvlistValue": {"values": [{"key": "n", "value": {"intValue": "2"}}]}}},
            {"no_key": True},
        ]
        out = _otlp_attrs(kvs)
        assert out == {"s": "x", "i": 5, "b": True, "arr": [1], "kv": {"n": 2}}


class TestRoundTrip:
    def test_encoder_output_parses_back(self):
        # Encoder (_otlp_json) -> file text -> reader (_parse_otlp_lines/_otlp_attrs).
        import io

        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        from airflow.providers.common.ai._otlp_json import OTLPJsonStreamExporter

        buf = io.StringIO()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(OTLPJsonStreamExporter(buf)))
        tracer = provider.get_tracer("pydantic-ai", "1.99.0")
        with tracer.start_as_current_span("chat") as s:
            s.set_attribute("gen_ai.operation.name", "chat")
            s.set_attribute("gen_ai.usage.input_tokens", 51)
        provider.shutdown()

        spans = _parse_otlp_lines(buf.getvalue())
        assert len(spans) == 1
        attrs = _otlp_attrs(spans[0]["attributes"])
        assert attrs["gen_ai.usage.input_tokens"] == 51
        assert attrs["gen_ai.operation.name"] == "chat"


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------
class TestEstimateGenerationCost:
    def test_known_model_returns_positive_float(self):
        cost = _estimate_generation_cost(
            model="claude-opus-4-8",
            provider="anthropic",
            input_tokens=1000,
            output_tokens=500,
            start_ns=1_000_000_000,
        )
        assert isinstance(cost, float)
        assert cost > 0

    def test_unknown_model_returns_none(self):
        assert (
            _estimate_generation_cost(
                model="test", provider=None, input_tokens=10, output_tokens=5, start_ns=None
            )
            is None
        )

    def test_no_tokens_returns_none(self):
        assert (
            _estimate_generation_cost(
                model="claude-opus-4-8",
                provider="anthropic",
                input_tokens=0,
                output_tokens=0,
                start_ns=None,
            )
            is None
        )

    def test_missing_library_returns_none(self):
        with mock.patch("airflow.providers.common.ai.plugins.ai_trace._calc_genai_price", None):
            assert (
                _estimate_generation_cost(
                    model="claude-opus-4-8",
                    provider="anthropic",
                    input_tokens=1000,
                    output_tokens=500,
                    start_ns=None,
                )
                is None
            )


# ---------------------------------------------------------------------------
# Small pure helpers
# ---------------------------------------------------------------------------
class TestTraceIdFromCarrier:
    def test_valid_traceparent(self):
        carrier = {"traceparent": f"00-{VALID_TRACE_ID}-0317ff83e621d28c-00"}
        assert _trace_id_from_context_carrier(carrier) == VALID_TRACE_ID

    @pytest.mark.parametrize("carrier", [None, {}, {"traceparent": "malformed"}, {"traceparent": "a-b-c"}])
    def test_invalid_returns_none(self, carrier):
        assert _trace_id_from_context_carrier(carrier) is None


class TestGetMapIndex:
    @pytest.mark.parametrize(
        ("raw", "expected"), [("0", 0), ("5", 5), ("-1", -1), ("{MAP_INDEX}", -1), ("", -1)]
    )
    def test_parsing(self, raw, expected):
        assert _get_map_index(raw) == expected


class TestPluginShape:
    def test_registers_one_fastapi_app_and_three_react_apps(self):
        assert len(AITracePlugin.fastapi_apps) == 1
        assert AITracePlugin.fastapi_apps[0]["url_prefix"] == "/ai-trace"
        destinations = {r["destination"] for r in AITracePlugin.react_apps}
        assert destinations == {"task_instance", "nav", "dag"}


# ---------------------------------------------------------------------------
# Endpoints (app + auth harness, mirrors test_hitl_review)
# ---------------------------------------------------------------------------
_AUTH_CONF = {
    (
        "core",
        "auth_manager",
    ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
    ("core", "lazy_discover_providers"): "false",
}


@pytest.fixture
def store_dir(tmp_path):
    """A file:// trace store seeded with one agent trace at a known TI path."""
    ti_dir = tmp_path / "d" / "r" / "t" / "-1"
    ti_dir.mkdir(parents=True)
    (ti_dir / "1.jsonl").write_text(_to_jsonl(_agent_trace_spans()))
    return tmp_path


@pytest.fixture
def admin_client():
    with conf_vars(_AUTH_CONF), mock.patch("airflow.settings.LAZY_LOAD_PROVIDERS", False):
        purge_cached_app()
        app = create_app()
        auth_manager: SimpleAuthManager = app.state.auth_manager
        before = datetime.datetime(2014, 1, 1)
        after = datetime.datetime.now() + datetime.timedelta(days=1)
        with time_machine.travel(before, tick=False):
            token = auth_manager._get_token_signer(
                expiration_time_in_seconds=(after - before).total_seconds()
            ).generate(auth_manager.serialize_user(SimpleAuthManagerUser(username="admin", role="admin")))
        yield TestClient(app, headers={"Authorization": f"Bearer {token}"}, base_url=BASE_URL)


@pytest.fixture
def unauthenticated_client():
    with conf_vars(_AUTH_CONF), mock.patch("airflow.settings.LAZY_LOAD_PROVIDERS", False):
        purge_cached_app()
        yield TestClient(create_app(), base_url=BASE_URL)


@pytest.fixture
def unauthorized_client():
    with conf_vars(_AUTH_CONF), mock.patch("airflow.settings.LAZY_LOAD_PROVIDERS", False):
        purge_cached_app()
        app = create_app()
        auth_manager: SimpleAuthManager = app.state.auth_manager
        token = auth_manager._get_token_signer().generate(
            auth_manager.serialize_user(SimpleAuthManagerUser(username="dummy", role=None))
        )
        yield TestClient(app, headers={"Authorization": f"Bearer {token}"}, base_url=BASE_URL)


@pytest.mark.db_test
class TestEndpointAuthorization:
    SUMMARY_PARAMS = {"dag_id": "d", "run_id": "r", "task_id": "t"}

    @pytest.mark.parametrize(
        ("method", "path", "params"),
        [
            ("get", "/ai-trace/trace-summary", SUMMARY_PARAMS),
            ("get", "/ai-trace/traces", None),
            ("get", f"/ai-trace/trace/{VALID_TRACE_ID}", None),
            ("get", "/ai-trace/observations/obs1", None),
            ("get", "/ai-trace/agents", None),
        ],
    )
    def test_401_unauthenticated(self, unauthenticated_client, method, path, params):
        resp = getattr(unauthenticated_client, method)(path, params=params)
        assert resp.status_code == 401

    @pytest.mark.parametrize(
        ("path", "params"),
        [
            ("/ai-trace/trace-summary", SUMMARY_PARAMS),
            ("/ai-trace/traces", None),
            (f"/ai-trace/trace/{VALID_TRACE_ID}", None),
            ("/ai-trace/observations/obs1", None),
            ("/ai-trace/agents", None),
        ],
    )
    def test_403_forbidden(self, unauthorized_client, path, params):
        resp = unauthorized_client.get(path, params=params)
        assert resp.status_code == 403

    def test_health_needs_no_auth(self, unauthenticated_client):
        assert unauthenticated_client.get("/ai-trace/health").status_code == 200


@pytest.mark.db_test
class TestStoreModeEndpoints:
    def test_trace_summary_reads_store_without_touching_connection(self, admin_client, store_dir):
        with (
            conf_vars({("common.ai", "trace_store_path"): f"file://{store_dir}"}),
            mock.patch(
                "airflow.providers.common.ai.plugins.ai_trace.BaseHook.get_connection", autospec=True
            ) as get_conn,
        ):
            resp = admin_client.get(
                "/ai-trace/trace-summary", params={"dag_id": "d", "run_id": "r", "task_id": "t"}
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["trace_id"] == VALID_TRACE_ID
        assert body["langfuse_url"] is None
        assert body["total_tokens"] == 158
        get_conn.assert_not_called()

    def test_trace_summary_traversal_rejected(self, admin_client, store_dir):
        with conf_vars({("common.ai", "trace_store_path"): f"file://{store_dir}"}):
            resp = admin_client.get(
                "/ai-trace/trace-summary", params={"dag_id": "..", "run_id": "..", "task_id": "etc"}
            )
        assert resp.status_code == 400

    def test_trace_summary_404_when_no_file(self, admin_client, tmp_path):
        with conf_vars({("common.ai", "trace_store_path"): f"file://{tmp_path}"}):
            resp = admin_client.get(
                "/ai-trace/trace-summary", params={"dag_id": "nope", "run_id": "x", "task_id": "y"}
            )
        assert resp.status_code == 404

    def test_observations_404_in_store_mode(self, admin_client, store_dir):
        with conf_vars({("common.ai", "trace_store_path"): f"file://{store_dir}"}):
            resp = admin_client.get("/ai-trace/observations/anything")
        assert resp.status_code == 404

    def test_trace_by_id_store_returns_200_for_authorized_dag(self, admin_client, store_dir, dag_maker):
        # The DAG must exist in the metadata DB for admin's authorized set to
        # include it (get_authorized_dag_ids intersects with real DAGs); the
        # store scan then resolves the file's owning DAG ("d") and access passes.
        with dag_maker("d", serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.sync_dagbag_to_db()
        with conf_vars({("common.ai", "trace_store_path"): f"file://{store_dir}"}):
            resp = admin_client.get(f"/ai-trace/trace/{VALID_TRACE_ID}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["trace_id"] == VALID_TRACE_ID
        assert body["airflow_ref"]["dag_id"] == "d"

    def test_trace_by_id_store_403_when_dag_not_authorized(self, admin_client, store_dir):
        # The trace resolves to DAG "d", but the user is authorized for no DAG
        # that includes it -> 403 before any span content is returned.
        restricted = mock.MagicMock(spec=BaseAuthManager)
        restricted.get_authorized_dag_ids.return_value = {"other_dag"}
        with (
            conf_vars({("common.ai", "trace_store_path"): f"file://{store_dir}"}),
            mock.patch(
                "airflow.providers.common.ai.plugins.ai_trace.get_auth_manager",
                autospec=True,
                return_value=restricted,
            ),
        ):
            resp = admin_client.get(f"/ai-trace/trace/{VALID_TRACE_ID}")
        assert resp.status_code == 403

    def test_trace_by_id_store_404_when_trace_absent(self, admin_client, store_dir):
        other_id = "ffffffffffffffffffffffffffffffff"
        with conf_vars({("common.ai", "trace_store_path"): f"file://{store_dir}"}):
            resp = admin_client.get(f"/ai-trace/trace/{other_id}")
        assert resp.status_code == 404


@pytest.mark.db_test
class TestTraceByIdValidation:
    @pytest.mark.parametrize(
        "bad", ["%", "notlongenough", "ZZZZa5e38f935f8c261c31575ee15666", VALID_TRACE_ID + "x"]
    )
    def test_non_hex_trace_id_rejected(self, admin_client, bad):
        resp = admin_client.get(f"/ai-trace/trace/{bad}")
        assert resp.status_code == 400
