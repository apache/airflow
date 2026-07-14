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
import json
import logging
import re
from datetime import datetime, timezone
from typing import Annotated, Any
from urllib.parse import urlparse

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.common.compat.sdk import conf
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS

_PLUGIN_PREFIX = "/ai-trace"
SECTION = "common.ai"
log = logging.getLogger(__name__)

# W3C trace ids are 32 lowercase-hex chars. Validated before a caller-supplied
# trace_id reaches a LIKE reverse-lookup or a file content scan.
_TRACE_ID_RE = re.compile(r"[0-9a-f]{32}")


def _get_base_url_path(path: str) -> str:
    """Construct URL path with webserver base_url prefix for non-root deployments."""
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        base_path = urlparse(base_url).path
    else:
        base_path = base_url
    base_path = base_path.rstrip("/")
    return base_path + path


def _get_bundle_url() -> str:
    """
    Return bundle URL for the React plugin.

    Uses an absolute URL when api.base_url is a full URL so the bundle loads
    correctly in Vite dev mode, where import() resolves relative to the script
    origin (5173) rather than the document origin (28080).
    """
    path = _get_base_url_path(f"{_PLUGIN_PREFIX}/static/main.umd.cjs")
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}" + path
    return path


def _trace_backend_conn_id() -> str:
    return conf.get(SECTION, "trace_backend_conn_id", fallback="langfuse_default")


if AIRFLOW_V_3_1_PLUS:
    import mimetypes
    from pathlib import Path

    import requests
    from fastapi import Depends, FastAPI, HTTPException, Query
    from fastapi.staticfiles import StaticFiles
    from sqlalchemy import String as SAString, case as sa_case, cast as sa_cast, func as sa_func, select
    from sqlalchemy.orm import Session

    from airflow.api_fastapi.app import get_auth_manager
    from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
    from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_dag
    from airflow.models.taskinstance import TaskInstance as TI
    from airflow.providers.common.compat.sdk import BaseHook, ObjectStoragePath
    from airflow.utils.session import create_session

    try:
        # Ships transitively with pydantic-ai-slim (a hard dependency of this
        # provider); guarded so a missing or API-shifted genai-prices can only
        # ever null the cost column, never break the panel.
        from genai_prices import Usage as GenAIUsage, calc_price as _calc_genai_price
    except ImportError:
        _calc_genai_price = None  # type: ignore[assignment]

    # common.ai operator classes that carry a GenAI trace worth listing. TI.operator
    # stores the concrete class name (task.task_type), so subclasses (LLMSQLQueryOperator
    # etc.) need listing explicitly -- there's no shared marker interface to filter on.
    _AGENT_OPERATOR_NAMES = (
        "AgentOperator",
        "LLMOperator",
        "LLMSQLQueryOperator",
        "LLMSchemaCompareOperator",
        "LLMBranchOperator",
        "LLMFileAnalysisOperator",
        "LlamaIndexRetrievalOperator",
        "LlamaIndexEmbeddingOperator",
        "DocumentLoaderOperator",
    )

    def _get_session():
        with create_session(scoped=False) as session:
            yield session

    SessionDep = Annotated[Session, Depends(_get_session)]

    def _get_map_index(q: str = Query("-1", alias="map_index")) -> int:
        """Parse map_index query; use -1 when placeholder unreplaced (e.g. ``{MAP_INDEX}``) or invalid."""
        try:
            return int(q)
        except (ValueError, TypeError):
            return -1

    MapIndexDep = Annotated[int, Depends(_get_map_index)]

    def _trace_id_from_context_carrier(carrier: dict[str, Any] | None) -> str | None:
        """
        Recover the W3C trace_id from a TaskInstance's stored traceparent.

        ``observability.py`` reuses the worker's live ``TracerProvider`` without
        forcing new span/trace ids, so pydantic-ai's GenAI spans nest under the
        task's own span and inherit its trace_id -- there is no separate
        correlation step (no XCom push, no ``override_ids``) needed to find
        "this task instance's trace": it's the same trace_id the task span
        itself carries in ``context_carrier``.
        """
        if not carrier:
            return None
        traceparent = carrier.get("traceparent")
        if not traceparent:
            return None
        parts = traceparent.split("-")
        if len(parts) != 4:
            return None
        return parts[1]

    def _fetch_langfuse_trace(*, host: str, token: str, trace_id: str) -> dict[str, Any]:
        resp = requests.get(
            f"{host}/api/public/traces/{trace_id}",
            headers={"Authorization": f"Basic {token}"},
            timeout=10.0,
        )
        if resp.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail="Trace not found in Langfuse yet -- ingestion can lag a few seconds after the task finishes.",
            )
        resp.raise_for_status()
        return resp.json()

    def _extract_text(messages: Any) -> str | None:
        """
        Flatten an OTel GenAI ``gen_ai.{input,output}.messages`` array to plain text.

        Verified 2026-07-02 against a live Langfuse response: Langfuse maps this
        attribute (JSON array of ``{role, parts: [{type, content}]}``) directly
        onto the observation's ``input``/``output`` field, unparsed -- so this
        reads Langfuse's field, not a raw OTel attribute.
        """
        if not isinstance(messages, list):
            return None
        chunks = []
        for message in messages:
            if not isinstance(message, dict):
                continue
            role = message.get("role", "?")
            texts = [
                str(part["content"])
                for part in message.get("parts") or []
                if isinstance(part, dict) and part.get("type") == "text" and part.get("content")
            ]
            if texts:
                chunks.append(f"{role}: " + " ".join(texts))
        return "\n".join(chunks) if chunks else None

    def _text_or_raw(value: Any) -> str | None:
        # Never silently blank content that exists: when the message array
        # carries no plain-text parts (tool-call parts, images, structured
        # output), fall back to the raw JSON instead of dropping it.
        text = _extract_text(value)
        if text is None and value is not None:
            return json.dumps(value, indent=2)
        return text

    def _messages_of(value: Any) -> list[dict[str, str]]:
        # Role-ordered conversation: one entry per message, text parts joined,
        # non-text parts kept as JSON so tool-call/structured content is never
        # silently dropped.
        out: list[dict[str, str]] = []
        if not isinstance(value, list):
            return out
        for m in value:
            if not isinstance(m, dict):
                continue
            chunks: list[str] = []
            for p in m.get("parts") or []:
                if isinstance(p, dict) and p.get("type") == "text" and p.get("content"):
                    chunks.append(str(p["content"]))
                elif p is not None:
                    chunks.append(json.dumps(p, indent=2))
            out.append({"role": str(m.get("role", "?")), "content": "\n".join(chunks)})
        return out

    def _normalize_trace(raw: dict[str, Any], *, host: str, trace_id: str) -> dict[str, Any]:
        """
        Reduce a Langfuse trace payload to the fields the panel renders.

        Field names verified 2026-07-02 against a live trace ingested through
        the real OTLP -> collector -> Langfuse path: ``observations[].type`` is
        one of ``GENERATION`` (``model``/``usage.total`` present directly) or
        ``TOOL`` (Langfuse's own classification of an ``execute_tool *`` OTel
        GenAI span, not ``SPAN`` as originally guessed before this was checked
        live). ``input``/``output`` on either type are populated ONLY when the
        run had ``[common.ai] capture_content = True`` -- same gate as the rest
        of this provider's content-capture story, not a separate opt-in here.
        """
        observations = raw.get("observations") or []
        generations = [o for o in observations if o.get("type") == "GENERATION"]
        total_tokens = sum((g.get("usage") or {}).get("total") or 0 for g in generations)
        # Failure surfaced at the top of the card, not buried in a tree node.
        first_error = next(
            (
                o.get("statusMessage") or "ERROR"
                for o in observations
                if o.get("level") == "ERROR" or o.get("statusMessage")
            ),
            None,
        )
        model = next((g.get("model") for g in generations if g.get("model")), None)
        generation = generations[0] if generations else None

        conversation = (
            _messages_of(generation.get("input")) + _messages_of(generation.get("output"))
            if generation
            else []
        )

        return {
            "trace_id": trace_id,
            "timestamp": raw.get("timestamp"),
            "latency": raw.get("latency"),
            "cost": raw.get("totalCost"),
            "model": model,
            "total_tokens": total_tokens or None,
            "prompt": _text_or_raw(generation.get("input")) if generation else None,
            "completion": _text_or_raw(generation.get("output")) if generation else None,
            "conversation": conversation,
            "observation_count": len(observations),
            "error": first_error,
            "metadata": raw.get("metadata"),
            # IO-stripped observation tree for the modal: the detail payload
            # carries structure + per-node summary numbers only; input/output
            # (the heavy chat-message arrays and tool payloads) are lazy-fetched
            # per node on expand via /observations/{id}.
            "observations": [
                {
                    "id": o.get("id"),
                    "parent_observation_id": o.get("parentObservationId"),
                    "type": o.get("type"),
                    "name": o.get("name"),
                    "start_time": o.get("startTime"),
                    "latency": o.get("latency"),
                    "model": o.get("model"),
                    "input_tokens": (o.get("usage") or {}).get("input"),
                    "output_tokens": (o.get("usage") or {}).get("output"),
                    "total_tokens": (o.get("usage") or {}).get("total"),
                    "cost": o.get("calculatedTotalCost") or o.get("totalCost"),
                    "level": o.get("level"),
                    "status_message": o.get("statusMessage"),
                }
                for o in observations
            ],
            "langfuse_url": f"{host}/trace/{trace_id}",
        }

    def _fetch_langfuse_trace_list(
        *, host: str, token: str, from_timestamp: str | None, limit: int
    ) -> dict[str, dict[str, Any]]:
        """
        One batched Langfuse list call to enrich many rows, not one GET per row.

        Langfuse's public API has no "give me exactly these trace ids" filter,
        so this pulls the most recent traces since ``from_timestamp`` (the
        oldest start_date among the Airflow rows being enriched) and matches by
        id client-side. Best-effort: a trace outside this window, or not yet
        ingested, just won't be in the returned map.
        """
        params: dict[str, Any] = {"limit": limit}
        if from_timestamp:
            params["fromTimestamp"] = from_timestamp
        resp = requests.get(
            f"{host}/api/public/traces",
            headers={"Authorization": f"Basic {token}"},
            params=params,
            timeout=10.0,
        )
        resp.raise_for_status()
        return {row["id"]: row for row in resp.json().get("data", [])}

    def _summarize_list_row(raw: dict[str, Any] | None) -> dict[str, Any]:
        """Reduce one Langfuse list-row to the fields the list view renders."""
        if raw is None:
            return {
                "model": None,
                "total_tokens": None,
                "input_tokens": None,
                "output_tokens": None,
                "cost": None,
                "latency": None,
                "input_preview": None,
            }
        attrs = (raw.get("metadata") or {}).get("attributes") or {}
        input_tokens = attrs.get("gen_ai.usage.input_tokens")
        output_tokens = attrs.get("gen_ai.usage.output_tokens")
        total_tokens = None
        if input_tokens is not None or output_tokens is not None:
            total_tokens = int(input_tokens or 0) + int(output_tokens or 0)
        # Trace-level input is the OTel message array when capture_content was
        # on for the run, absent otherwise -- flatten to a short plain-text
        # preview for the list row (full text lives in the detail modal).
        preview = _extract_text(raw.get("input"))
        if preview and len(preview) > 200:
            preview = preview[:200] + "…"
        return {
            "model": attrs.get("gen_ai.response.model"),
            "total_tokens": total_tokens,
            "input_tokens": int(input_tokens) if input_tokens is not None else None,
            "output_tokens": int(output_tokens) if output_tokens is not None else None,
            "cost": raw.get("totalCost"),
            "latency": raw.get("latency"),
            "input_preview": preview,
        }

    # ---- ObjectStorage trace store (backend-free mode) -------------------
    # When [common.ai] trace_store_path is set, the emit side (observability.py)
    # writes each task try's GenAI spans as OTLP JSON lines under
    # {store}/{dag_id}/{run_id}/{task_id}/{map_index}/{try_number}.jsonl.
    # The readers below parse those files back into the exact same response
    # shapes the Langfuse path produces, so the frontend needs no mode switch
    # beyond a nullable external link. Store mode takes precedence over the
    # Langfuse connection and needs neither a backend nor core tracing.

    def _trace_store_path() -> str:
        return conf.get(SECTION, "trace_store_path", fallback="")

    def _is_safe_segment(seg: str) -> bool:
        """
        Report whether ``seg`` is a single, contained path component.

        ObjectStoragePath does NOT normalize ``..`` and treats a leading ``/``
        as absolute (verified: ``store / ".." / "x"`` escapes the root and
        ``store / "/etc"`` resolves to ``file:///etc``), so a task-instance
        coordinate that reaches a path join must be rejected first -- otherwise
        an attacker-controlled ``dag_id``/``run_id``/``task_id`` reads any file
        the API server can. Allow only what real Airflow ids contain; reject
        empties, separators, ``.``/``..``, and NUL.
        """
        return bool(seg) and seg not in (".", "..") and not (set(seg) & {"/", "\\", "\x00"})

    def _reject_unsafe_ti_coords(dag_id: str, run_id: str, task_id: str) -> None:
        if not all(_is_safe_segment(s) for s in (dag_id, run_id, task_id)):
            raise HTTPException(status_code=400, detail="Invalid task instance identifier.")

    def _otlp_value(value: dict[str, Any]) -> Any:
        """Decode one OTLP JSON ``AnyValue`` (``{"stringValue": ...}`` etc.) to a plain value."""
        if "stringValue" in value:
            return value["stringValue"]
        if "intValue" in value:
            # The OTLP JSON encoding carries int64 as a decimal string.
            return int(value["intValue"])
        if "doubleValue" in value:
            return value["doubleValue"]
        if "boolValue" in value:
            return value["boolValue"]
        if "arrayValue" in value:
            return [_otlp_value(v) for v in value["arrayValue"].get("values") or []]
        if "kvlistValue" in value:
            return _otlp_attrs(value["kvlistValue"].get("values"))
        return value.get("bytesValue")

    def _otlp_attrs(kvs: list[dict[str, Any]] | None) -> dict[str, Any]:
        return {kv["key"]: _otlp_value(kv.get("value") or {}) for kv in kvs or [] if "key" in kv}

    def _parse_otlp_lines(text: str) -> list[dict[str, Any]]:
        """Flatten OTLP JSON lines (one ``TracesData`` per line) to a list of raw spans."""
        spans: list[dict[str, Any]] = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except ValueError:
                continue
            for resource_spans in data.get("resourceSpans") or []:
                for scope_spans in resource_spans.get("scopeSpans") or []:
                    spans.extend(scope_spans.get("spans") or [])
        return spans

    def _ns_to_iso(ns: Any) -> str | None:
        try:
            return datetime.fromtimestamp(int(ns) / 1e9, tz=timezone.utc).isoformat()
        except (TypeError, ValueError):
            return None

    def _json_maybe(value: Any) -> Any:
        # gen_ai.* content attributes (messages, tool arguments/results) are
        # JSON-encoded strings on the wire; decode so the UI renders structure,
        # but pass through verbatim when a producer wrote plain text.
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                return value
        return value

    _GENERATION_OPS = frozenset({"chat", "text_completion", "generate_content"})

    def _estimate_generation_cost(
        *, model: Any, provider: Any, input_tokens: Any, output_tokens: Any, start_ns: Any
    ) -> float | None:
        """
        Best-effort USD estimate for one generation via genai-prices.

        Store mode has no ingest step to price tokens the way Langfuse does,
        so the read side estimates at render time instead: always current with
        the library's bundled price data (no runtime price fetch), retroactive
        for files recorded before that data existed, and never persisted --
        the span files stay pure OTLP. The span timestamp keys historic
        pricing, so replayed old files price at the rates of their day.
        These are estimates, not billing data; unknown or self-hosted models
        return None and render as an em dash.
        """
        if _calc_genai_price is None or not model or not (input_tokens or output_tokens):
            return None
        try:
            when = datetime.fromtimestamp(int(start_ns) / 1e9, tz=timezone.utc) if start_ns else None
        except (TypeError, ValueError):
            when = None
        try:
            price = _calc_genai_price(
                GenAIUsage(input_tokens=int(input_tokens or 0), output_tokens=int(output_tokens or 0)),
                model_ref=str(model),
                provider_id=str(provider) if provider else None,
                genai_request_timestamp=when,
            )
        except Exception:
            # LookupError for unknown models is the expected common case;
            # anything else (upstream API drift) equally must not break the
            # panel -- cost is an annotation, never load-bearing.
            return None
        return float(price.total_price)

    def _store_span_node(span: dict[str, Any]) -> dict[str, Any]:
        """
        Map one raw OTLP span to the observation-node shape the modal tree renders.

        Type comes from ``gen_ai.operation.name`` (the OTel GenAI semconv field
        pydantic-ai emits): a model call is GENERATION, ``execute_tool`` is
        TOOL, anything else (agent run wrapper span) is SPAN -- mirroring how
        Langfuse classifies the same spans at ingest. Unlike the Langfuse path,
        ``input``/``output`` are INLINE on the node: the whole file was just
        read anyway, so there is nothing to lazy-fetch. Keys are always present
        (null when a span has no IO) -- the frontend uses key-presence to skip
        its lazy /observations fetch in store mode.
        """
        attrs = _otlp_attrs(span.get("attributes"))
        op = attrs.get("gen_ai.operation.name")
        obs_type = "GENERATION" if op in _GENERATION_OPS else "TOOL" if op == "execute_tool" else "SPAN"
        status = span.get("status") or {}
        is_error = status.get("code") in (2, "2", "STATUS_CODE_ERROR")
        try:
            latency = (int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"])) / 1e9
        except (KeyError, TypeError, ValueError):
            latency = None
        input_tokens = attrs.get("gen_ai.usage.input_tokens")
        output_tokens = attrs.get("gen_ai.usage.output_tokens")
        model = attrs.get("gen_ai.response.model") or attrs.get("gen_ai.request.model")
        if obs_type == "GENERATION":
            io_in = _json_maybe(attrs.get("gen_ai.input.messages"))
            io_out = _json_maybe(attrs.get("gen_ai.output.messages"))
            cost = _estimate_generation_cost(
                model=model,
                provider=attrs.get("gen_ai.provider.name") or attrs.get("gen_ai.system"),
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                start_ns=span.get("startTimeUnixNano"),
            )
        elif obs_type == "TOOL":
            io_in = _json_maybe(attrs.get("gen_ai.tool.call.arguments"))
            io_out = _json_maybe(attrs.get("gen_ai.tool.call.result"))
            cost = None
        else:
            io_in = io_out = None
            cost = None
        return {
            "id": span.get("spanId"),
            "parent_observation_id": span.get("parentSpanId") or None,
            "type": obs_type,
            "name": span.get("name"),
            "start_time": _ns_to_iso(span.get("startTimeUnixNano")),
            "latency": latency,
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": ((input_tokens or 0) + (output_tokens or 0)) or None,
            "cost": cost,
            "level": "ERROR" if is_error else None,
            "status_message": (status.get("message") or "ERROR") if is_error else None,
            "input": io_in,
            "output": io_out,
        }

    def _normalize_store_trace(raw_spans: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Build the trace-detail payload (same shape as ``_normalize_trace``) from raw spans.

        ``metadata`` is null (there is no ingest step to stamp it) and
        ``langfuse_url`` is null -- the frontend hides the external link.
        ``cost`` is a read-time genai-prices estimate summed over generations
        (None when every model is unknown). With core tracing off each agent
        run roots its own
        trace id, so one task try's file can legitimately hold several trace
        ids; the file (one task try) is the trace unit here, and the reported
        trace_id is the first root span's.
        """
        nodes = sorted((_store_span_node(s) for s in raw_spans), key=lambda n: n["start_time"] or "")
        generations = [n for n in nodes if n["type"] == "GENERATION"]
        total_tokens = sum(n["total_tokens"] or 0 for n in generations)
        model = next((n["model"] for n in generations if n["model"]), None)
        generation = generations[0] if generations else None
        conversation = (
            _messages_of(generation["input"]) + _messages_of(generation["output"]) if generation else []
        )
        first_error = next((n["status_message"] or "ERROR" for n in nodes if n["level"] == "ERROR"), None)
        starts = [int(s["startTimeUnixNano"]) for s in raw_spans if s.get("startTimeUnixNano")]
        ends = [int(s["endTimeUnixNano"]) for s in raw_spans if s.get("endTimeUnixNano")]
        latency = (max(ends) - min(starts)) / 1e9 if starts and ends else None
        node_ids = {n["id"] for n in nodes}
        trace_id = next(
            (
                s.get("traceId")
                for s in raw_spans
                if not s.get("parentSpanId") or s.get("parentSpanId") not in node_ids
            ),
            raw_spans[0].get("traceId") if raw_spans else None,
        )
        return {
            "trace_id": trace_id,
            "timestamp": _ns_to_iso(min(starts)) if starts else None,
            "latency": latency,
            "cost": sum(n["cost"] for n in generations if n["cost"]) or None,
            "model": model,
            "total_tokens": total_tokens or None,
            "prompt": _text_or_raw(generation["input"]) if generation else None,
            "completion": _text_or_raw(generation["output"]) if generation else None,
            "conversation": conversation,
            "observation_count": len(nodes),
            "error": first_error,
            "metadata": None,
            "observations": nodes,
            "langfuse_url": None,
        }

    def _latest_try_file(store: Any, dag_id: str, run_id: str, task_id: str, map_index: int) -> Any | None:
        """Return the highest-try ``{try}.jsonl`` for one TI, or None if nothing was stored."""
        # Defense in depth: endpoints reject unsafe query params up front, but
        # this also guards DB-sourced coordinates (list enrichment) so a single
        # poisoned row can never escape the store root via a path join.
        if not all(_is_safe_segment(s) for s in (dag_id, run_id, task_id)):
            return None
        ti_dir = store / dag_id / run_id / task_id / str(map_index)
        try:
            files = [p for p in ti_dir.iterdir() if p.name.endswith(".jsonl")]
        except (FileNotFoundError, NotADirectoryError, OSError):
            return None

        def try_number(p: Any) -> int:
            try:
                return int(p.stem)
            except ValueError:
                return -1

        return max(files, key=try_number, default=None)

    # (path, mtime)-keyed list-enrichment cache: span files are append-only per
    # try, so an unchanged mtime means the parsed summary is still valid.
    _STORE_SUMMARY_CACHE: dict[tuple[str, float], dict[str, Any]] = {}

    def _store_row_summary(
        store: Any, dag_id: str, run_id: str, task_id: str, map_index: int
    ) -> dict[str, Any] | None:
        """List-row enrichment from the store; None when the TI has no stored spans."""
        path = _latest_try_file(store, dag_id, run_id, task_id, map_index)
        if path is None:
            return None
        try:
            mtime = float(path.stat().st_mtime)
        except (OSError, TypeError, ValueError):
            mtime = 0.0
        key = (str(path), mtime)
        cached = _STORE_SUMMARY_CACHE.get(key)
        if cached is not None:
            return dict(cached)
        spans = _parse_otlp_lines(path.read_text())
        if not spans:
            return None
        trace = _normalize_store_trace(spans)
        generations = [n for n in trace["observations"] if n["type"] == "GENERATION"]
        preview = trace["prompt"]
        if preview and len(preview) > 200:
            preview = preview[:200] + "…"
        summary = {
            "trace_id": trace["trace_id"],
            "model": trace["model"],
            "total_tokens": trace["total_tokens"],
            "input_tokens": sum(n["input_tokens"] or 0 for n in generations) or None,
            "output_tokens": sum(n["output_tokens"] or 0 for n in generations) or None,
            "cost": trace["cost"],
            "latency": trace["latency"],
            "input_preview": preview,
        }
        if len(_STORE_SUMMARY_CACHE) > 512:
            _STORE_SUMMARY_CACHE.clear()
        _STORE_SUMMARY_CACHE[key] = summary
        return dict(summary)

    def _store_find_trace_files(store: Any, trace_id: str) -> list[Any]:
        """
        Full-store scan for span files containing ``trace_id``.

        Fallback for /trace/{id} when the context_carrier reverse-lookup finds
        nothing (pure store mode runs with core tracing off, so TIs carry no
        traceparent). Walks the fixed dag/run/task/map_index layout and
        substring-checks each latest-try file -- O(store) reads, which is the
        honest cost of a bare-id lookup without an index; fine at the local-dev
        scale this mode targets.
        """

        def subdirs(p: Any) -> list[Any]:
            try:
                return [c for c in p.iterdir() if c.is_dir()]
            except (FileNotFoundError, NotADirectoryError, OSError):
                return []

        matches = []
        for dag_dir in subdirs(store):
            for run_dir in subdirs(dag_dir):
                for task_dir in subdirs(run_dir):
                    for mi_dir in subdirs(task_dir):
                        latest = max(
                            (p for p in mi_dir.iterdir() if p.name.endswith(".jsonl")),
                            key=lambda p: int(p.stem) if p.stem.isdigit() else -1,
                            default=None,
                        )
                        if latest is not None and trace_id in latest.read_text():
                            matches.append(latest)
        return matches

    def _airflow_ref_from_path(path: Any) -> dict[str, Any]:
        """Recover TI coordinates from a span file's {dag}/{run}/{task}/{map_index}/{try}.jsonl path."""
        mi_dir = path.parent
        task_dir = mi_dir.parent
        run_dir = task_dir.parent
        try:
            map_index = int(mi_dir.name)
        except ValueError:
            map_index = -1
        return {
            "dag_id": run_dir.parent.name,
            "run_id": run_dir.name,
            "task_id": task_dir.name,
            "map_index": map_index,
        }

    def _require_dag_access(user: Any, dag_id: str) -> None:
        """
        Raise 403 unless ``user`` may read ``dag_id``.

        The bare-id endpoints resolve a trace/observation to its owning task
        instance and then gate on access to *that* DAG -- so a user authorized
        for one DAG cannot read another DAG's captured prompts, completions, or
        tool IO. Uses the same ``get_authorized_dag_ids`` mechanism the list
        endpoints scope with (the module-level ``get_auth_manager()`` singleton,
        since ``request.app.state`` is not populated in a mounted sub-app).
        """
        if dag_id not in get_auth_manager().get_authorized_dag_ids(user=user, method="GET"):
            raise HTTPException(status_code=403, detail="Not authorized to read this trace's DAG.")

    ai_trace_app = FastAPI(
        title="AI Trace",
        description=(
            "Read-side panel resolving a task instance's GenAI trace via the "
            "configured trace backend (Langfuse today) and linking out to it."
        ),
    )

    @ai_trace_app.get("/health")
    async def health() -> dict[str, str]:
        """Liveness check."""
        return {"status": "ok"}

    @ai_trace_app.get(
        "/trace-summary",
        dependencies=[
            Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))
        ],
    )
    def trace_summary(
        db: SessionDep,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: MapIndexDep,
    ) -> dict[str, Any]:
        """Resolve this task instance's trace_id and return a normalized summary."""
        _reject_unsafe_ti_coords(dag_id, run_id, task_id)
        store = _trace_store_path()
        if store:
            # The store layout is keyed by TI coordinates, so the path IS the
            # correlation -- no context_carrier (and hence no core tracing)
            # required, and no trace backend touched.
            path = _latest_try_file(ObjectStoragePath(store), dag_id, run_id, task_id, map_index)
            spans = _parse_otlp_lines(path.read_text()) if path is not None else []
            if not spans:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        "No stored trace for this task instance yet. The trace store only has "
                        "spans after an instrumented agent has run (and, on object stores, "
                        "after the task process exits)."
                    ),
                )
            return _normalize_store_trace(spans)

        carrier = db.scalar(
            select(TI.context_carrier).where(
                TI.dag_id == dag_id,
                TI.run_id == run_id,
                TI.task_id == task_id,
                TI.map_index == map_index,
            )
        )
        trace_id = _trace_id_from_context_carrier(carrier)
        if trace_id is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    "No trace context on this task instance yet. Core tracing "
                    "([traces] otel_on) may be off, or the task hasn't started."
                ),
            )

        conn_id = _trace_backend_conn_id()
        conn = BaseHook.get_connection(conn_id)
        host = (conn.host or "").rstrip("/")
        if not host:
            raise HTTPException(status_code=500, detail=f"Connection {conn_id!r} has no host set.")
        token = base64.b64encode(f"{conn.login}:{conn.password}".encode()).decode()

        raw = _fetch_langfuse_trace(host=host, token=token, trace_id=trace_id)
        return _normalize_trace(raw, host=host, trace_id=trace_id)

    @ai_trace_app.get(
        "/trace/{trace_id}",
        dependencies=[Depends(requires_access_dag(method="GET"))],
    )
    def trace_by_id(db: SessionDep, user: GetUserDep, trace_id: str) -> dict[str, Any]:
        """
        Direct trace-by-id lookup, bypassing the task-instance correlation.

        The trace is resolved to its owning task instance first, and access is
        then gated on *that* DAG (``_require_dag_access``): a user authorized
        for one DAG cannot read another's captured prompts/completions/tool IO.
        A trace that resolves to no task instance returns 404 -- there is no DAG
        to authorize against, so it is treated as not found rather than served
        under the any-DAG floor.

        ``trace_id`` is validated as a 32-char lowercase-hex W3C trace id
        before use: it flows into a ``LIKE %{trace_id}%`` reverse-lookup (a
        bare ``%`` would otherwise match an arbitrary TI) and into a substring
        content scan of stored files.
        """
        if not _TRACE_ID_RE.fullmatch(trace_id):
            raise HTTPException(status_code=400, detail="Invalid trace id (expected 32 hex characters).")
        store = _trace_store_path()
        if store:
            root = ObjectStoragePath(store)
            # Cheap path first: when core tracing was on, the TI's traceparent
            # points straight at the file; otherwise fall back to scanning.
            row = db.execute(
                select(TI.dag_id, TI.run_id, TI.task_id, TI.map_index)
                .where(sa_cast(TI.context_carrier, SAString).like(f"%{trace_id}%"))
                .limit(1)
            ).first()
            files = []
            if row:
                f = _latest_try_file(root, row.dag_id, row.run_id, row.task_id, row.map_index)
                files = [f] if f is not None else []
            if not files:
                files = _store_find_trace_files(root, trace_id)
            if not files:
                raise HTTPException(status_code=404, detail="No stored trace with this id.")
            ref = (
                {
                    "dag_id": row.dag_id,
                    "run_id": row.run_id,
                    "task_id": row.task_id,
                    "map_index": row.map_index,
                }
                if row
                else _airflow_ref_from_path(files[0])
            )
            # Authorize the owning DAG BEFORE reading any span content.
            _require_dag_access(user, ref["dag_id"])
            # A single task-try file can hold several trace ids (one per agent
            # run when core tracing is off), so keep ONLY the requested trace's
            # spans -- normalizing the whole file together would mix an
            # unrelated agent's prompt/model into this response and expose every
            # root in one tree.
            spans = [
                span
                for f in files
                for span in _parse_otlp_lines(f.read_text())
                if span.get("traceId") == trace_id
            ]
            if not spans:
                raise HTTPException(status_code=404, detail="No stored trace with this id.")
            out = _normalize_store_trace(spans)
            out["trace_id"] = trace_id
            out["airflow_ref"] = ref
            return out

        # Langfuse mode: resolve the owning TI FIRST and authorize its DAG
        # before fetching any content from the backend. The trace_id is embedded
        # in the stored traceparent, so a substring match on the serialized
        # context_carrier finds it exactly (W3C trace ids are 32-hex and
        # collision-free in practice). POC-grade; an indexed column would
        # replace this LIKE if the plugin ever grows up.
        row = db.execute(
            select(TI.dag_id, TI.run_id, TI.task_id, TI.map_index)
            .where(sa_cast(TI.context_carrier, SAString).like(f"%{trace_id}%"))
            .limit(1)
        ).first()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail="No task instance owns this trace id, so it cannot be authorized.",
            )
        _require_dag_access(user, row.dag_id)

        conn_id = _trace_backend_conn_id()
        conn = BaseHook.get_connection(conn_id)
        host = (conn.host or "").rstrip("/")
        if not host:
            raise HTTPException(status_code=500, detail=f"Connection {conn_id!r} has no host set.")
        token = base64.b64encode(f"{conn.login}:{conn.password}".encode()).decode()
        raw = _fetch_langfuse_trace(host=host, token=token, trace_id=trace_id)
        out = _normalize_trace(raw, host=host, trace_id=trace_id)
        out["airflow_ref"] = {
            "dag_id": row.dag_id,
            "run_id": row.run_id,
            "task_id": row.task_id,
            "map_index": row.map_index,
        }
        return out

    @ai_trace_app.get(
        "/observations/{obs_id}",
        dependencies=[Depends(requires_access_dag(method="GET"))],
    )
    def observation_io(db: SessionDep, user: GetUserDep, obs_id: str) -> dict[str, Any]:
        """
        Lazy-fetch a single observation's input/output for the modal tree.

        The trace-detail payload deliberately strips per-observation IO (full
        chat-message arrays and tool payloads are the heavy part); the frontend
        calls this only when a node is first expanded -- the same pattern
        Langfuse's own peek view uses (includeIO: false on the tree query).

        The observation is resolved to its trace and then to the owning task
        instance, and access is gated on that DAG -- so this cannot be used to
        read another DAG's tool IO by guessing observation ids. Content is
        returned only after the access check passes. Never called in store mode
        -- store observations carry their IO inline (the whole file is read
        anyway) and the frontend skips the lazy fetch when the keys are present.
        """
        if _trace_store_path():
            raise HTTPException(
                status_code=404, detail="Observation IO is inlined in the trace payload in store mode."
            )
        conn_id = _trace_backend_conn_id()
        conn = BaseHook.get_connection(conn_id)
        host = (conn.host or "").rstrip("/")
        if not host:
            raise HTTPException(status_code=500, detail=f"Connection {conn_id!r} has no host set.")
        token = base64.b64encode(f"{conn.login}:{conn.password}".encode()).decode()
        resp = requests.get(
            f"{host}/api/public/observations/{obs_id}",
            headers={"Authorization": f"Basic {token}"},
            timeout=10.0,
        )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="observation not found")
        resp.raise_for_status()
        obs = resp.json()
        # Resolve observation -> trace -> owning TI -> DAG, and authorize before
        # returning any IO. An observation whose trace owns no task instance is
        # treated as not found (nothing to authorize against).
        obs_trace_id = obs.get("traceId")
        row = (
            db.execute(
                select(TI.dag_id)
                .where(sa_cast(TI.context_carrier, SAString).like(f"%{obs_trace_id}%"))
                .limit(1)
            ).first()
            if obs_trace_id
            else None
        )
        if row is None:
            raise HTTPException(
                status_code=404,
                detail="No task instance owns this observation's trace, so it cannot be authorized.",
            )
        _require_dag_access(user, row.dag_id)
        return {
            "id": obs.get("id"),
            "input": obs.get("input"),
            "output": obs.get("output"),
            "model_parameters": obs.get("modelParameters"),
        }

    @ai_trace_app.get(
        "/agents",
        dependencies=[Depends(requires_access_dag(method="GET"))],
    )
    def list_agents(db: SessionDep, user: GetUserDep) -> dict[str, Any]:
        """
        Aggregate every agent in the deployment.

        One row per (dag, task) that ran a common.ai operator, RBAC-scoped to
        the user's readable DAGs. Airflow-only on purpose -- run counts,
        failure counts, and recency come straight from the TI table with no
        tracing prerequisite, so an agent that ran with tracing off still
        shows up in the inventory.
        """
        readable_dag_ids = get_auth_manager().get_authorized_dag_ids(user=user, method="GET")
        rows = db.execute(
            select(
                TI.dag_id,
                TI.task_id,
                TI.operator,
                sa_func.count().label("runs"),
                sa_func.sum(sa_case((TI.state == "failed", 1), else_=0)).label("failed"),
                sa_func.max(TI.start_date).label("last_run"),
            )
            .where(TI.operator.in_(_AGENT_OPERATOR_NAMES))
            .where(TI.dag_id.in_(readable_dag_ids))
            .group_by(TI.dag_id, TI.task_id, TI.operator)
            .order_by(sa_func.max(TI.start_date).desc())
        ).all()
        return {
            "items": [
                {
                    "dag_id": r.dag_id,
                    "task_id": r.task_id,
                    "operator": r.operator,
                    "runs": r.runs,
                    "failed": int(r.failed or 0),
                    "last_run": r.last_run.isoformat() if r.last_run else None,
                }
                for r in rows
            ]
        }

    @ai_trace_app.get(
        "/traces",
        dependencies=[Depends(requires_access_dag(method="GET"))],
    )
    def list_traces(
        db: SessionDep,
        user: GetUserDep,
        dag_id: str | None = Query(None),
        task_id: str | None = Query(None),
        since: str | None = Query(None),
        state: str | None = Query(None),
        min_latency: float | None = Query(None, description="seconds; drops rows without enrichment"),
        min_cost: float | None = Query(None, description="USD; drops rows without enrichment"),
        limit: int = Query(50, le=200),
    ) -> dict[str, Any]:
        """
        List recent agent task instances, enriched with trace summaries.

        Airflow supplies the rows (which tasks, dag/run/state); Langfuse is hit
        exactly ONCE for the whole list (not once per row) to enrich them with
        model/tokens/cost/latency/score-count, via its list-traces endpoint
        rather than N per-trace GETs. If that call fails, the list still
        renders with Airflow-only columns -- enrichment is best-effort, the
        list itself is not. Each row also links to the existing
        per-task-instance panel for the full detail.

        ``dag_id``, when passed, scopes both the query AND the RBAC check:
        ``requires_access_dag`` reads ``request.query_params.get("dag_id")``
        itself, so passing it here checks access to that specific DAG rather
        than the generic "any DAG" floor -- this is what the ``dag``-destination
        tab uses; the ``nav``-destination list omits it and gets the floor.

        Uses ``get_auth_manager()`` (the module-level singleton) rather than
        the ``ReadableDagsFilterDep``/``AuthManagerDep`` dependency chain --
        that chain resolves the auth manager from ``request.app.state``, which
        is only populated on the main api-server app. A plugin's ``fastapi_apps``
        entry is a separately mounted sub-app, so ``request.app`` there is this
        module's ``ai_trace_app``, not the main app; ``ReadableDagsFilterDep``
        raises ``KeyError: 'auth_manager'`` in that context.
        """
        readable_dag_ids = get_auth_manager().get_authorized_dag_ids(user=user, method="GET")
        store = _trace_store_path()
        query = (
            select(
                TI.dag_id,
                TI.run_id,
                TI.task_id,
                TI.map_index,
                TI.state,
                TI.start_date,
                TI.operator,
                TI.context_carrier,
            )
            .where(TI.operator.in_(_AGENT_OPERATOR_NAMES))
            .where(TI.dag_id.in_(readable_dag_ids))
            .order_by(TI.start_date.desc())
            .limit(limit)
        )
        if not store:
            # A row without a traceparent can't be correlated to a Langfuse
            # trace, so it has nothing to show. In store mode the FILE is the
            # correlation and pure store mode runs with core tracing off, so
            # every carrier would be empty -- the filter would hide everything.
            query = query.where(TI.context_carrier.is_not(None))
        if dag_id:
            query = query.where(TI.dag_id == dag_id)
        if task_id:
            query = query.where(TI.task_id == task_id)
        if state:
            query = query.where(TI.state == state)
        if since:
            try:
                since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
            except ValueError:
                raise HTTPException(status_code=400, detail="invalid `since` (must be ISO8601)") from None
            query = query.where(TI.start_date >= since_dt)
        rows = db.execute(query).all()

        langfuse_by_id: dict[str, dict[str, Any]] = {}
        if not store:
            # Store mode never touches the trace-backend connection -- part of
            # the zero-infra contract (no Langfuse conn need exist at all).
            conn_id = _trace_backend_conn_id()
            conn = BaseHook.get_connection(conn_id)
            host = (conn.host or "").rstrip("/")
            if host and rows:
                token = base64.b64encode(f"{conn.login}:{conn.password}".encode()).decode()
                min_start = min((r.start_date for r in rows if r.start_date), default=None)
                try:
                    langfuse_by_id = _fetch_langfuse_trace_list(
                        host=host,
                        token=token,
                        from_timestamp=min_start.isoformat() if min_start else None,
                        limit=max(limit, 50),
                    )
                except requests.RequestException:
                    log.warning(
                        "Could not enrich AI Trace list from connection %r; showing Airflow-only rows.",
                        conn_id,
                    )

        store_root = ObjectStoragePath(store) if store else None
        items = []
        for r in rows:
            trace_id = _trace_id_from_context_carrier(r.context_carrier)
            if store_root is not None:
                row_summary = _store_row_summary(store_root, r.dag_id, r.run_id, r.task_id, r.map_index)
                if row_summary is not None:
                    # With core tracing off the carrier is empty; the file's own
                    # trace id is what makes the row clickable (modal deep link).
                    file_trace_id = row_summary.pop("trace_id", None)
                    trace_id = trace_id or file_trace_id
                    summary = row_summary
                else:
                    summary = _summarize_list_row(None)
            else:
                summary = _summarize_list_row(langfuse_by_id.get(trace_id) if trace_id else None)
            items.append(
                {
                    "dag_id": r.dag_id,
                    "run_id": r.run_id,
                    "task_id": r.task_id,
                    "map_index": r.map_index,
                    "state": r.state,
                    "start_date": r.start_date.isoformat() if r.start_date else None,
                    "operator": r.operator,
                    "trace_id": trace_id,
                    **summary,
                }
            )
        # Enrichment-derived filters run post-merge: latency/cost only exist on
        # the Langfuse side, so rows without enrichment are dropped when one of
        # these is set (documented on the params; asking "cost > X" about a row
        # with unknown cost has no honest answer other than exclusion).
        if min_latency is not None:
            items = [i for i in items if i["latency"] is not None and i["latency"] >= min_latency]
        if min_cost is not None:
            items = [i for i in items if i["cost"] is not None and i["cost"] >= min_cost]
        return {"items": items}

    @ai_trace_app.middleware("http")
    async def _revalidate_static(request, call_next):  # type: ignore[no-untyped-def]
        """
        Serve the bundle with ``Cache-Control: no-cache``.

        The bundle URL is unversioned (no content hash), so browsers heuristically
        disk-cache it and keep rendering a stale UI after a plugin upgrade.
        ``no-cache`` forces revalidation, which Starlette's StaticFiles answers
        with a cheap 304 via ETag/Last-Modified when the file hasn't changed.
        """
        response = await call_next(request)
        # request.url.path is the FULL path even inside a mounted sub-app
        # (e.g. /ai-trace/static/main.umd.cjs), so match on the segment.
        if "/static/" in request.url.path:
            response.headers["Cache-Control"] = "no-cache"
        return response

    # Ensure proper MIME types for plugin bundle (FastAPI serves .cjs as text/plain by default)
    mimetypes.add_type("application/javascript", ".cjs")

    _WWW_DIR = Path(__file__).parent / "ai_trace_www"
    _dist_dir = _WWW_DIR / "dist"
    if _dist_dir.is_dir():
        ai_trace_app.mount(
            "/static",
            StaticFiles(directory=str(_dist_dir.absolute()), html=True),
            name="ai_trace_static",
        )


class AITracePlugin(AirflowPlugin):
    """Register the AI Trace REST API + panel on the Airflow API server."""

    name = "ai_trace"
    fastapi_apps: list[dict[str, Any]] = []
    react_apps: list[dict[str, str]] = []
    if AIRFLOW_V_3_1_PLUS:
        fastapi_apps = [
            {
                "name": "ai-trace",
                "app": ai_trace_app,
                "url_prefix": _PLUGIN_PREFIX,
            }
        ]
        react_apps = [
            {
                "name": "AI Trace",
                "bundle_url": _get_bundle_url(),
                "destination": "task_instance",
                "url_route": "ai-trace",
            },
            {
                # Same bundle as the task_instance entry -- the component branches
                # on whether dagId/runId/taskId route params are present (nav has
                # none) to decide list-view vs single-trace-card.
                "name": "AI Traces",
                "bundle_url": _get_bundle_url(),
                "destination": "nav",
                "url_route": "ai-traces",
            },
            {
                # Same bundle; dagId present without runId/taskId -> dag-scoped list.
                # Label matches the nav entry so the feature has ONE name. The
                # host loader caches the component under reactApp.name, but both
                # entries resolve the identical bundle, so sharing is benign.
                "name": "AI Traces",
                "bundle_url": _get_bundle_url(),
                "destination": "dag",
                "url_route": "ai-traces-dag",
            },
        ]
