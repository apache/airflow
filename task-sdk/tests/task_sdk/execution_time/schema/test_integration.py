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
In-process integration tests for the supervisor schema migration seam.

Drive ``WatchedSubprocess.send_msg`` and ``WatchedSubprocess.handle_requests``
directly against a ``MagicMock`` socket, then decode the bytes the production
code wrote and assert on the wire shape. The migrator runs for real against the
mock Cadwyn bundle in :mod:`_mock_version_bundle`, swapped in via
``monkeypatch`` for the duration of one test.
"""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import MagicMock, call

import attrs
import msgspec
import psutil
import pytest
import structlog
from pydantic import TypeAdapter
from task_sdk.execution_time.schema._mock_version_bundle import (
    ALL_VERSIONS,
    MOCK_REGISTRY,
    MOCK_VERSION_BUNDLE,
    _LangSdkRequest,
    _SupervisorResponse,
)
from uuid6 import uuid7

from airflow.sdk.execution_time.comms import _RequestFrame, _ResponseFrame
from airflow.sdk.execution_time.schema import SchemaVersionMigrator
from airflow.sdk.execution_time.supervisor import WatchedSubprocess


@pytest.fixture
def mock_version_migrator(monkeypatch) -> SchemaVersionMigrator:
    """
    Bind the production migrator factory and registry to :data:`MOCK_VERSION_BUNDLE`.

    Three patches are applied:

    1. ``supervisor.get_schema_version_migrator`` -- the already-imported binding
       inside :mod:`airflow.sdk.execution_time.supervisor` that
       ``_serialize_response`` and ``_deserialize_request`` call.
    2. ``schema.get_schema_version_migrator`` -- the canonical
       location, kept in sync so any code that re-imports the symbol sees the
       mock too.
    3. ``schema.registered_models_by_name`` -- used by
       ``resolve_body_class`` (called from ``_deserialize_request``) to map wire
       discriminators to head classes.  Swapped for :data:`MOCK_REGISTRY` so the
       upgrade path resolves ``_LangSdkRequest`` without touching the real comms
       registry.
    """
    migrator = SchemaVersionMigrator(
        bundle=MOCK_VERSION_BUNDLE,
        supervisor_version=MOCK_VERSION_BUNDLE.versions[0].value,
    )
    mock_migrator_factory = lambda: migrator

    monkeypatch.setattr(
        "airflow.sdk.execution_time.supervisor.get_schema_version_migrator",
        mock_migrator_factory,
    )
    monkeypatch.setattr(
        "airflow.sdk.execution_time.schema.get_schema_version_migrator",
        mock_migrator_factory,
    )
    monkeypatch.setattr(
        "airflow.sdk.execution_time.schema.registered_models_by_name",
        lambda: MOCK_REGISTRY,
    )
    return migrator


@attrs.define(kw_only=True)
class _RecordingSupervisor(WatchedSubprocess):
    """``WatchedSubprocess`` that captures every upgraded body it dispatches.

    Production splits the supervisor side across ``ActivitySubprocess``
    (task-execution channel) and ``DagFileProcessorProcess``
    (dag-processing channel). Both subclasses differ only in their
    ``decoder`` ClassVar and forward ``_handle_request`` to channel-specific
    logic. The migration seam exercised here is identical on both
    channels, so one class with the mock-bundle decoder is enough.
    """

    decoder: ClassVar[TypeAdapter] = TypeAdapter(_LangSdkRequest)
    received_msgs: list = attrs.field(factory=list, init=False)

    def _handle_request(self, msg, log, req_id):
        self.received_msgs.append(msg)


def _new_supervisor(pinned_version: str) -> _RecordingSupervisor:
    """Build a :class:`_RecordingSupervisor` with a mock stdin and a pinned migrator version."""
    ws = _RecordingSupervisor(
        id=uuid7(),
        pid=1,
        stdin=MagicMock(),
        process=MagicMock(spec=psutil.Process),
        process_log=structlog.get_logger(),
    )
    # In the reimplementation the field is ``_subprocess_schema_version``,
    # not ``lang_sdk_msg_schema_version``.
    ws._subprocess_schema_version = pinned_version
    return ws


class _WireFrameBody:
    """
    Mock argument matcher that decodes a ``sendall(bytes)`` payload and
    compares the embedded ``_ResponseFrame`` body to *expected_body*.

    Using a matcher (rather than reaching into ``mock.call_args``) lets
    the test stay on the high-level ``assert_called_once_with`` /
    ``assert_has_calls`` API while still asserting on the decoded wire
    dict rather than raw msgpack bytes. ``__eq__`` is invoked by mock
    when comparing recorded call arguments against the expectation.
    """

    def __init__(self, expected_body: dict[str, Any]) -> None:
        self.expected_body = expected_body

    __hash__ = None  # type: ignore[assignment]  # matcher is value-compared, never hashed

    def __eq__(self, raw: object) -> bool:
        if not isinstance(raw, (bytes, bytearray)):
            return NotImplemented
        length = int.from_bytes(raw[:4], "big")
        payload = raw[4 : 4 + length]
        frame = msgspec.msgpack.Decoder(_ResponseFrame).decode(bytes(payload))
        return frame.body == self.expected_body

    def __repr__(self) -> str:
        return f"_WireFrameBody({self.expected_body!r})"


# Full expected wire-body dict per pinned lang-SDK version. Fields
# introduced after the pinned version are absent (trimmed by the
# downgrade walk); fields at-or-before are present with their value
# from ``_HEAD_SUPERVISOR_RESPONSE``.
_EXPECTED_WIRE_BY_VERSION: dict[str, dict[str, Any]] = {
    "3025-12-01": {"type": "_SupervisorResponse", "ti_id": "ti-resp"},
    "3026-02-15": {"type": "_SupervisorResponse", "ti_id": "ti-resp"},
    "3026-03-01": {"type": "_SupervisorResponse", "ti_id": "ti-resp", "response_x": "x-value"},
    "3026-05-10": {"type": "_SupervisorResponse", "ti_id": "ti-resp", "response_x": "x-value"},
    "3026-06-15": {
        "type": "_SupervisorResponse",
        "ti_id": "ti-resp",
        "response_x": "x-value",
        "response_y": "y-value",
    },
    "3026-08-22": {
        "type": "_SupervisorResponse",
        "ti_id": "ti-resp",
        "response_x": "x-value",
        "response_y": "y-value",
    },
    "3026-09-30": {
        "type": "_SupervisorResponse",
        "ti_id": "ti-resp",
        "response_x": "x-value",
        "response_y": "y-value",
        "response_z": "z-value",
    },
}


def _expected_wire_body(pinned_version: str, ti_id: str) -> dict[str, Any]:
    """Return the wire body the lang-SDK runtime must observe, with *ti_id* substituted in."""
    return {**_EXPECTED_WIRE_BY_VERSION[pinned_version], "ti_id": ti_id}


_HEAD_SUPERVISOR_RESPONSE = _SupervisorResponse(
    ti_id="ti-resp",
    response_x="x-value",
    response_y="y-value",
    response_z="z-value",
)


def _wire_request_for(pinned_version: str, ti_id: str) -> dict[str, Any]:
    """
    Build a wire-shape ``_LangSdkRequest`` dict containing exactly the fields a lang-SDK
    runtime pinned to *pinned_version* was built to send.
    """
    wire: dict[str, Any] = {"type": "_LangSdkRequest", "ti_id": ti_id}
    if pinned_version >= "3026-02-15":
        wire["field_a"] = 11
    if pinned_version >= "3026-05-10":
        wire["field_b"] = 22
    if pinned_version >= "3026-08-22":
        wire["field_c"] = 33
    return wire


def _expected_head_request_for(pinned_version: str, ti_id: str) -> _LangSdkRequest:
    """
    Build the head Pydantic shape the supervisor must see after upgrade for a lang-SDK
    runtime pinned to *pinned_version*. Fields the runtime did not send are backfilled to ``0``.
    """
    return _LangSdkRequest(
        ti_id=ti_id,
        field_a=11 if pinned_version >= "3026-02-15" else 0,
        field_b=22 if pinned_version >= "3026-05-10" else 0,
        field_c=33 if pinned_version >= "3026-08-22" else 0,
    )


@pytest.mark.parametrize("pinned_version", ALL_VERSIONS)
def test_send_msg_downgrades_to_pinned_wire_shape(mock_version_migrator, pinned_version):
    """Drive ``send_msg`` and confirm the bytes that hit stdin decode to the expected wire-version dict."""
    ws = _new_supervisor(pinned_version)
    ws.send_msg(_HEAD_SUPERVISOR_RESPONSE, request_id=0)

    expected = _expected_wire_body(pinned_version, ti_id="ti-resp")
    ws.stdin.sendall.assert_called_once_with(_WireFrameBody(expected))


@pytest.mark.parametrize("pinned_version", ALL_VERSIONS)
def test_handle_requests_upgrades_wire_to_head_shape(mock_version_migrator, pinned_version):
    """Drive ``handle_requests`` with a wire-shape frame and confirm the upgraded body reaches the decoder."""
    ws = _new_supervisor(pinned_version)
    wire = _wire_request_for(pinned_version, ti_id="ti-up")

    gen = ws.handle_requests(structlog.get_logger())
    next(gen)
    try:
        gen.send(_RequestFrame(id=1, body=wire))
    finally:
        gen.close()

    assert ws.received_msgs == [_expected_head_request_for(pinned_version, ti_id="ti-up")]


def test_round_trip_preserves_state_across_multiple_frames(mock_version_migrator):
    """
    Send three responses and two requests at the middle pinned version to confirm neither
    direction drops state between frames.
    """
    pinned_version = "3026-05-10"
    ws = _new_supervisor(pinned_version)

    responses = [
        _SupervisorResponse(
            ti_id=f"ti-{i}",
            response_x="x-value",
            response_y="y-value",
            response_z="z-value",
        )
        for i in range(3)
    ]
    for index, response in enumerate(responses):
        ws.send_msg(response, request_id=index)

    ws.stdin.sendall.assert_has_calls(
        [call(_WireFrameBody(_expected_wire_body(pinned_version, ti_id=f"ti-{i}"))) for i in range(3)]
    )
    assert ws.stdin.sendall.call_count == 3

    request_wires = [_wire_request_for(pinned_version, ti_id=f"ti-up-{i}") for i in range(2)]
    expected_heads = [_expected_head_request_for(pinned_version, ti_id=f"ti-up-{i}") for i in range(2)]

    gen = ws.handle_requests(structlog.get_logger())
    next(gen)
    try:
        for index, wire in enumerate(request_wires):
            gen.send(_RequestFrame(id=index + 1, body=wire))
    finally:
        gen.close()

    assert ws.received_msgs == expected_heads


def test_no_migration_when_subprocess_schema_version_unset(monkeypatch):
    """
    When ``_subprocess_schema_version`` is ``None`` (the subprocess has not
    negotiated a schema version), ``send_msg`` must send the head-shape body
    verbatim without invoking the migrator at all.
    """
    # Replace get_schema_version_migrator with a sentinel that fails loudly
    # if called -- it must never be reached when the version is unset.
    sentinel = MagicMock(name="should_not_be_called")
    sentinel.side_effect = AssertionError(
        "get_schema_version_migrator must not be called when version is unset"
    )
    monkeypatch.setattr(
        "airflow.sdk.execution_time.supervisor.get_schema_version_migrator",
        sentinel,
    )

    ws = _RecordingSupervisor(
        id=uuid7(),
        pid=1,
        stdin=MagicMock(),
        process=MagicMock(spec=psutil.Process),
        process_log=structlog.get_logger(),
    )
    # ``_subprocess_schema_version`` is ``None`` by default; no version
    # negotiation has happened.
    assert ws._subprocess_schema_version is None

    head_response = _SupervisorResponse(
        ti_id="no-migration",
        response_x="x",
        response_y="y",
        response_z="z",
    )
    ws.send_msg(head_response, request_id=0)

    # The migrator factory must never have been called.
    sentinel.assert_not_called()
    # The wire body must contain all head fields (no trimming).
    expected = {
        "type": "_SupervisorResponse",
        "ti_id": "no-migration",
        "response_x": "x",
        "response_y": "y",
        "response_z": "z",
    }
    ws.stdin.sendall.assert_called_once_with(_WireFrameBody(expected))
