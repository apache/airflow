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
"""Shared helpers for the Language SDK compatibility matrix.

Every Language SDK (Go, Java, TypeScript) declares what it supports in a hand-authored
capability constant in its own source tree. That constant is serialised to a committed
``<sdk>/generated/lang-sdk/capabilities.json`` file which is the single source consumed here.
This module owns the *schema* of that file, the *registry* of SDKs, and :func:`render_markdown_table`,
which renders the per-SDK Markdown table that each SDK's own prek hook embeds in its docs.

The normative meaning of each dimension lives in ``contributing-docs/30_new_language_sdk.rst``
(the "Conformance" section). Keep the dimensions below in sync with that document.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import NamedTuple, TypedDict

from common_prek_utils import AIRFLOW_ROOT_PATH

# Markers wrapping the generated tables. insert_documentation() keeps these lines and rewrites
# everything between them, so the same constants are reused by every SDK's README hook.
README_MATRIX_HEADER = "<!-- BEGIN AUTO-GENERATED LANG-SDK COMPAT MATRIX -->"
README_MATRIX_FOOTER = "<!-- END AUTO-GENERATED LANG-SDK COMPAT MATRIX -->"

SUPPORTED_MARK = "✓"
UNSUPPORTED_MARK = "✗"
NA_MARK = "n/a"  # a gated native-Dag capability while native-dag-authoring is unsupported
NO_VERSION_MARK = "–"  # "Since" placeholder for a dimension that is not supported

# The umbrella capability that gates the conditional native-Dag capabilities: when an SDK does
# not support it, every gated native-Dag capability is "not applicable" rather than unsupported.
NATIVE_DAG_GATE = "native-dag-authoring"

# TaskInstance states a subprocess can emit, in display order, with their conformance tier.
# Scheduler-owned states (queued, scheduled, running, restarting, upstream_failed) are never
# emitted by an SDK runtime and are deliberately excluded.
STATE_DIMENSIONS: list[tuple[str, str]] = [
    ("success", "MUST"),
    ("failed", "MUST"),
    ("up_for_retry", "MUST"),
    ("skipped", "SHOULD"),
    ("deferred", "MAY"),
    ("up_for_reschedule", "MAY"),
    ("awaiting_input", "MAY"),
    ("removed", "MAY"),
]


class Capability(NamedTuple):
    name: str
    tier: str
    group: str  # "runtime" or "native"
    gated: bool  # renders n/a (not ✗) when NATIVE_DAG_GATE is unsupported


# Capability flags, in display order. Runtime capabilities describe what a task body can do while
# it runs (in either a mixed-lang or native Dag); native-Dag capabilities describe authoring a
# whole Dag in the target language and are gated by NATIVE_DAG_GATE (except the gate itself).
CAPABILITY_DIMENSIONS: list[Capability] = [
    Capability("mixed-lang-stub-target", "MUST", "runtime", False),
    Capability("task-logging", "MUST", "runtime", False),
    Capability("xcom-read-write", "MUST", "runtime", False),
    Capability("connection-read", "MUST", "runtime", False),
    Capability("variable-read-write", "MUST", "runtime", False),
    Capability("self-contained-bundle", "MUST", "runtime", False),
    Capability("task-state-store", "MAY", "runtime", False),
    Capability("asset-state-store", "MAY", "runtime", False),
    Capability("asset-event-emit", "MAY", "runtime", False),
    Capability("asset-event-read", "MAY", "runtime", False),
    Capability(NATIVE_DAG_GATE, "SHOULD", "native", False),
    Capability("task-args", "MUST", "native", True),
    Capability("dag-params", "MUST", "native", True),
    Capability("taskflow-dependencies", "MUST", "native", True),
    Capability("branching", "SHOULD", "native", True),
    Capability("dag-test", "SHOULD", "native", True),
    Capability("task-group", "MAY", "native", True),
    Capability("dynamic-task-mapping", "MAY", "native", True),
    Capability("asset-inlets-outlets", "MAY", "native", True),
    Capability("asset-scheduling", "MAY", "native", True),
    Capability("object-store", "MAY", "native", True),
]

CAPABILITY_NAMES = {cap.name for cap in CAPABILITY_DIMENSIONS}

GROUP_LABELS = {"runtime": "Runtime capabilities", "native": "Native-Dag authoring"}
STATES_GROUP_LABEL = "TaskInstance states"

LEGEND = (
    f"Marks: {SUPPORTED_MARK} supported · {UNSUPPORTED_MARK} not supported · "
    f"{NA_MARK} not applicable. A tier marked † applies only when `{NATIVE_DAG_GATE}` is supported."
)


class LangSdk(TypedDict):
    id: str
    capabilities_json: Path
    readme: Path


# The registry of Language SDKs and where each one's manifest and README live. Each SDK's dumper
# writes its manifest to generated/lang-sdk/capabilities.json under the SDK root (a generated
# artifact kept out of the hand-authored source tree). Only the Java SDK publishes one so far; the
# Go and TypeScript entries record where theirs go when those runtimes declare their capabilities.
# Because of that, `capabilities_json` is a declared location and not a promise the file exists — a
# consumer walking the whole registry must check `.exists()` before calling load_capabilities().
LANG_SDKS: list[LangSdk] = [
    {
        "id": "go",
        "capabilities_json": AIRFLOW_ROOT_PATH / "go-sdk" / "generated" / "lang-sdk" / "capabilities.json",
        "readme": AIRFLOW_ROOT_PATH / "go-sdk" / "README.md",
    },
    {
        "id": "java",
        "capabilities_json": AIRFLOW_ROOT_PATH / "java-sdk" / "generated" / "lang-sdk" / "capabilities.json",
        "readme": AIRFLOW_ROOT_PATH / "java-sdk" / "README.md",
    },
    {
        "id": "ts",
        "capabilities_json": AIRFLOW_ROOT_PATH / "ts-sdk" / "generated" / "lang-sdk" / "capabilities.json",
        "readme": AIRFLOW_ROOT_PATH / "ts-sdk" / "README.md",
    },
]

VALID_SDK_IDS = {sdk["id"] for sdk in LANG_SDKS}


class DimensionEntry(TypedDict, total=False):
    supported: bool
    since: str | None
    note: str


class CapabilitiesDoc(TypedDict):
    sdk: str
    supervisor_schema_version: str
    min_airflow_version: str
    states: dict[str, DimensionEntry]
    capabilities: dict[str, DimensionEntry]


class CapabilitiesError(ValueError):
    """Raised when a capabilities.json file does not match the expected schema."""


def load_capabilities(path: Path, *, expected_sdk: str | None = None) -> CapabilitiesDoc:
    """Load and validate a ``capabilities.json`` file.

    ``expected_sdk`` binds the file to the SDK it belongs to: passing it makes a manifest whose
    ``sdk`` field disagrees with the file's own SDK (e.g. ``go-sdk/capabilities.json`` declaring
    ``"sdk": "java"``) a validation error instead of silently rendering in the wrong column.
    """
    doc = json.loads(path.read_text())
    validate_capabilities(doc, source=str(path), expected_sdk=expected_sdk)
    return doc


def validate_capabilities(doc: object, *, source: str, expected_sdk: str | None = None) -> None:
    """Validate a decoded capabilities document, raising :class:`CapabilitiesError` on any issue.

    Validation is done by hand rather than with jsonschema so the prek hooks stay dependency-free.
    When ``expected_sdk`` is given, the document's ``sdk`` field must equal it.
    """
    if not isinstance(doc, dict):
        raise CapabilitiesError(f"{source}: top-level value must be an object")
    required = {"sdk", "supervisor_schema_version", "min_airflow_version", "states", "capabilities"}
    missing = required - doc.keys()
    if missing:
        raise CapabilitiesError(f"{source}: missing required keys: {', '.join(sorted(missing))}")
    if doc["sdk"] not in VALID_SDK_IDS:
        raise CapabilitiesError(
            f"{source}: unknown sdk {doc['sdk']!r}; expected one of {sorted(VALID_SDK_IDS)}"
        )
    if expected_sdk is not None and doc["sdk"] != expected_sdk:
        raise CapabilitiesError(f"{source}: sdk is {doc['sdk']!r} but this file belongs to {expected_sdk!r}")
    for field in ("supervisor_schema_version", "min_airflow_version"):
        if not isinstance(doc[field], str):
            raise CapabilitiesError(f"{source}: {field} must be a string")
    _validate_entries(
        doc["states"], expected={state for state, _ in STATE_DIMENSIONS}, kind="states", source=source
    )
    _validate_entries(doc["capabilities"], expected=CAPABILITY_NAMES, kind="capabilities", source=source)


def _validate_entries(entries: object, *, expected: set[str], kind: str, source: str) -> None:
    if not isinstance(entries, dict):
        raise CapabilitiesError(f"{source}: {kind!r} must be an object")
    actual = set(entries.keys())
    if actual != expected:
        missing = expected - actual
        unknown = actual - expected
        problems = []
        if missing:
            problems.append(f"missing {sorted(missing)}")
        if unknown:
            problems.append(f"unknown {sorted(unknown)}")
        raise CapabilitiesError(f"{source}: {kind} keys mismatch: {'; '.join(problems)}")
    for name, entry in entries.items():
        if not isinstance(entry, dict) or not isinstance(entry.get("supported"), bool):
            raise CapabilitiesError(f"{source}: {kind}.{name} must be an object with a boolean 'supported'")
        if not isinstance(entry.get("since", None), (str, type(None))):
            raise CapabilitiesError(f"{source}: {kind}.{name}.since must be a string or null")
        if not entry["supported"] and entry.get("since") is not None:
            # "Since" means "supported since"; carrying one while unsupported is contradictory and
            # would silently render as the not-supported placeholder. Supported *without* a version
            # stays legal — an SDK may not know which release first shipped a dimension.
            raise CapabilitiesError(
                f"{source}: {kind}.{name} is not supported but carries since="
                f"{entry['since']!r}; drop the version or mark it supported"
            )
        if not isinstance(entry.get("note", ""), str):
            raise CapabilitiesError(f"{source}: {kind}.{name}.note must be a string")


def _state_mark(entry: DimensionEntry) -> str:
    return SUPPORTED_MARK if entry.get("supported") else UNSUPPORTED_MARK


def _capability_mark(doc: CapabilitiesDoc, cap: Capability) -> str:
    if cap.gated and not doc["capabilities"][NATIVE_DAG_GATE].get("supported"):
        return NA_MARK
    return SUPPORTED_MARK if doc["capabilities"][cap.name].get("supported") else UNSUPPORTED_MARK


def _since(entry: DimensionEntry) -> str:
    if not entry.get("supported"):
        return NO_VERSION_MARK
    return entry.get("since") or NO_VERSION_MARK


def _note(entry: DimensionEntry) -> str:
    return (entry.get("note") or "").replace("|", "\\|")


def _tier_label(cap: Capability) -> str:
    return f"{cap.tier} †" if cap.gated else cap.tier


def render_markdown_table(doc: CapabilitiesDoc) -> list[str]:
    """Render the per-SDK Markdown compatibility table as a list of lines (trailing newlines)."""
    lines = [
        "\n",
        f"*Min. Airflow version: {doc['min_airflow_version']} · "
        f"supervisor schema: {doc['supervisor_schema_version']}*\n",
        "\n",
        "| Dimension | Tier | Supported | Since | Notes |\n",
        "|---|---|---|---|---|\n",
        f"| **{STATES_GROUP_LABEL}** |  |  |  |  |\n",
    ]
    for state, tier in STATE_DIMENSIONS:
        entry = doc["states"][state]
        lines.append(
            f"| state: `{state}` | {tier} | {_state_mark(entry)} | {_since(entry)} | {_note(entry)} |\n"
        )
    current_group = ""
    for cap in CAPABILITY_DIMENSIONS:
        if cap.group != current_group:
            current_group = cap.group
            lines.append(f"| **{GROUP_LABELS[cap.group]}** |  |  |  |  |\n")
        entry = doc["capabilities"][cap.name]
        lines.append(
            f"| capability: `{cap.name}` | {_tier_label(cap)} | {_capability_mark(doc, cap)} | "
            f"{_since(entry)} | {_note(entry)} |\n"
        )
    lines.append("\n")
    lines.append(f"*{LEGEND}*\n")
    lines.append("\n")
    return lines
