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
``<sdk>/capabilities.yaml`` at the root of its own tree. This module owns the *schema* of that
file, the *registry* of SDKs, and :func:`render_markdown_table`, which renders the per-SDK Markdown
table that each SDK's own prek hook embeds in its docs.

Because the manifest is hand-authored, :func:`validate_capabilities` is the only thing standing
between a typo and a wrong published table, so it rejects unknown keys as well as missing ones.

The normative meaning of each dimension lives in ``contributing-docs/30_new_language_sdk.rst``
(the "Conformance" section). Keep the dimensions below in sync with that document.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, NamedTuple, TypedDict, cast

import yaml
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

# A shared capability applies to both authoring modes and is declared once per mode, as
# ``<capability>: {native: <entry>, stub: <entry>}``.
NATIVE_MODE = "native"
STUB_MODE = "stub"
AUTHORING_MODES = (NATIVE_MODE, STUB_MODE)

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
    group: str  # "runtime", "native", "stub", or "shared"
    gated: bool  # its native-Dag entry renders n/a (not ✗) when NATIVE_DAG_GATE is unsupported


# Capability flags, in display order. Runtime capabilities describe what a task body can do in
# either authoring mode; native capabilities describe authoring a whole Dag in the target language;
# stub capabilities describe running @task.stub tasks of a Python Dag; shared capabilities exist in
# both authoring modes and are supported per mode.
CAPABILITY_DIMENSIONS: list[Capability] = [
    Capability("task-logging", "MUST", "runtime", False),
    Capability("xcom-read-write", "MUST", "runtime", False),
    Capability("connection-read", "MUST", "runtime", False),
    Capability("variable-read-write", "MUST", "runtime", False),
    Capability("self-contained-bundle", "MUST", "runtime", False),
    Capability("retry-policy", "MAY", "runtime", False),
    Capability("task-state-store", "MAY", "runtime", False),
    Capability("asset-state-store", "MAY", "runtime", False),
    Capability("asset-event-emit", "MAY", "runtime", False),
    Capability("asset-event-read", "MAY", "runtime", False),
    Capability("object-store", "MAY", "runtime", False),
    Capability(NATIVE_DAG_GATE, "SHOULD", "native", False),
    Capability("dag-args", "MUST", "native", True),
    Capability("task-args", "MUST", "native", True),
    Capability("dag-params", "MUST", "native", True),
    Capability("explicit-dependencies", "MUST", "native", True),
    Capability("taskflow-dependencies", "MUST", "native", True),
    Capability("task-group", "MAY", "native", True),
    Capability("if-else-branching", "SHOULD", "native", True),
    Capability("switch-case-branching", "SHOULD", "native", True),
    Capability("trigger-dag-run", "MAY", "native", True),
    Capability("dynamic-dag-generation", "MAY", "native", True),
    Capability("human-in-the-loop", "MAY", "native", True),
    Capability("dag-test", "SHOULD", "native", True),
    Capability("asset-scheduling", "MAY", "native", True),
    Capability("mixed-lang-stub-target", "MUST", "stub", False),
    Capability("taskflow-binding", "MUST", "stub", False),
    Capability("dynamic-task-mapping", "MAY", "shared", True),
    Capability("asset-inlets-outlets", "MAY", "shared", True),
]

CAPABILITY_NAMES = {cap.name for cap in CAPABILITY_DIMENSIONS}
SHARED_CAPABILITY_NAMES = {cap.name for cap in CAPABILITY_DIMENSIONS if cap.group == "shared"}

LEGEND = (
    f"Marks: {SUPPORTED_MARK} supported · {UNSUPPORTED_MARK} not supported · "
    f"{NA_MARK} not applicable. A tier marked † applies only when `{NATIVE_DAG_GATE}` is supported."
)


class MatrixRow(NamedTuple):
    kind: str  # "state" or "capability"
    name: str
    tier: str
    gated: bool
    mode: str | None  # the authoring mode whose entry a shared capability shows


class MatrixGroup(NamedTuple):
    label: str
    rows: list[MatrixRow]


class MatrixPart(NamedTuple):
    title: str
    groups: list[MatrixGroup]


def _build_capability_rows(groups: tuple[str, ...], mode: str | None) -> list[MatrixRow]:
    return [
        MatrixRow("capability", cap.name, cap.tier, cap.gated and mode != STUB_MODE, mode)
        for group in groups
        for cap in CAPABILITY_DIMENSIONS
        if cap.group == group
    ]


def build_matrix_parts() -> list[MatrixPart]:
    """Split the dimensions into the parts both renderers publish, in display order."""
    return [
        MatrixPart(
            "Common to both authoring modes",
            [
                MatrixGroup(
                    "TaskInstance states",
                    [MatrixRow("state", state, tier, False, None) for state, tier in STATE_DIMENSIONS],
                ),
                MatrixGroup("Task runtime", _build_capability_rows(("runtime",), None)),
            ],
        ),
        MatrixPart(
            "Dag definition",
            [MatrixGroup("Dag definition", _build_capability_rows(("native", "shared"), NATIVE_MODE))],
        ),
        MatrixPart(
            "Python Dag with stub TaskHandler",
            [
                MatrixGroup(
                    "Python Dag with stub TaskHandler", _build_capability_rows(("stub", "shared"), STUB_MODE)
                )
            ],
        ),
    ]


class LangSdk(TypedDict):
    id: str
    capabilities_yaml: Path
    readme: Path


# The registry of Language SDKs and where each one's manifest and README live. The Go and Java SDKs
# declare manifests; the TypeScript entry records where its manifest goes when that runtime declares
# its capabilities. Because of that, `capabilities_yaml` is a declared location and not a promise the
# file exists — a consumer walking the whole registry must check `.exists()` before calling
# load_capabilities().
LANG_SDKS: list[LangSdk] = [
    {
        "id": "go",
        "capabilities_yaml": AIRFLOW_ROOT_PATH / "go-sdk" / "capabilities.yaml",
        "readme": AIRFLOW_ROOT_PATH / "go-sdk" / "README.md",
    },
    {
        "id": "java",
        "capabilities_yaml": AIRFLOW_ROOT_PATH / "java-sdk" / "capabilities.yaml",
        "readme": AIRFLOW_ROOT_PATH / "java-sdk" / "README.md",
    },
    {
        "id": "ts",
        "capabilities_yaml": AIRFLOW_ROOT_PATH / "ts-sdk" / "capabilities.yaml",
        "readme": AIRFLOW_ROOT_PATH / "ts-sdk" / "README.md",
    },
]

VALID_SDK_IDS = {sdk["id"] for sdk in LANG_SDKS}
SDK_DISPLAY_NAMES = {"go": "Go", "java": "Java", "ts": "TypeScript"}


class DimensionEntry(TypedDict, total=False):
    supported: bool
    since: str | None
    sdk_version: str | None
    note: str


class CapabilitiesDoc(TypedDict):
    sdk: str
    supervisor_schema_version: str
    min_airflow_version: str
    states: dict[str, DimensionEntry]
    capabilities: dict[str, DimensionEntry | dict[str, DimensionEntry]]


class CapabilitiesError(ValueError):
    """Raised when a capabilities.yaml file does not match the expected schema."""


def load_capabilities(path: Path, *, expected_sdk: str | None = None) -> CapabilitiesDoc:
    """Load and validate a ``capabilities.yaml`` file.

    ``expected_sdk`` binds the file to the SDK it belongs to: passing it makes a manifest whose
    ``sdk`` field disagrees with the file's own SDK (e.g. ``go-sdk/capabilities.yaml`` declaring
    ``sdk: java``) a validation error instead of silently rendering in the wrong column.
    """
    doc = yaml.safe_load(path.read_text())
    validate_capabilities(doc, source=str(path), expected_sdk=expected_sdk)
    return doc


def validate_capabilities(doc: object, *, source: str, expected_sdk: str | None = None) -> None:
    """Validate a decoded capabilities document, raising :class:`CapabilitiesError` on any issue.

    When ``expected_sdk`` is given, the document's ``sdk`` field must equal it.
    """
    if not isinstance(doc, dict):
        raise CapabilitiesError(f"{source}: top-level value must be a mapping")
    required = {"sdk", "supervisor_schema_version", "min_airflow_version", "states", "capabilities"}
    missing = required - doc.keys()
    if missing:
        raise CapabilitiesError(f"{source}: missing required keys: {', '.join(sorted(missing))}")
    unknown = doc.keys() - required
    if unknown:
        raise CapabilitiesError(f"{source}: unknown top-level keys: {', '.join(sorted(unknown))}")
    if doc["sdk"] not in VALID_SDK_IDS:
        raise CapabilitiesError(
            f"{source}: unknown sdk {doc['sdk']!r}; expected one of {sorted(VALID_SDK_IDS)}"
        )
    if expected_sdk is not None and doc["sdk"] != expected_sdk:
        raise CapabilitiesError(f"{source}: sdk is {doc['sdk']!r} but this file belongs to {expected_sdk!r}")
    for field in ("supervisor_schema_version", "min_airflow_version"):
        if not isinstance(doc[field], str):
            raise CapabilitiesError(f"{source}: {field} must be a string")
    _validate_keys(
        doc["states"], expected={state for state, _ in STATE_DIMENSIONS}, kind="states", source=source
    )
    for name, entry in doc["states"].items():
        _validate_entry(entry, label=f"states.{name}", source=source)
    capabilities = doc["capabilities"]
    _validate_keys(capabilities, expected=CAPABILITY_NAMES, kind="capabilities", source=source)
    for name, entry in capabilities.items():
        if name in SHARED_CAPABILITY_NAMES:
            _validate_keys(entry, expected=set(AUTHORING_MODES), kind=f"capabilities.{name}", source=source)
            for mode in AUTHORING_MODES:
                _validate_entry(entry[mode], label=f"capabilities.{name}.{mode}", source=source)
        else:
            _validate_entry(entry, label=f"capabilities.{name}", source=source)
    if not capabilities[NATIVE_DAG_GATE]["supported"]:
        supported_gated = sorted(
            row.name
            for row in _build_capability_rows(("native", "shared"), NATIVE_MODE)
            if row.gated and get_entry(cast("CapabilitiesDoc", doc), row)["supported"]
        )
        if supported_gated:
            raise CapabilitiesError(
                f"{source}: gated capabilities cannot be supported while {NATIVE_DAG_GATE!r} "
                f"is not supported: {', '.join(supported_gated)}"
            )


def _validate_keys(entries: object, *, expected: set[str], kind: str, source: str) -> None:
    if not isinstance(entries, dict):
        raise CapabilitiesError(f"{source}: {kind!r} must be a mapping")
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


def _validate_entry(entry: object, *, label: str, source: str) -> None:
    if not isinstance(entry, dict) or not isinstance(entry.get("supported"), bool):
        raise CapabilitiesError(f"{source}: {label} must be a mapping with a boolean 'supported'")
    # A misspelled optional key would otherwise be dropped silently and render as a blank cell.
    unknown_fields = entry.keys() - {"supported", "since", "sdk_version", "note"}
    if unknown_fields:
        raise CapabilitiesError(f"{source}: {label} has unknown keys: {', '.join(sorted(unknown_fields))}")
    for field in ("since", "sdk_version"):
        if not isinstance(entry.get(field, None), (str, type(None))):
            raise CapabilitiesError(f"{source}: {label}.{field} must be a string or null")
        if not entry["supported"] and entry.get(field) is not None:
            # A version means "supported since"; carrying one while unsupported is contradictory and
            # would silently render as the not-supported placeholder. Supported *without* a version
            # stays legal — an SDK may not know which release first shipped a dimension.
            raise CapabilitiesError(
                f"{source}: {label} is not supported but carries {field}="
                f"{entry[field]!r}; drop the version or mark it supported"
            )
    if not isinstance(entry.get("note", ""), str):
        raise CapabilitiesError(f"{source}: {label}.note must be a string")


def get_entry(doc: CapabilitiesDoc, row: MatrixRow) -> DimensionEntry:
    if row.kind == "state":
        return doc["states"][row.name]
    entry = doc["capabilities"][row.name]
    if row.mode is not None and row.name in SHARED_CAPABILITY_NAMES:
        return cast("dict[str, DimensionEntry]", entry)[row.mode]
    return cast("DimensionEntry", entry)


def get_mark(doc: CapabilitiesDoc, row: MatrixRow) -> str:
    if row.gated and not doc["capabilities"][NATIVE_DAG_GATE].get("supported"):
        return NA_MARK
    return SUPPORTED_MARK if get_entry(doc, row).get("supported") else UNSUPPORTED_MARK


def format_tier(row: MatrixRow) -> str:
    return f"{row.tier} †" if row.gated else row.tier


def _format_version(entry: DimensionEntry, field: Literal["since", "sdk_version"]) -> str:
    if not entry.get("supported"):
        return NO_VERSION_MARK
    return entry.get(field) or NO_VERSION_MARK


def _format_note(entry: DimensionEntry) -> str:
    return (entry.get("note") or "").replace("|", "\\|")


def render_markdown_table(doc: CapabilitiesDoc) -> list[str]:
    """Render the per-SDK Markdown compatibility table as a list of lines (trailing newlines)."""
    lines = [
        "\n",
        f"*Min. Airflow version: {doc['min_airflow_version']} · "
        f"supervisor schema: {doc['supervisor_schema_version']}*\n",
        "\n",
        "| Dimension | Tier | Supported | Since (Airflow) | Since (SDK) | Notes |\n",
        "|---|---|---|---|---|---|\n",
    ]
    for part in build_matrix_parts():
        for group in part.groups:
            lines.append(f"| **{group.label}** |  |  |  |  |  |\n")
            for row in group.rows:
                entry = get_entry(doc, row)
                lines.append(
                    f"| {row.kind}: `{row.name}` | {format_tier(row)} | {get_mark(doc, row)} | "
                    f"{_format_version(entry, 'since')} | {_format_version(entry, 'sdk_version')} | "
                    f"{_format_note(entry)} |\n"
                )
    lines.append("\n")
    lines.append(f"*{LEGEND}*\n")
    lines.append("\n")
    return lines
