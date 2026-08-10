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
Unit tests for migration 0055 (e39a26ac59f6) conf sanitization.

The 2.x -> 3.x conversion of ``dag_run.conf`` from pickled bytea to JSON/JSONB happens
Python-side (``json.dumps`` + a per-row insert). ``_json_safe`` quotes non-finite floats
and strips embedded NUL characters so confs carrying those values are preserved instead of
being dropped by the migration's per-row error handler. NUL is handled on the object (not
on the dumped text) so a genuinely escaped backslash sequence is not corrupted. These are
pure-Python tests; no database is required.
"""

from __future__ import annotations

import importlib.util
import json
from collections import OrderedDict
from pathlib import Path

import pytest

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

# A single backslash, built via chr() so no literal escape appears in the source.
_BS = chr(92)
# The 6-char escape json.dumps emits for an embedded null byte.
_NUL_ESCAPE = _BS + "u0000"

_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0055_3_0_0_remove_pickled_data_from_dagrun_table.py"
)
_spec = importlib.util.spec_from_file_location("migration_0055", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

_json_safe = _migration._json_safe


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (float("nan"), "NaN"),
        (float("inf"), "Infinity"),
        (float("-inf"), "-Infinity"),
        (1.5, 1.5),
        (0.0, 0.0),
        (-2.0, -2.0),
        ("plain", "plain"),
        (42, 42),
        (None, None),
        (True, True),
    ],
)
def test_json_safe_scalars(value, expected):
    assert _json_safe(value) == expected


def test_json_safe_strips_null_bytes_in_strings():
    assert _json_safe("foo" + chr(0) + "bar") == "foobar"
    assert _json_safe(chr(0)) == ""


def test_json_safe_preserves_literal_backslash_u_text():
    """A string literally containing backslash-u-0000 (no null byte) must survive intact."""
    literal = "foo" + _NUL_ESCAPE + "bar"
    assert _json_safe(literal) == literal
    # and round-trips through json without corruption
    assert json.loads(json.dumps(_json_safe({"k": literal}))) == {"k": literal}


def test_json_safe_recurses_into_mappings_and_sequences():
    data = OrderedDict(
        [
            ("f", float("nan")),
            ("lst", [float("inf"), 1, {"deep": float("-inf")}]),
            ("tpl", (float("nan"), 2)),
            ("nul" + chr(0), "v" + chr(0)),
            ("keep", 3.14),
        ]
    )
    assert _json_safe(data) == {
        "f": "NaN",
        "lst": ["Infinity", 1, {"deep": "-Infinity"}],
        "tpl": ["NaN", 2],  # tuples normalize to lists, like json.dumps would
        "nul": "v",  # NUL stripped from both key and value
        "keep": 3.14,
    }


def test_json_safe_does_not_explode_strings_into_chars():
    assert _json_safe("hello") == "hello"


def _reject_constant(token):
    raise AssertionError(f"non-finite token survived sanitization: {token!r}")


def test_full_pipeline_yields_strict_valid_json():
    """Mirror the migration's exact serialization: json.dumps(_json_safe(...))."""
    original = {
        "d": "F" + chr(0) + "oo",  # real embedded null byte
        "lit": "x" + _NUL_ESCAPE + "y",  # literal backslash-u-0000 text, must survive
        "a": float("nan"),
        "b": float("inf"),
        "c": float("-inf"),
        "ok": 1.5,
    }
    json_data = json.dumps(_json_safe(original))

    # parse_constant fires on any surviving bare NaN/Infinity/-Infinity token.
    parsed = json.loads(json_data, parse_constant=_reject_constant)
    assert parsed == {
        "d": "Foo",
        "lit": "x" + _NUL_ESCAPE + "y",
        "a": "NaN",
        "b": "Infinity",
        "c": "-Infinity",
        "ok": 1.5,
    }


def test_finite_floats_are_untouched():
    original = {"x": 1.25, "y": [0.0, -3.5], "z": 1000000.0}
    json_data = json.dumps(_json_safe(original))
    assert json.loads(json_data, parse_constant=_reject_constant) == original
