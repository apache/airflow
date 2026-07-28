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

import pytest

from airflow.api_fastapi.logging.decorators import (
    _mask_connection_fields,
    _mask_variable_fields,
    _sanitize_for_stdlib_log,
)


class TestSanitizeForStdlibLog:
    """User input passed to stdlib ``%s``-style logging must have CR/LF stripped.

    On a deployment configured with a non-JSON (plain-text) log formatter, a newline in the
    value would otherwise let an attacker forge log lines (CWE-117). The audit logging path
    in ``action_logging`` passes ``logical_date`` from the request through stdlib's ``logger``,
    so this sanitisation is unconditional regardless of the formatter actually in use.
    """

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
            ("bad\ndate", "bad date"),
            ("bad\r\ndate", "bad  date"),
            ("bad\rdate", "bad date"),
            ("a\nb\nc", "a b c"),
            ("", ""),
        ],
    )
    def test_strips_cr_and_lf(self, raw, expected):
        assert _sanitize_for_stdlib_log(raw) == expected


class TestMaskConnectionFields:
    """Connection ``extra`` values must never be recorded verbatim in the audit log.

    Secrets routinely live in connection ``extra`` under key names that are not in the
    masker's sensitive-key list (``sas_url``, ``endpoint``, ``jdbc_url``, …). Masking by
    key name alone therefore lets those values through. ``_mask_connection_fields`` fails
    closed instead: every ``extra`` value is masked and only the key names are kept.
    """

    def test_masks_extra_value_under_non_sensitive_key(self):
        result = _mask_connection_fields(
            {"extra": json.dumps({"sas_url": "SAS_LEAK_value", "account_name": "prodacct"})}
        )
        assert result["extra"] == {"sas_url": "***", "account_name": "***"}

    def test_masks_extra_value_under_sensitive_key(self):
        result = _mask_connection_fields({"extra": json.dumps({"password": "hunter2"})})
        assert result["extra"] == {"password": "***"}

    def test_preserves_extra_key_names(self):
        result = _mask_connection_fields({"extra": json.dumps({"host": "h", "login": "l", "endpoint": "e"})})
        assert set(result["extra"]) == {"host", "login", "endpoint"}
        assert all(v == "***" for v in result["extra"].values())

    @pytest.mark.enable_redact
    def test_top_level_password_is_masked_by_key_name(self):
        # Top-level connection fields keep the existing key-name redaction path
        # (unchanged by this fix); it only masks when the secrets masker is active.
        result = _mask_connection_fields({"connection_id": "c1", "password": "hunter2"})
        assert result["connection_id"] == "c1"
        assert result["password"] == "***"

    def test_non_dict_json_extra_returns_informational_string(self):
        result = _mask_connection_fields({"extra": json.dumps(["not", "a", "dict"])})
        assert result["extra"] == "Expected JSON object in `extra` field, got non-dict JSON"

    def test_non_json_extra_returns_informational_string(self):
        result = _mask_connection_fields({"extra": "this is not json"})
        assert result["extra"] == "Encountered non-JSON in `extra` field"

    def test_empty_extra_passes_through_key_name_redaction(self):
        # falsy ``extra`` skips the JSON branch and goes through key-name redaction
        result = _mask_connection_fields({"extra": ""})
        assert result["extra"] == ""


class TestMaskVariableFields:
    """The variable value is masked unconditionally in the audit log.

    A secret stored as a Variable under an innocuous key name (e.g.
    ``campaign_signing_material``) must not be persisted to the audit log, so the value is
    masked regardless of the key name; the key and description are kept.
    """

    def test_masks_value_under_non_sensitive_key(self):
        result = _mask_variable_fields(
            {"key": "campaign_signing_material", "value": "VARVAL_LEAK_token", "description": "d"}
        )
        assert result == {"key": "campaign_signing_material", "value": "***", "description": "d"}

    def test_masks_value_under_sensitive_key(self):
        result = _mask_variable_fields({"key": "api_secret", "value": "topsecret"})
        assert result == {"key": "api_secret", "value": "***"}

    def test_masks_val_alias(self):
        result = _mask_variable_fields({"key": "k", "val": "secretval"})
        assert result == {"key": "k", "val": "***"}

    def test_value_without_key_is_still_masked(self):
        result = _mask_variable_fields({"value": "secretval"})
        assert result == {"value": "***"}
