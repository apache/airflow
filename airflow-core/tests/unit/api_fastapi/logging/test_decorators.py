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

from airflow.api_fastapi.logging.decorators import _sanitize_for_stdlib_log


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
