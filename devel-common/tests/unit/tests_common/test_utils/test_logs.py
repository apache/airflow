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

import re

import pytest

from tests_common.test_utils.logs import StructlogCapture


@pytest.fixture
def capture() -> StructlogCapture:
    cap = StructlogCapture()
    cap.entries.extend(
        [
            {"event": "some message", "level": "info", "count": 1},
            {"event": "other message", "level": "error"},
        ]
    )
    return cap


class TestStructlogCaptureContains:
    def test_str_matches_event(self, capture):
        assert "some message" in capture
        assert "missing" not in capture

    def test_single_key_exact_value(self, capture):
        assert {"event": "some message"} in capture
        assert {"level": "error"} in capture

    def test_single_key_mismatch(self, capture):
        assert {"event": "nope"} not in capture
        assert {"event": "some"} not in capture

    def test_single_key_pattern(self, capture):
        assert {"event": re.compile(r"some mess")} in capture
        assert {"event": re.compile(r"^nope")} not in capture

    def test_multi_key(self, capture):
        assert {"event": "some message", "level": "info"} in capture
        assert {"event": "some message", "level": "error"} not in capture
        assert {"event": re.compile("some"), "count": 1} in capture

    def test_missing_key_is_false(self, capture):
        assert {"absent": 1} not in capture
        assert {"event": "some message", "absent": 1} not in capture

    @pytest.mark.parametrize("target", [1, ["event"], None])
    def test_unsupported_type_raises(self, capture, target):
        with pytest.raises(TypeError, match="Can't search logs"):
            capture.__contains__(target)
