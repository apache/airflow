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
    @pytest.mark.parametrize(
        "target",
        [
            pytest.param({"event": "some message"}, id="str-event"),
            pytest.param({"level": "error"}, id="str-other-key"),
            pytest.param({"count": 1}, id="non-str-value"),
            pytest.param({"event": re.compile(r"some mess")}, id="pattern"),
        ],
    )
    def test_single_key_dict_is_found(self, capture, target):
        assert target in capture
