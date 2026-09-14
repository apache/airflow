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
def capture():
    cap = StructlogCapture()
    cap.entries.append({"event": "some event", "field1": False, "field2": [1, 2]})
    return cap


class TestStructlogCaptureContains:
    """The dict branch of ``__contains__`` used to collapse a single-key target and never match."""

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param({"event": "some event"}, id="single-key-event"),
            pytest.param({"field2": [1, 2]}, id="single-key-other-field"),
            pytest.param({"event": re.compile("some")}, id="single-key-pattern"),
            pytest.param({"event": "some event", "field1": False}, id="two-keys"),
        ],
    )
    def test_match(self, capture, target):
        assert target in capture

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param({"event": "other event"}, id="single-key-wrong-value"),
            pytest.param({"missing": "x"}, id="single-key-missing"),
            pytest.param({"event": "some event", "field1": True}, id="two-keys-one-wrong"),
        ],
    )
    def test_no_match(self, capture, target):
        assert target not in capture

    def test_empty_target_is_rejected(self, capture):
        with pytest.raises(ValueError, match="empty dict"):
            assert {} in capture
