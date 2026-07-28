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

from unittest import mock

import pytest

from airflow_shared.logging.percent_formatter import PercentFormatRender


class TestPercentFormatRender:
    def test_no_callsite(self):
        fmter = PercentFormatRender("%(filename)s:%(lineno)d %(message)s")

        formatted = fmter(mock.Mock(name="Logger"), "info", {"event": "our msg"})

        assert formatted == "(unknown file):0 our msg"

    def test_lineno_is_none(self):
        fmter = PercentFormatRender("%(filename)s:%(lineno)d %(message)s")

        formatted = fmter(
            mock.Mock(name="Logger"),
            "info",
            {"event": "our msg", "filename": "test.py", "lineno": None},
        )

        assert formatted == "test.py:0 our msg"

    @pytest.mark.parametrize(
        "event",
        [
            pytest.param({"event": "our msg"}, id="missing"),
            pytest.param({"event": "our msg", "process": None, "thread": None}, id="none"),
        ],
    )
    def test_numeric_callsite_without_process_or_thread(self, event):
        # Regression for a scheduler crash: a %d specifier for process/thread with no callsite
        # info (e.g. a warning routed through the logging bridge) must not raise TypeError.
        fmter = PercentFormatRender("%(process)d:%(thread)d %(message)s")

        formatted = fmter(mock.Mock(name="Logger"), "info", event)

        assert formatted == "0:0 our msg"
