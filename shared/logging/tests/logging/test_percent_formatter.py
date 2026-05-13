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

    @mock.patch("airflow_shared.logging.percent_formatter.os.getpid")
    @mock.patch("airflow_shared.logging.percent_formatter.threading.get_ident")
    def test_process_thread_missing_returns_live_values(self, mock_get_ident, mock_getpid):
        mock_getpid.return_value = 123
        mock_get_ident.return_value = 456
        fmt = "%(process)d %(thread)d %(lineno)d %(message)s"
        renderer = PercentFormatRender(fmt)

        event_dict = {"event": "test message"}
        result = renderer(None, "info", event_dict)
        assert result.startswith("123 456 0 test message")

    @mock.patch("airflow_shared.logging.percent_formatter.os.getpid")
    @mock.patch("airflow_shared.logging.percent_formatter.threading.get_ident")
    def test_process_thread_unknown_string_returns_live_values(self, mock_get_ident, mock_getpid):
        mock_getpid.return_value = 123
        mock_get_ident.return_value = 456
        fmt = "%(process)d %(thread)d %(lineno)d %(message)s"
        renderer = PercentFormatRender(fmt)

        event_dict = {
            "event": "test message",
            "process": "(unknown)",
            "thread": "(unknown)",
            "lineno": "(unknown)",
        }
        result = renderer(None, "info", event_dict)
        assert result.startswith("123 456 0 test message")

    def test_process_thread_valid_int_preserved(self):
        fmt = "%(process)d %(thread)d %(lineno)d %(message)s"
        renderer = PercentFormatRender(fmt)

        event_dict = {"event": "test message", "process": 111, "thread": 222, "lineno": 333}
        result = renderer(None, "info", event_dict)
        assert result.startswith("111 222 333 test message")
