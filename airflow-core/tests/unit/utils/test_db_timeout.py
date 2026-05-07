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

from airflow.utils.db import timeout_with_traceback


class TestTimeoutWithTraceback:
    def test_timeout_with_traceback_basic(self):
        """Test that the context manager works normally on supported systems."""
        with mock.patch("signal.signal") as mock_signal, mock.patch("signal.alarm") as mock_alarm:
            mock_signal.return_value = "old_handler"

            with timeout_with_traceback(10):
                pass

            # Check signal handler was set
            mock_signal.assert_any_call(mock.ANY, mock.ANY)
            mock_alarm.assert_any_call(10)

            # Check cleanup happened
            mock_alarm.assert_any_call(0)
            mock_signal.assert_any_call(mock.ANY, "old_handler")

    @pytest.mark.parametrize("exception", [AttributeError, ValueError])
    def test_timeout_with_traceback_unsupported(self, exception, caplog):
        """
        Test that it handles missing SIGALRM or non-main thread gracefully.
        AttributeError happens on Windows (missing SIGALRM).
        ValueError happens in non-main threads (signal.signal not allowed).
        """
        # Patch BOTH signal.signal and signal.SIGALRM to be safe across platforms
        with mock.patch("signal.signal", side_effect=exception), mock.patch("signal.SIGALRM", create=True):
            with timeout_with_traceback(10):
                # Should not raise exception
                pass

            assert "timeout_with_traceback requires signal.SIGALRM and the main thread" in caplog.text
            assert "Proceeding without a timeout" in caplog.text
