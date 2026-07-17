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

import signal
from unittest import mock

import pytest

from airflow.utils.db import timeout_with_traceback


class TestTimeoutWithTraceback:
    def test_timeout_supported_unix(self):
        """Test that it works normally on Unix-like systems where SIGALRM is supported."""
        if not hasattr(signal, "SIGALRM"):
            pytest.skip("SIGALRM not supported on this platform")

        with mock.patch("signal.signal") as mock_signal, mock.patch("signal.alarm") as mock_alarm:
            mock_signal.return_value = "old_handler"

            with timeout_with_traceback(seconds=5):
                pass

            mock_signal.assert_any_call(signal.SIGALRM, mock.ANY)
            mock_alarm.assert_any_call(5)
            # Cleanup
            mock_alarm.assert_any_call(0)
            mock_signal.assert_any_call(signal.SIGALRM, "old_handler")

    @pytest.mark.parametrize(
        "exception",
        [
            pytest.param(AttributeError("SIGALRM missing"), id="windows_attribute_error"),
            pytest.param(ValueError("signal only works in main thread"), id="non_main_thread_value_error"),
        ],
    )
    def test_timeout_unsupported_platforms_or_threads(self, exception):
        """Test that it handles unsupported platforms (Windows) or non-main threads gracefully."""
        # We need to patch signal.signal to raise the exception
        # Even if SIGALRM exists, we force it to fail to test the catch block

        with (
            mock.patch("signal.signal", side_effect=exception),
            mock.patch("airflow.utils.db.log") as mock_log,
        ):
            with timeout_with_traceback(seconds=5):
                # Should not raise any exception
                pass

            mock_log.warning.assert_called_once_with(
                "timeout_with_traceback requires signal.SIGALRM and the main thread. "
                "Proceeding without a timeout."
            )

    def test_timeout_happens(self):
        """Test that it actually raises TimeoutException when time is up."""
        if not hasattr(signal, "SIGALRM"):
            pytest.skip("SIGALRM not supported on this platform")

        # We can't easily test real timeout in unit test without blocking
        # but we can call the handler directly to see if it raises

        with mock.patch("signal.signal") as mock_signal:
            with timeout_with_traceback(seconds=1):
                # The handler is the second argument to the first call of signal.signal
                handler = mock_signal.call_args_list[0][0][1]

                with pytest.raises(Exception, match="Operation timed out") as excinfo:
                    handler(signal.SIGALRM, None)

                assert "Operation timed out" in str(excinfo.value)
                assert excinfo.type.__name__ == "TimeoutException"
