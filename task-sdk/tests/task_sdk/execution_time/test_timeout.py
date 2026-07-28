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

from airflow.sdk.exceptions import AirflowTaskTimeout
from airflow.sdk.execution_time.timeout import TimeoutPosix


class TestTimeoutPosix:
    """Mirror of TestTimeoutWithTraceback in airflow-core/tests/unit/utils/test_timeout_traceback.py."""

    def test_timeout_supported_unix(self):
        """On Unix-like systems with SIGALRM, setup arms the handler + itimer and cleanup disarms it."""
        if not hasattr(signal, "SIGALRM"):
            pytest.skip("SIGALRM not supported on this platform")

        with (
            mock.patch("signal.signal") as mock_signal,
            mock.patch("signal.setitimer") as mock_setitimer,
        ):
            with TimeoutPosix(seconds=5):
                pass

            mock_signal.assert_any_call(signal.SIGALRM, mock.ANY)
            mock_setitimer.assert_any_call(signal.ITIMER_REAL, 5)
            # cleanup disarms the itimer
            mock_setitimer.assert_any_call(signal.ITIMER_REAL, 0)

    @pytest.mark.parametrize(
        "exception",
        [
            pytest.param(AttributeError("SIGALRM missing"), id="windows_attribute_error"),
            pytest.param(ValueError("signal only works in main thread"), id="non_main_thread_value_error"),
        ],
    )
    def test_timeout_unsupported_platforms_or_threads(self, exception):
        """Windows (no SIGALRM) and non-main threads degrade gracefully: warn + no-op, no exception leaks."""
        with mock.patch("signal.signal", side_effect=exception):
            tp = TimeoutPosix(seconds=5)
            tp.log = mock.MagicMock()
            with tp:
                # body must execute; the context manager must not raise
                pass

            tp.log.warning.assert_called_once_with(
                "TimeoutPosix requires signal.SIGALRM and the main thread. Proceeding without a timeout."
            )
            # cleanup must also be a no-op (it relies on _timeout_supported, not hasattr)
            assert tp._timeout_supported is False

    def test_timeout_happens(self):
        """Calling the handler directly raises AirflowTaskTimeout with the configured message."""
        if not hasattr(signal, "SIGALRM"):
            pytest.skip("SIGALRM not supported on this platform")

        tp = TimeoutPosix(seconds=1, error_message="boom")
        with pytest.raises(AirflowTaskTimeout, match="boom"):
            tp.handle_timeout(signal.SIGALRM, None)
