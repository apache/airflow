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

from datetime import datetime
from unittest import mock

import pytest

from airflow.providers.amazon.aws.executors.utils.exponential_backoff_retry import (
    calculate_next_attempt_delay,
    exponential_backoff_retry,
)


class TestExponentialBackoffRetry:
    @mock.patch("airflow.utils.timezone.utcnow")
    def test_exponential_backoff_retry_base_case(self, mock_utcnow):
        mock_utcnow.return_value = datetime(2023, 1, 1, 12, 0, 5)
        mock_callable_function = mock.Mock()
        exponential_backoff_retry(
            last_attempt_time=datetime(2023, 1, 1, 12, 0, 0),
            attempts_since_last_successful=0,
            callable_function=mock_callable_function,
        )
        mock_callable_function.assert_called_once()

    @mock.patch("airflow.utils.timezone.utcnow")
    @pytest.mark.parametrize(
        "attempt_number, utcnow_value, expected_calls",
        [
            (
                0,
                datetime(2023, 1, 1, 12, 0, 2),
                1,
            ),
            (
                1,
                datetime(2023, 1, 1, 12, 0, 3),
                0,
            ),  # delay is 4 seconds; no call made
            (
                1,
                datetime(2023, 1, 1, 12, 0, 4),
                1,
            ),
            (
                2,
                datetime(2023, 1, 1, 12, 0, 15),
                0,
            ),  # delay is 16 seconds; no call made
            (
                2,
                datetime(2023, 1, 1, 12, 0, 16),
                1,
            ),
            (
                3,
                datetime(2023, 1, 1, 12, 1, 3),
                0,
            ),  # delay is 64 seconds; no call made
            (
                3,
                datetime(2023, 1, 1, 12, 1, 4),
                1,
            ),
            (
                4,
                datetime(2023, 1, 1, 12, 1, 59),
                0,
            ),  # delay is 120 seconds; no call made
            (
                4,
                datetime(2023, 1, 1, 12, 2, 0),
                1,
            ),
            (
                5,
                datetime(2023, 1, 1, 12, 1, 59),
                0,
            ),  # delay is 120 seconds; no call made
            (
                5,
                datetime(2023, 1, 1, 12, 2, 0),
                1,
            ),
            (
                99,
                datetime(2023, 1, 1, 12, 1, 59),
                0,
            ),  # delay is 120 seconds; no call made
            (
                99,
                datetime(2023, 1, 1, 12, 2, 0),
                1,
            ),
        ],
    )
    def test_exponential_backoff_retry_parameterized(
        self, mock_utcnow, attempt_number, utcnow_value, expected_calls
    ):
        mock_callable_function = mock.Mock()
        mock_callable_function.__name__ = "test_callable_function"
        mock_callable_function.side_effect = Exception()
        mock_utcnow.return_value = utcnow_value

        exponential_backoff_retry(
            last_attempt_time=datetime(2023, 1, 1, 12, 0, 0),
            attempts_since_last_successful=attempt_number,
            callable_function=mock_callable_function,
        )
        assert mock_callable_function.call_count == expected_calls

    @mock.patch("airflow.utils.timezone.utcnow")
    def test_exponential_backoff_retry_fail_success(self, mock_utcnow, caplog):
        mock_callable_function = mock.Mock()
        mock_callable_function.__name__ = "test_callable_function"
        mock_callable_function.side_effect = [Exception(), True]
        mock_utcnow.return_value = datetime(2023, 1, 1, 12, 0, 2)
        exponential_backoff_retry(
            last_attempt_time=datetime(2023, 1, 1, 12, 0, 0),
            attempts_since_last_successful=0,
            callable_function=mock_callable_function,
        )
        mock_callable_function.assert_called_once()
        assert any("Error calling" in log for log in caplog.messages)
        caplog.clear()  # clear messages so that we have clean logs for the next call

        mock_utcnow.return_value = datetime(2023, 1, 1, 12, 0, 6)
        exponential_backoff_retry(
            last_attempt_time=datetime(2023, 1, 1, 12, 0, 0),
            attempts_since_last_successful=1,
            callable_function=mock_callable_function,
        )
        assert all("Error calling" not in log for log in caplog.messages)

    @mock.patch("airflow.utils.timezone.utcnow")
    def test_exponential_backoff_retry_max_delay(self, mock_utcnow):
        mock_callable_function = mock.Mock()
        mock_callable_function.__name__ = "test_callable_function"
        mock_callable_function.return_value = Exception()
        mock_utcnow.return_value = datetime(2023, 1, 1, 12, 4, 15)
        exponential_backoff_retry(
            last_attempt_time=datetime(2023, 1, 1, 12, 0, 0),
            attempts_since_last_successful=4,
            callable_function=mock_callable_function,
            max_delay=60 * 5,
        )
        mock_callable_function.assert_not_called()  # delay is 256 seconds; no calls made
        mock_utcnow.return_value = datetime(2023, 1, 1, 12, 4, 16)
        exponential_backoff_retry(
            last_attempt_time=datetime(2023, 1, 1, 12, 0, 0),
            attempts_since_last_successful=4,
            callable_function=mock_callable_function,
            max_delay=60 * 5,
        )
        mock_callable_function.assert_called_once()

        mock_utcnow.return_value = datetime(2023, 1, 1, 12, 5, 0)
        exponential_backoff_retry(
            last_attempt_time=datetime(2023, 1, 1, 12, 0, 0),
            attempts_since_last_successful=5,
            callable_function=mock_callable_function,
            max_delay=60 * 5,
        )
        # delay should be 4^5=1024 seconds, but max_delay is 60*5=300 seconds
        assert mock_callable_function.call_count == 2

    @mock.patch("airflow.utils.timezone.utcnow")
    def test_exponential_backoff_retry_max_attempts(self, mock_utcnow, caplog):
        mock_callable_function = mock.Mock()
        mock_callable_function.__name__ = "test_callable_function"
        mock_callable_function.return_value = Exception()
        mock_utcnow.return_value = datetime(2023, 1, 1, 12, 55, 0)
        for i in range(10):
            exponential_backoff_retry(
                last_attempt_time=datetime(2023, 1, 1, 12, 0, 0),
                attempts_since_last_successful=i,
                callable_function=mock_callable_function,
                max_attempts=3,
            )
        assert any("Max attempts reached." in log for log in caplog.messages)
        assert mock_callable_function.call_count == 3

    @mock.patch("airflow.utils.timezone.utcnow")
    @pytest.mark.parametrize(
        "attempt_number, utcnow_value, expected_calls",
        [
            (
                0,
                datetime(2023, 1, 1, 12, 0, 2),
                1,
            ),
            (
                1,
                datetime(2023, 1, 1, 12, 0, 2),
                0,
            ),  # delay is 3 seconds; no call made
            (
                1,
                datetime(2023, 1, 1, 12, 0, 3),
                1,
            ),
            (
                2,
                datetime(2023, 1, 1, 12, 0, 8),
                0,
            ),  # delay is 9 seconds; no call made
            (
                2,
                datetime(2023, 1, 1, 12, 0, 9),
                1,
            ),
            (
                3,
                datetime(2023, 1, 1, 12, 0, 26),
                0,
            ),  # delay is 27 seconds; no call made
            (
                3,
                datetime(2023, 1, 1, 12, 0, 27),
                1,
            ),
            (
                4,
                datetime(2023, 1, 1, 12, 1, 20),
                0,
            ),  # delay is 81 seconds; no call made
            (
                4,
                datetime(2023, 1, 1, 12, 1, 21),
                1,
            ),
            (
                5,
                datetime(2023, 1, 1, 12, 1, 59),
                0,
            ),  # delay is 120 seconds; no call made
            (
                5,
                datetime(2023, 1, 1, 12, 2, 0),
                1,
            ),
            (
                99,
                datetime(2023, 1, 1, 12, 1, 59),
                0,
            ),  # delay is 120 seconds; no call made
            (
                99,
                datetime(2023, 1, 1, 12, 2, 0),
                1,
            ),
        ],
    )
    def test_exponential_backoff_retry_exponent_base_parameterized(
        self, mock_utcnow, attempt_number, utcnow_value, expected_calls
    ):
        mock_callable_function = mock.Mock()
        mock_callable_function.__name__ = "test_callable_function"
        mock_callable_function.side_effect = Exception()
        mock_utcnow.return_value = utcnow_value

        exponential_backoff_retry(
            last_attempt_time=datetime(2023, 1, 1, 12, 0, 0),
            attempts_since_last_successful=attempt_number,
            callable_function=mock_callable_function,
            exponent_base=3,
        )
        assert mock_callable_function.call_count == expected_calls

    def test_calculate_next_attempt_delay(self):
        exponent_base: int = 4
        num_loops: int = 3
        # Setting max_delay this way means there will be three loops will run to test:
        # one will return a value under max_delay, one equal to max_delay, and one over.
        max_delay: int = exponent_base**num_loops - 1

        for attempt_number in range(1, num_loops):
            returned_delay = calculate_next_attempt_delay(attempt_number, max_delay, exponent_base).seconds

            if (expected_delay := exponent_base**attempt_number) <= max_delay:
                assert returned_delay == expected_delay
            else:
                assert returned_delay == max_delay
