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

import logging
from datetime import datetime, timedelta
from typing import Callable

from airflow.utils import timezone

log = logging.getLogger(__name__)


def calculate_next_attempt_delay(
    attempt_number: int,
    max_delay: int = 60 * 2,
    exponent_base: int = 4,
) -> timedelta:
    """
    Calculate the exponential backoff (in seconds) until the next attempt.

    :param attempt_number: Number of attempts since last success.
    :param max_delay: Maximum delay in seconds between retries. Default 120.
    :param exponent_base: Exponent base to calculate delay. Default 4.
    """
    return timedelta(seconds=min((exponent_base**attempt_number), max_delay))


def exponential_backoff_retry(
    last_attempt_time: datetime,
    attempts_since_last_successful: int,
    callable_function: Callable,
    max_delay: int = 60 * 2,
    max_attempts: int = -1,
    exponent_base: int = 4,
) -> None:
    """
    Retry a callable function with exponential backoff between attempts if it raises an exception.

    :param last_attempt_time: Timestamp of last attempt call.
    :param attempts_since_last_successful: Number of attempts since last success.
    :param callable_function: Callable function that will be called if enough time has passed.
    :param max_delay: Maximum delay in seconds between retries. Default 120.
    :param max_attempts: Maximum number of attempts before giving up. Default -1 (no limit).
    :param exponent_base: Exponent base to calculate delay. Default 4.
    """
    if max_attempts != -1 and attempts_since_last_successful >= max_attempts:
        log.error("Max attempts reached. Exiting.")
        return

    next_retry_time = last_attempt_time + calculate_next_attempt_delay(
        attempt_number=attempts_since_last_successful, max_delay=max_delay, exponent_base=exponent_base
    )

    current_time = timezone.utcnow()

    if current_time >= next_retry_time:
        try:
            callable_function()
        except Exception:
            log.exception("Error calling %r", callable_function.__name__)
            next_delay = calculate_next_attempt_delay(
                attempts_since_last_successful + 1, max_delay, exponent_base
            )
            log.info("Waiting for %s seconds before retrying.", next_delay)
