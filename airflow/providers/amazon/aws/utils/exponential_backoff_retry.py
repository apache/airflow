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


def exponential_backoff_retry(
    last_attempt_time: datetime,
    attempts_since_last_successful: int,
    callable_function: Callable,
    initial_delay: int = 1,
    max_delay: int = 60 * 2,
    max_attempts: int = -1,
    exponent_base: int = 4,
) -> None:
    if max_attempts != -1 and attempts_since_last_successful >= max_attempts:
        log.error("Max attempts reached. Exiting.")
        return

    delay = min(initial_delay * (exponent_base**attempts_since_last_successful), max_delay)
    next_retry_time = last_attempt_time + timedelta(seconds=delay)
    current_time = timezone.utcnow()

    if current_time >= next_retry_time:
        try:
            callable_function()
        except Exception:
            log.error(f"Error calling {getattr(callable_function, '__name__', repr(callable_function))}")
    else:
        log.info(f"Waiting for {(next_retry_time - current_time).total_seconds()} seconds before retrying.")
