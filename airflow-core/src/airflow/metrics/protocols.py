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

import datetime
import time
from typing import TYPE_CHECKING, Protocol, Union

if TYPE_CHECKING:
    from airflow.typing_compat import Self

DeltaType = Union[int, float, datetime.timedelta]


class TimerProtocol(Protocol):
    """Type protocol for StatsLogger.timer."""

    def __enter__(self) -> Self: ...

    def __exit__(self, exc_type, exc_value, traceback) -> None: ...

    def start(self) -> Self:
        """Start the timer."""
        ...

    def stop(self, send: bool = True) -> None:
        """Stop, and (by default) submit the timer to StatsD."""
        ...


class Timer(TimerProtocol):
    """
    Timer that records duration, and optional sends to StatsD backend.

    This class lets us have an accurate timer with the logic in one place (so
    that we don't use datetime math for duration -- it is error prone).

    Example usage:

    .. code-block:: python

        with Stats.timer() as t:
            # Something to time
            frob_the_foos()

        log.info("Frobbing the foos took %.2f", t.duration)

    Or without a context manager:

    .. code-block:: python

        timer = Stats.timer().start()

        # Something to time
        frob_the_foos()

        timer.end()

        log.info("Frobbing the foos took %.2f", timer.duration)

    To send a metric:

    .. code-block:: python

        with Stats.timer("foos.frob"):
            # Something to time
            frob_the_foos()

    Or both:

    .. code-block:: python

        with Stats.timer("foos.frob") as t:
            # Something to time
            frob_the_foos()

        log.info("Frobbing the foos took %.2f", t.duration)
    """

    # pystatsd and dogstatsd both have a timer class, but present different API
    # so we can't use this as a mixin on those, instead this class contains the "real" timer

    _start_time: float | None
    duration: float | None

    def __init__(self, real_timer: Timer | None = None) -> None:
        self.real_timer = real_timer

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()

    def start(self) -> Self:
        """Start the timer."""
        if self.real_timer:
            self.real_timer.start()
        self._start_time = time.perf_counter()
        return self

    def stop(self, send: bool = True) -> None:
        """Stop the timer, and optionally send it to stats backend."""
        if self._start_time is not None:
            self.duration = 1000.0 * (time.perf_counter() - self._start_time)  # Convert to milliseconds.
        if send and self.real_timer:
            self.real_timer.stop()
