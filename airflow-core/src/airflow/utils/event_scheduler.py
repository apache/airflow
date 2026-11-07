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

from collections.abc import Callable
from sched import scheduler

from airflow.utils.log.logging_mixin import LoggingMixin


class EventScheduler(scheduler, LoggingMixin):
    """General purpose event scheduler."""

    def call_regular_interval(
        self,
        delay: float,
        action: Callable,
        arguments=(),
        kwargs=None,
        non_fatal: bool = False,
    ):
        """
        Call a function at (roughly) a given interval.

        :param non_fatal: If True, an exception raised by ``action`` is logged and
            swallowed instead of propagating, so a single bad cycle can't kill
            whatever is driving this scheduler. The next cycle is still scheduled either
            way. Defaults to False (propagate), preserving prior behavior for callers
            that rely on the exception surfacing.
        """

        def repeat(*args, **kwargs):
            self.log.debug("Calling %s", action)
            result = None
            if non_fatal:
                try:
                    result = action(*args, **kwargs)
                except Exception as e:
                    self.log.warning(
                        "Failed to run periodic action %s due to %s; will retry on the next cycle",
                        getattr(action, "__name__", action),
                        e,
                        exc_info=True,
                    )
            else:
                result = action(*args, **kwargs)
            # An action may request an earlier next run -- e.g. it cleared a full bounded batch and
            # more work is likely waiting -- by returning a non-negative delay in seconds; otherwise
            # the regular interval is used. A bool is ignored so a truthy/falsey return can't re-time
            # the timer by accident.
            next_delay = delay
            if isinstance(result, (int, float)) and not isinstance(result, bool):
                next_delay = max(0.0, float(result))
            # This is not perfect. If we want a timer every 60s, but action
            # takes 10s to run, this will run it every 70s.
            # Good enough for now
            self.enter(next_delay, 1, repeat, args, kwargs)

        self.enter(delay, 1, repeat, arguments, kwargs or {})
