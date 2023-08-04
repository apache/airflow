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

from sched import scheduler
from typing import Callable


class EventScheduler(scheduler):
    """General purpose event scheduler."""

    def call_regular_interval(
        self,
        delay: float,
        action: Callable,
        arguments=(),
        kwargs={},
    ):
        """Helper to call a function at (roughly) a given interval."""

        def repeat(*args, **kwargs):
            action(*args, **kwargs)
            # This is not perfect. If we want a timer every 60s, but action
            # takes 10s to run, this will run it every 70s.
            # Good enough for now
            self.enter(delay, 1, repeat, args, kwargs)

        self.enter(delay, 1, repeat, arguments, kwargs)
