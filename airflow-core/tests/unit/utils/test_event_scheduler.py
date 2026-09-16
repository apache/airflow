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

from airflow.utils.event_scheduler import EventScheduler


class TestEventScheduler:
    def test_call_regular_interval(self):
        somefunction = mock.MagicMock()

        timers = EventScheduler()
        timers.call_regular_interval(30, somefunction)
        assert len(timers.queue) == 1
        somefunction.assert_not_called()

        # Fake a run (it won't actually pop from the queue):
        timers.queue[0].action()

        # Make sure it added another event to the queue
        assert len(timers.queue) == 2
        somefunction.assert_called_once()
        assert timers.queue[0].time < timers.queue[1].time

    def test_call_regular_interval_propagates_exception_by_default(self):
        """Without opting in, an action's exception still propagates (unchanged default behavior)."""
        failing_action = mock.MagicMock(side_effect=RuntimeError("boom"))

        timers = EventScheduler()
        timers.call_regular_interval(30, failing_action)
        assert len(timers.queue) == 1

        with pytest.raises(RuntimeError, match="boom"):
            timers.queue[0].action()

        failing_action.assert_called_once()
        # The next cycle was never scheduled because the exception propagated.
        assert len(timers.queue) == 1

    def test_call_regular_interval_non_fatal_swallows_action_exception(self):
        """With non_fatal=True, a raising action is swallowed and the next cycle is still scheduled."""
        failing_action = mock.MagicMock(side_effect=RuntimeError("boom"))

        timers = EventScheduler()
        timers.call_regular_interval(30, failing_action, non_fatal=True)
        assert len(timers.queue) == 1

        # Should not raise, even though the action does.
        timers.queue[0].action()

        failing_action.assert_called_once()
        # The next cycle was still scheduled despite the exception.
        assert len(timers.queue) == 2
        assert timers.queue[0].time < timers.queue[1].time
