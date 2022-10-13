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
