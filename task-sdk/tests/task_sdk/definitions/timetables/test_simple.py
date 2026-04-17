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

from airflow.sdk import PartitionAtRuntime
from airflow.sdk.definitions.timetables.simple import (
    ContinuousTimetable,
    NullTimetable,
    OnceTimetable,
    PartitionAtRuntime as PartitionAtRuntimeDirect,
)


class TestPartitionAtRuntime:
    def test_is_importable_from_airflow_sdk(self):
        assert PartitionAtRuntime is PartitionAtRuntimeDirect

    def test_cannot_be_scheduled(self):
        assert PartitionAtRuntime.can_be_scheduled is False

    def test_distinct_from_null_timetable(self):
        assert not isinstance(PartitionAtRuntime(), NullTimetable)
        assert not isinstance(NullTimetable(), PartitionAtRuntime)

    def test_distinct_from_other_simple_timetables(self):
        assert not isinstance(PartitionAtRuntime(), (OnceTimetable, ContinuousTimetable))
