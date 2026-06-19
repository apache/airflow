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

from airflow.api_fastapi.core_api.datamodels.pools import PoolResponse, _sanitize_open_slots


class _BusyUnlimitedPool:
    """Stand-in for an unlimited Pool ORM object with occupied task slots."""

    pool = "unlimited_pool"
    slots = -1
    description = None
    include_deferred = False
    team_name = None

    def occupied_slots(self):
        return 3

    def running_slots(self):
        return 2

    def queued_slots(self):
        return 1

    def scheduled_slots(self):
        return 0

    def open_slots(self):
        return float("inf")

    def deferred_slots(self):
        return 0


def test_sanitize_open_slots_converts_infinity_to_unlimited():
    assert _sanitize_open_slots(float("inf")) == -1
    assert _sanitize_open_slots(3) == 3


def test_pool_response_serializes_busy_unlimited_pool_open_slots_as_unlimited():
    response = PoolResponse.model_validate(_BusyUnlimitedPool(), from_attributes=True)

    assert response.occupied_slots == 3
    assert response.running_slots == 2
    assert response.queued_slots == 1
    assert response.open_slots == -1
    assert response.model_dump(by_alias=True)["name"] == "unlimited_pool"
