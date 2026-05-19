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

import pytest

from airflow.sdk.definitions.timetables._cron import CronMixin, cron_presets
from airflow.sdk.exceptions import AirflowTimetableInvalid


@pytest.mark.parametrize("preset", list(cron_presets))
def test_validate_accepts_cron_preset(preset):
    CronMixin(expression=preset, timezone="UTC").validate()


@pytest.mark.parametrize(("preset", "expanded"), list(cron_presets.items()))
def test_preset_expanded_at_construction(preset, expanded):
    assert CronMixin(expression=preset, timezone="UTC").expression == expanded


def test_validate_rejects_invalid_expression():
    with pytest.raises(AirflowTimetableInvalid):
        CronMixin(expression="not a cron", timezone="UTC").validate()
