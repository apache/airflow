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

from typing import TYPE_CHECKING

import attrs
from croniter import CroniterBadCronError, CroniterBadDateError, croniter

from airflow.sdk.exceptions import AirflowTimetableInvalid

if TYPE_CHECKING:
    from pendulum.tz.timezone import FixedTimezone, Timezone


# NOTE: Keep in sync with cron_presets in airflow-core/src/airflow/utils/dates.py
# Core cannot be imported from the SDK, so both dicts must be updated together.
CRON_PRESETS: dict[str, str] = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@quarterly": "0 0 1 */3 *",
    "@yearly": "0 0 1 1 *",
}


@attrs.define
class CronMixin:
    """Mixin to provide interface to work with croniter."""

    expression: str
    timezone: str | Timezone | FixedTimezone

    def __attrs_post_init__(self) -> None:
        # Resolve preset aliases (e.g. "@quarterly") to their cron expressions
        # in-place. After this point the original preset string is lost;
        # attrs.evolve, equality, and serialisation all see the resolved form.
        self.expression = CRON_PRESETS.get(self.expression, self.expression)

    def validate(self) -> None:
        try:
            croniter(self.expression)
        except (CroniterBadCronError, CroniterBadDateError) as e:
            raise AirflowTimetableInvalid(str(e))
