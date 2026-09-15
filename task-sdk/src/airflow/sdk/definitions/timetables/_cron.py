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
    """
    Mixin to provide interface to work with croniter.

    Optionally applies a deterministic, per-DAG jitter: when ``max_jitter`` is set, every
    cron boundary is shifted by a fixed offset derived from ``seed`` and spread across
    ``[0, max_jitter)``, so DAGs sharing a cron expression no longer all fire at the same
    instant. The offset is computed scheduler-side; this class only carries and validates
    the settings.

    :param seed: stable, unique-per-DAG string the offset is derived from (the DAG id is a
        natural choice). Must be non-empty whenever ``max_jitter`` is set.
    :param max_jitter: upper bound of the jitter window; the offset falls in
        ``[0, max_jitter)``. Defaults to zero, i.e. no jitter.
    """

    expression: str
    timezone: str | Timezone | FixedTimezone
    seed: str = attrs.field(kw_only=True, default="")
    max_jitter: datetime.timedelta = attrs.field(kw_only=True, default=datetime.timedelta())

    def __attrs_post_init__(self) -> None:
        # Resolve preset aliases (e.g. "@quarterly") to their cron expressions
        # in-place. After this point the original preset string is lost;
        # attrs.evolve, equality, and serialisation all see the resolved form.
        self.expression = CRON_PRESETS.get(self.expression, self.expression)
        if self.max_jitter > datetime.timedelta(0) and not self.seed:
            raise ValueError("seed must be a non-empty, unique-per-DAG string when max_jitter > 0")

    def validate(self) -> None:
        try:
            croniter(self.expression)
        except (CroniterBadCronError, CroniterBadDateError) as e:
            raise AirflowTimetableInvalid(str(e))
