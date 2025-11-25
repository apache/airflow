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

from airflow._shared.timezones.timezone import convert_to_utc

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta
    from pendulum import DateTime


class DeltaMixin:
    """Mixin to provide interface to work with timedelta and relativedelta."""

    def __init__(self, delta: datetime.timedelta | relativedelta) -> None:
        self._delta = delta

    @property
    def summary(self) -> str:
        return str(self._delta)

    def _get_next(self, current: DateTime) -> DateTime:
        return convert_to_utc(current + self._delta)

    def _get_prev(self, current: DateTime) -> DateTime:
        return convert_to_utc(current - self._delta)

    def _align_to_next(self, current: DateTime) -> DateTime:
        return current

    def _align_to_prev(self, current: DateTime) -> DateTime:
        return current
