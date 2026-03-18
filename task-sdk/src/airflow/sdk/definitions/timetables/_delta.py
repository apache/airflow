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

from airflow.sdk.exceptions import AirflowTimetableInvalid

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta


@attrs.define
class DeltaMixin:
    """Mixin to provide interface to work with timedelta and relativedelta."""

    delta: datetime.timedelta | relativedelta

    def validate(self) -> None:
        now = datetime.datetime.now()
        if (now + self.delta) <= now:
            raise AirflowTimetableInvalid(f"schedule interval must be positive, not {self.delta!r}")
