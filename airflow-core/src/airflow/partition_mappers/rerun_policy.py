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

from enum import Enum


class RerunPolicy(str, Enum):
    """
    Core-side mirror of the SDK :class:`airflow.sdk.RerunPolicy`.

    Decides what the scheduler does when an upstream partition is cleared and
    re-run after a rollup's downstream window has already fired. See the SDK
    class for the authoring-facing documentation; the two are serialized by
    string value so they round-trip across the Dag-parse / scheduler boundary.

    ``HOLD`` (default): queue a provisional run that waits for the entire window
    to re-materialize before firing again. This is the historical behavior, so it
    is the default to keep existing Dags unchanged.

    ``REFRESH``: re-fire the downstream Dag run immediately so it reprocesses with
    the corrected upstream data.

    ``IGNORE``: drop the late upstream event; do not re-fire.
    """

    REFRESH = "refresh"
    HOLD = "hold"
    IGNORE = "ignore"

    @property
    def fires_immediately(self) -> bool:
        """Whether a provisional run stamped with this policy fires at once, skipping the wait policy."""
        return self is RerunPolicy.REFRESH

    @property
    def drops_event(self) -> bool:
        """Whether a re-arriving event for an already-fired window is dropped without queuing a run."""
        return self is RerunPolicy.IGNORE
