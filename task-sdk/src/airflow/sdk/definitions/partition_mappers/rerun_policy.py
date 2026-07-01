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
    How a rollup reacts when an upstream partition is re-emitted after its downstream window already fired.

    A rollup fires its downstream Dag run once the window is satisfied (e.g. a
    monthly rollup fires once all of March's daily partitions arrive). If an
    upstream partition that the fired window already consumed is later cleared
    and re-run, a fresh asset event arrives for an already-materialized window.
    This policy decides what the scheduler does with it.

    ``HOLD`` (default): queue a provisional run that waits for the *entire*
    window to re-materialize before firing again. A single re-run does not
    re-fire; only a full recompute of the window does. This is the historical
    behavior of a rollup before this policy existed, so it is the default to keep
    existing Dags unchanged.

    ``REFRESH``: re-fire the downstream Dag run so it reprocesses with the
    corrected upstream data. The rest of the window is still materialized, so the
    refresh run fires immediately rather than waiting for the whole window to
    re-arrive. This mirrors how a non-partitioned asset-triggered Dag re-runs on
    every new asset event.

    ``IGNORE``: drop the late upstream event. The downstream Dag run is not
    re-fired and no provisional run is queued.
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
