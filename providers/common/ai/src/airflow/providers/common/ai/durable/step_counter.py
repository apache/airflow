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
"""Shared step counter for durable execution caching."""

from __future__ import annotations


class DurableStepCounter:
    """
    Monotonically increasing counter shared between CachingModel and CachingToolset.

    Each model call and tool call increments the counter. The step index
    is used as the cache key, ensuring deterministic replay on retry.
    """

    def __init__(self) -> None:
        self._step: int = 0
        self.replayed_model: int = 0
        self.replayed_tool: int = 0
        self.cached_model: int = 0
        self.cached_tool: int = 0

    def next_step(self) -> int:
        """Return the current step and advance the counter."""
        step = self._step
        self._step += 1
        return step

    @property
    def total_steps(self) -> int:
        """Total number of steps executed so far."""
        return self._step
