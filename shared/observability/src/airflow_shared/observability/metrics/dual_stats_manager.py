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

from typing import Any

from .stats import decr, gauge, incr, timer, timing


class DualStatsManager:
    """
    Backward-compatibility shim for providers released against the old DualStatsManager API.

    Merges ``extra_tags`` into ``tags`` and delegates to the module-level stats functions.
    The Edge3 provider was using the DualStatsManager in the released version 3.4.0,
    and it had the following version check `if AIRFLOW_V_3_2_PLUS:`.
    This shim can be removed, only after support for Edge3 3.4.0 is dropped.
    """

    @staticmethod
    def incr(
        stat: str,
        count: int | None = None,
        rate: int | float | None = None,
        *,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
    ) -> None:
        incr(stat, count, rate, tags={**(tags or {}), **(extra_tags or {})} or None)

    @staticmethod
    def decr(
        stat: str,
        count: int | None = None,
        rate: int | float | None = None,
        *,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
    ) -> None:
        decr(stat, count, rate, tags={**(tags or {}), **(extra_tags or {})} or None)

    @staticmethod
    def gauge(
        stat: str,
        value: float,
        rate: int | float | None = None,
        delta: bool | None = None,
        *,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
    ) -> None:
        gauge(stat, value, rate, delta, tags={**(tags or {}), **(extra_tags or {})} or None)

    @staticmethod
    def timing(
        stat: str,
        dt: Any,
        *,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
    ) -> None:
        timing(stat, dt, tags={**(tags or {}), **(extra_tags or {})} or None)

    @staticmethod
    def timer(
        stat: str,
        tags: dict[str, Any] | None = None,
        extra_tags: dict[str, Any] | None = None,
        **kwargs,
    ):
        return timer(stat, tags={**(tags or {}), **(extra_tags or {})} or None, **kwargs)
