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

from contextlib import AbstractContextManager, ExitStack, nullcontext
from typing import TYPE_CHECKING, Any, cast

from airflow.configuration import conf
from airflow.stats import Stats

if TYPE_CHECKING:
    from airflow.metrics.protocols import DeltaType


class DualStatsManager:
    """Helper class to abstract enabling/disabling the export of metrics with legacy names."""

    export_legacy_names = conf.getboolean("metrics", "legacy_names_on")

    @classmethod
    def incr(
        cls,
        legacy_stat: str,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        if cls.export_legacy_names:
            Stats.incr(legacy_stat, count, rate, tags=tags)

        Stats.incr(stat, count, rate, tags=tags)

    @classmethod
    def decr(
        cls,
        legacy_stat: str,
        stat: str,
        count: int = 1,
        rate: float = 1,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        if cls.export_legacy_names:
            Stats.decr(legacy_stat, count, rate, tags=tags)

        Stats.decr(stat, count, rate, tags=tags)

    @classmethod
    def gauge(
        cls,
        legacy_stat: str,
        stat: str,
        value: int | float,
        rate: float = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        if cls.export_legacy_names:
            Stats.gauge(legacy_stat, value, rate, delta, tags=tags)

        Stats.gauge(stat, value, rate, delta, tags=tags)

    @classmethod
    def timing(
        cls,
        legacy_stat: str,
        stat: str,
        dt: DeltaType,
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        if cls.export_legacy_names:
            Stats.timing(legacy_stat, dt, tags=tags)

        Stats.timing(stat, dt, tags=tags)

    @classmethod
    def timer(
        cls,
        legacy_stat: str | None = None,
        stat: str | None = None,
        tags: dict[str, str] | None = None,
        **kwargs,
    ):
        # Used with a context manager.
        stack = ExitStack()
        ctx_mg1: AbstractContextManager[Any] = (
            cast("AbstractContextManager[Any]", Stats.timer(legacy_stat, tags, **kwargs))
            if cls.export_legacy_names
            else nullcontext()
        )

        stack.enter_context(ctx_mg1)
        stack.enter_context(Stats.timer(stat, tags, **kwargs))

        return stack
