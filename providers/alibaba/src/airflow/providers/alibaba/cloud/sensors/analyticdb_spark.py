#
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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.alibaba.cloud.hooks.analyticdb_spark import AnalyticDBSparkHook, AppState
from airflow.providers.common.compat.sdk import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AnalyticDBSparkSensor(BaseSensorOperator):
    """
    Monitor a AnalyticDB Spark session for termination.

    :param app_id: identifier of the monitored app depends on the option that's being modified.
    :param adb_spark_conn_id: reference to a pre-defined ADB Spark connection.
    :param region: AnalyticDB MySQL region you want to submit spark application.
    """

    template_fields: Sequence[str] = ("app_id",)

    def __init__(
        self,
        *,
        app_id: str,
        adb_spark_conn_id: str = "adb_spark_default",
        region: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.app_id = app_id
        self._region = region
        self._adb_spark_conn_id = adb_spark_conn_id

    @cached_property
    def hook(self) -> AnalyticDBSparkHook:
        """Get valid hook."""
        return AnalyticDBSparkHook(adb_spark_conn_id=self._adb_spark_conn_id, region=self._region)

    def poke(self, context: Context) -> bool:
        app_id = self.app_id

        state = self.hook.get_spark_state(app_id)
        return AppState(state) in AnalyticDBSparkHook.TERMINAL_STATES
