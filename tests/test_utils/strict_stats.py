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

from statsd import StatsClient

from airflow.stats import METRICS_LIST, DummyTimer, TimerProtocol


class StrictStatsLogger(StatsClient):
    @staticmethod
    def incr(stat: str, count: int = 1, rate: int = 1, *, labels=None) -> None:
        assert any(
            stat == metric.key for metric in METRICS_LIST
        ), f"Undocumented metric: {stat}, labels={labels}"

    @staticmethod
    def decr(stat: str, count: int = 1, rate: int = 1, *, labels=None) -> None:
        assert any(
            stat == metric.key for metric in METRICS_LIST
        ), f"Undocumented metrics: {stat}, labels={labels}"

    @staticmethod
    def gauge(stat: str, value: float, rate: int = 1, delta: bool = False, *, labels=None) -> None:
        del value
        del rate
        del delta
        assert any(
            stat == metric.key for metric in METRICS_LIST
        ), f"Undocumented metrics: {stat}, labels={labels}"

    @staticmethod
    def timing(stat: str, dt, *, labels=None) -> None:
        del dt
        assert any(
            stat == metric.key for metric in METRICS_LIST
        ), f"Undocumented metrics: {stat}, labels={labels}"

    @staticmethod
    def timer(stat, *, labels=None) -> TimerProtocol:
        assert any(
            stat == metric.key for metric in METRICS_LIST
        ), f"Undocumented metrics: {stat}, labels={labels}"
        return DummyTimer()
