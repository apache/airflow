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

from airflow.typing_compat import Protocol


class RunStrategy(Protocol):
    def calculate_run_after(self, last_automated_data_interval, restriction):
        raise NotImplementedError()


class IntervalStrategy(Protocol):
    def calculate_interval(self, last_automated_data_interval, restriction, run_after):
        raise NotImplementedError()


class ManualStrategy(Protocol):
    def calculate_manual_run(self, run_after, run_strategy, interval_strategy):
        raise NotImplementedError()
