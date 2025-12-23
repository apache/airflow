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

from collections import defaultdict
from typing import TYPE_CHECKING

import airflow.executors.executor_loader as executor_loader

if TYPE_CHECKING:
    from airflow.executors.executor_utils import ExecutorName


def clean_executor_loader_module():
    """Clean the executor_loader state, as it stores global variables in the module, causing side effects for some tests."""
    executor_loader._alias_to_executors_per_team: dict[str | None, dict[str, ExecutorName]] = defaultdict(
        dict
    )
    executor_loader._module_to_executors_per_team: dict[str | None, dict[str, ExecutorName]] = defaultdict(
        dict
    )
    executor_loader._classname_to_executors_per_team: dict[str | None, dict[str, ExecutorName]] = defaultdict(
        dict
    )
    executor_loader._team_name_to_executors: dict[str | None, list[ExecutorName]] = defaultdict(list)
    executor_loader._executor_names: list[ExecutorName] = []
