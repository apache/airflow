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

import pytest

from airflow.serialization.serialized_objects import _PriorityWeightStrategyNotRegistered
from airflow.task.priority_strategy import (
    PriorityWeightStrategy,
    validate_and_load_priority_weight_strategy,
)


class UnregisteredPriorityWeightStrategy(PriorityWeightStrategy):
    def get_weight(self, ti):
        return 99


class TestValidateAndLoadPriorityWeightStrategy:
    @pytest.mark.parametrize(
        ("weight_rule", "expected_type_string"),
        [
            pytest.param("no rule", "no rule", id="unknown-string"),
            pytest.param(
                UnregisteredPriorityWeightStrategy(),
                "unit.task.test_priority_strategy.UnregisteredPriorityWeightStrategy",
                id="unregistered-instance",
            ),
        ],
    )
    def test_unregistered_strategy_hints_at_plugin_loading(self, weight_rule, expected_type_string):
        with pytest.raises(_PriorityWeightStrategyNotRegistered) as ctx:
            validate_and_load_priority_weight_strategy(weight_rule)

        # The loader has raised a plain ``ValueError`` here since #60112, and the sibling
        # ``TimetableNotRegistered`` is one too -- keep existing handlers working.
        assert isinstance(ctx.value, ValueError)
        assert str(ctx.value) == (
            f"Priority weight strategy class {expected_type_string!r} is not registered or "
            "you have a top level database access that disrupted the session. "
            "Please check the airflow best practices documentation."
        )
