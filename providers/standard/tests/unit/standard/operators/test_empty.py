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

import pytest

from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS


class TestEmptyOperator:
    def test_execute_is_a_no_op(self):
        assert EmptyOperator(task_id="empty").execute(context={}) is None

    def test_is_marked_as_empty_operator(self):
        assert EmptyOperator.inherits_from_empty_operator is True
        assert EmptyOperator(task_id="empty").inherits_from_empty_operator is True

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="TaskInstance.is_task_schedulable added in 3.2")
    @pytest.mark.parametrize(
        ("extra_kwargs", "schedulable"),
        [
            pytest.param({}, False, id="plain-empty-task-is-short-circuited"),
            pytest.param(
                {"on_execute_callback": [lambda ctx: None]}, True, id="execute-callback-forces-scheduling"
            ),
            pytest.param(
                {"on_success_callback": [lambda ctx: None]}, True, id="success-callback-forces-scheduling"
            ),
            pytest.param({"outlets": [object()]}, True, id="outlets-force-scheduling"),
            pytest.param({"inlets": [object()]}, True, id="inlets-force-scheduling"),
        ],
    )
    def test_scheduler_short_circuit_contract(self, extra_kwargs, schedulable):
        """A trivial EmptyOperator is marked success without being scheduled; side effects opt out."""
        from airflow.models.taskinstance import TaskInstance

        task = EmptyOperator(task_id="empty", **extra_kwargs)

        assert TaskInstance.is_task_schedulable(task) is schedulable
