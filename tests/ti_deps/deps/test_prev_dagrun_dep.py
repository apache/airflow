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


from unittest.mock import Mock

import pytest

from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.utils.state import State
from airflow.utils.timezone import datetime


@pytest.mark.parametrize(
    "depends_on_past, wait_for_downstream, prev_ti, context_ignore_depends_on_past, dep_met",
    [
        # If the task does not set depends_on_past, the previous dagrun should
        # be ignored, even though previous_ti would otherwise fail the dep.
        pytest.param(
            False,
            False,  # wait_for_downstream=True overrides depends_on_past=False.
            Mock(
                state=State.NONE,
                **{"are_dependents_done.return_value": False},
            ),
            False,
            True,
            id="not_depends_on_past",
        ),
        # If the context overrides depends_on_past, the dep should be met even
        # though there is no previous_ti which would normally fail the dep.
        pytest.param(
            True,
            False,
            Mock(
                state=State.SUCCESS,
                **{"are_dependents_done.return_value": True},
            ),
            True,
            True,
            id="context_ignore_depends_on_past",
        ),
        # The first task run should pass since it has no previous dagrun.
        pytest.param(True, False, None, False, True, id="first_task_run"),
        # Previous TI did not complete execution. This dep should fail.
        pytest.param(
            True,
            False,
            Mock(
                state=State.NONE,
                **{"are_dependents_done.return_value": True},
            ),
            False,
            False,
            id="prev_ti_bad_state",
        ),
        # Previous TI specified to wait for the downstream tasks of the previous
        # dagrun. It should fail this dep if the previous TI's downstream TIs
        # are not done.
        pytest.param(
            True,
            True,
            Mock(
                state=State.SUCCESS,
                **{"are_dependents_done.return_value": False},
            ),
            False,
            False,
            id="failed_wait_for_downstream",
        ),
        # All the conditions for the dep are met.
        pytest.param(
            True,
            True,
            Mock(
                state=State.SUCCESS,
                **{"are_dependents_done.return_value": True},
            ),
            False,
            True,
            id="all_met",
        ),
    ],
)
def test_dagrun_dep(
    depends_on_past,
    wait_for_downstream,
    prev_ti,
    context_ignore_depends_on_past,
    dep_met,
):
    task = BaseOperator(
        task_id="test_task",
        dag=DAG("test_dag"),
        depends_on_past=depends_on_past,
        start_date=datetime(2016, 1, 1),
        wait_for_downstream=wait_for_downstream,
    )
    if prev_ti:
        prev_dagrun = Mock(
            execution_date=datetime(2016, 1, 2),
            **{"get_task_instance.return_value": prev_ti},
        )
    else:
        prev_dagrun = None
    dagrun = Mock(
        **{
            "get_previous_scheduled_dagrun.return_value": prev_dagrun,
            "get_previous_dagrun.return_value": prev_dagrun,
        },
    )
    ti = Mock(task=task, **{"get_dagrun.return_value": dagrun})
    dep_context = DepContext(ignore_depends_on_past=context_ignore_depends_on_past)

    assert PrevDagrunDep().is_met(ti=ti, dep_context=dep_context) == dep_met
