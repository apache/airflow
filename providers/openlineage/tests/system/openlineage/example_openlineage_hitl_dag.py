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
"""
DAG with all four HITL operator variants (requires Airflow 3.1+).

It checks:
    - AirflowJobFacet.tasks[task_id] includes hitl_summary with subject, options, defaults
    - task START event airflow run facet includes hitl_summary attribute
    - task COMPLETE event is emitted after response_timeout auto-applies defaults
    - ApprovalOperator COMPLETE event has hitl_summary.approved
    - HITLBranchOperator COMPLETE event has hitl_summary.branches_to_execute
    - HITLEntryOperator and base HITLOperator emit correct events
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLBranchOperator,
    HITLEntryOperator,
    HITLOperator,
)
from airflow.sdk.definitions.param import Param
from airflow.utils.trigger_rule import TriggerRule

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_hitl_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    # Base HITLOperator — auto-approves "Yes" after timeout;
    # assigned_users exercises non-null serialization of that field
    base_hitl = HITLOperator(
        task_id="base_hitl",
        subject="Base HITL subject",
        body="Base HITL body for OpenLineage system test.",
        options=["Yes", "No"],
        defaults=["Yes"],
        response_timeout=timedelta(seconds=2),
        assigned_users=[{"id": "user-001", "name": "Test User"}],
    )

    # ApprovalOperator — auto-approves after timeout;
    # assigned_users tests serialization on ApprovalOperator specifically
    approval = ApprovalOperator(
        task_id="approval",
        subject="Approval subject",
        body="Approval body for OpenLineage system test.",
        defaults=["Approve"],
        response_timeout=timedelta(seconds=2),
        assigned_users=[{"id": "user-002", "name": "Approver"}],
    )

    # HITLBranchOperator — routes to path_a_task after timeout via default;
    # options_mapping exercises non-empty mapping serialization
    branch_hitl = HITLBranchOperator(
        task_id="branch_hitl",
        subject="Branch HITL subject",
        body="Branch HITL body — routes to path_a.",
        options=["Some task", "Another task"],
        defaults=["Some task"],
        response_timeout=timedelta(seconds=2),
        assigned_users=[{"id": "user-003", "name": "Branch User"}],
        options_mapping={"Some task": "path_a_task", "Another task": "path_b_task"},
    )
    path_a_task = BashOperator(task_id="path_a_task", bash_command="exit 0;")
    path_b_task = BashOperator(task_id="path_b_task", bash_command="exit 0;")

    # HITLEntryOperator — accepts user input form, auto-completes after timeout;
    # multiple=True exercises the non-default value of that field;
    # params exercises serialized_params serialization (source="task" required for filter)
    entry_hitl = HITLEntryOperator(
        task_id="entry_hitl",
        subject="Entry HITL subject",
        body="Entry HITL body for OpenLineage system test.",
        defaults=["OK"],
        multiple=True,
        response_timeout=timedelta(seconds=2),
        trigger_rule=TriggerRule.ALL_DONE,
        assigned_users=[{"id": "user-004", "name": "Entry User"}],
        params={
            "note": Param(
                "default note",
                description="Optional feedback note",
                source="task",
                type="string",
            )
        },
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        # HITL operators defer at least once, causing two START events (initial run + resume).
        event_count_assertions={
            f"{DAG_ID}.base_hitl.event.start": ">=2",
            f"{DAG_ID}.approval.event.start": ">=2",
            f"{DAG_ID}.branch_hitl.event.start": ">=2",
            f"{DAG_ID}.entry_hitl.event.start": ">=2",
        },
    )

    base_hitl >> approval >> branch_hitl >> [path_a_task, path_b_task]
    [path_a_task, path_b_task] >> entry_hitl >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
