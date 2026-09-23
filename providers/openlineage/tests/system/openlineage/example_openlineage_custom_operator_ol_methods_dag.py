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
DAG with custom operators implementing every non-empty subset of OpenLineage lifecycle hooks.

It checks:
    - on_start only: COMPLETE event reuses on_start result (DefaultExtractor fallback)
    - on_complete only: START event is empty; COMPLETE carries custom facets
    - on_failure only: START and COMPLETE are empty on the happy path
    - on_start + on_complete: each event carries its own independent facets
    - on_start + on_failure: COMPLETE falls back to on_start (no on_complete)
    - on_complete + on_failure: START is empty; COMPLETE uses on_complete
    - all three: START uses on_start, COMPLETE uses on_complete (on_failure ignored on success)
    - run_facets, job_facets, input dataset facets, and output dataset facets are preserved
      for each method and appear only in the events driven by that method
"""

from __future__ import annotations

from datetime import datetime

from openlineage.client.event_v2 import InputDataset, OutputDataset

from airflow import DAG
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.openlineage.extractors.base import OperatorLineage

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class OnlyStartOp(BaseOperator):
    def execute(self, context):
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"only-start-run": {"key": "value"}},
            job_facets={"only-start-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://only-start",
                    name="input.csv",
                    facets={"only-start-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://only-start",
                    name="output.csv",
                    facets={"only-start-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )


class OnlyCompleteOp(BaseOperator):
    def execute(self, context):
        pass

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"only-complete-run": {"key": "value"}},
            job_facets={"only-complete-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://only-complete",
                    name="input.csv",
                    facets={"only-complete-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://only-complete",
                    name="output.csv",
                    facets={"only-complete-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )


class OnlyFailureOp(BaseOperator):
    def execute(self, context):
        pass

    def get_openlineage_facets_on_failure(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"only-failure-run": {"key": "value"}},
            job_facets={"only-failure-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://only-failure",
                    name="failure_input.csv",
                    facets={"only-failure-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://only-failure",
                    name="output.csv",
                    facets={"only-failure-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )


class StartCompleteOp(BaseOperator):
    def execute(self, context):
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"sc-start-run": {"key": "value"}},
            job_facets={"sc-start-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://start-complete",
                    name="start_input.csv",
                    facets={"sc-start-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://start-complete",
                    name="start_output.csv",
                    facets={"sc-start-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"sc-complete-run": {"key": "value"}},
            job_facets={"sc-complete-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://start-complete",
                    name="complete_input.csv",
                    facets={"sc-complete-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://start-complete",
                    name="complete_output.csv",
                    facets={"sc-complete-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )


class StartFailureOp(BaseOperator):
    def execute(self, context):
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"sf-start-run": {"key": "value"}},
            job_facets={"sf-start-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://start-failure",
                    name="start_input.csv",
                    facets={"sf-start-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://start-failure",
                    name="start_output.csv",
                    facets={"sf-start-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )

    def get_openlineage_facets_on_failure(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"sf-failure-run": {"key": "value"}},
            job_facets={"sf-failure-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://start-failure",
                    name="failure_input.csv",
                    facets={"sf-failure-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://start-failure",
                    name="failure_output.csv",
                    facets={"sf-failure-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )


class CompleteFailureOp(BaseOperator):
    def execute(self, context):
        pass

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"cf-complete-run": {"key": "value"}},
            job_facets={"cf-complete-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://complete-failure",
                    name="complete_input.csv",
                    facets={"cf-complete-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://complete-failure",
                    name="complete_output.csv",
                    facets={"cf-complete-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )

    def get_openlineage_facets_on_failure(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"cf-failure-run": {"key": "value"}},
            job_facets={"cf-failure-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://complete-failure",
                    name="failure_input.csv",
                    facets={"cf-failure-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://complete-failure",
                    name="failure_output.csv",
                    facets={"cf-failure-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )


class AllThreeOp(BaseOperator):
    def execute(self, context):
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"at-start-run": {"key": "value"}},
            job_facets={"at-start-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://all-three",
                    name="start_input.csv",
                    facets={"at-start-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://all-three",
                    name="start_output.csv",
                    facets={"at-start-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"at-complete-run": {"key": "value"}},
            job_facets={"at-complete-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://all-three",
                    name="complete_input.csv",
                    facets={"at-complete-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://all-three",
                    name="complete_output.csv",
                    facets={"at-complete-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )

    def get_openlineage_facets_on_failure(self, task_instance) -> OperatorLineage:
        return OperatorLineage(
            run_facets={"at-failure-run": {"key": "value"}},
            job_facets={"at-failure-job": {"key": "value"}},
            inputs=[
                InputDataset(
                    namespace="s3://all-three",
                    name="failure_input.csv",
                    facets={"at-failure-input-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
            outputs=[
                OutputDataset(
                    namespace="s3://all-three",
                    name="failure_output.csv",
                    facets={"at-failure-output-ds": {"key": "value"}},  # type: ignore[dict-item]
                )
            ],
        )


DAG_ID = "openlineage_custom_operator_ol_methods_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    only_start_task = OnlyStartOp(task_id="only_start_task")
    only_complete_task = OnlyCompleteOp(task_id="only_complete_task")
    only_failure_task = OnlyFailureOp(task_id="only_failure_task")
    start_complete_task = StartCompleteOp(task_id="start_complete_task")
    start_failure_task = StartFailureOp(task_id="start_failure_task")
    complete_failure_task = CompleteFailureOp(task_id="complete_failure_task")
    all_three_task = AllThreeOp(task_id="all_three_task")

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    only_start_task >> only_complete_task
    only_complete_task >> only_failure_task
    only_failure_task >> start_complete_task
    start_complete_task >> start_failure_task
    start_failure_task >> complete_failure_task
    complete_failure_task >> all_three_task
    all_three_task >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
