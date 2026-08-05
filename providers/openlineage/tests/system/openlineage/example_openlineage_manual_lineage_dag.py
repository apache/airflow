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
DAG exercising the public OpenLineage manual lineage API: emit_dataset_lineage and emit_query_lineage.

It checks:
    - emit_dataset_lineage produces a RUNNING event with inputs/outputs
    - emit_dataset_lineage accepts additional_run_facets and additional_job_facets
    - datasets can carry dataset-level facets (DataQualityAssertionsDatasetFacet)
    - emit_query_lineage produces a START + COMPLETE event pair
    - query_text attaches a sql job facet
    - query_id + query_source_namespace attaches external query run facet
    - multiple emit_query_lineage calls increment the job name counter (.1, .2, .3)
    - failed query (is_successful=False) produces a FAIL event with error message run facet
    - counter resets to .1 in a new task (no cross-task spill)
    - explicit job_name= bypasses the counter
    - explicit task_instance= bypasses context resolution inside the helper
"""

from __future__ import annotations

import datetime as dt

from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import data_quality_assertions_dataset, source_code_location_job

from airflow.providers.openlineage.api import emit_dataset_lineage, emit_query_lineage

try:
    from airflow.sdk import dag, get_current_context, task
except ImportError:
    from airflow.decorators import dag, task  # type: ignore[no-redef, attr-defined]
    from airflow.operators.python import get_current_context  # type: ignore[no-redef]

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def _dataset_with_assertions(namespace: str, name: str) -> Dataset:
    return Dataset(
        namespace=namespace,
        name=name,
        facets={
            "dataQualityAssertions": data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(  # type: ignore[dict-item]
                assertions=[
                    data_quality_assertions_dataset.Assertion(
                        assertion="not_null", success=True, column="id"
                    ),
                    data_quality_assertions_dataset.Assertion(assertion="unique", success=True, column="id"),
                ],
            ),
        },
    )


DAG_ID = "openlineage_manual_lineage_dag"


@dag(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=dt.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
)
def openlineage_manual_lineage_dag():
    @task
    def datasets_minimal() -> None:
        """Minimal emit_dataset_lineage — just one input dataset."""
        emit_dataset_lineage(
            inputs=[Dataset(namespace="s3://example-bucket", name="raw/orders.csv")],
        )

    @task
    def datasets_maximal() -> None:
        """Maximal emit_dataset_lineage — every kwarg set, datasets with facets."""
        ctx = get_current_context()
        ti = ctx["task_instance"]

        emit_dataset_lineage(
            inputs=[_dataset_with_assertions("s3://example-bucket", "raw/2024/01/01/orders.csv")],
            outputs=[
                _dataset_with_assertions("snowflake://example-acct", "analytics.public.orders_enriched")
            ],
            task_instance=ti,
            additional_run_facets={"my_custom_run_facet": {"key": "value"}},  # type: ignore[dict-item]
            additional_job_facets={
                "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(
                    type="git",
                    url="https://github.com/apache/airflow",
                    repoUrl="https://github.com/apache/airflow",
                    path="providers/openlineage/tests/system/openlineage/example_openlineage_manual_lineage_dag.py",
                    version="main",
                    branch="main",
                ),
            },
            raise_on_error=True,
        )

    @task
    def query_minimal() -> None:
        """Minimal emit_query_lineage — query_id + source only."""
        emit_query_lineage(
            query_id="qid-min-1",
            query_source_namespace="snowflake://example-acct",
        )

    @task
    def query_maximal() -> None:
        """Maximal emit_query_lineage — every kwarg set."""
        ctx = get_current_context()
        ti = ctx["task_instance"]

        start = dt.datetime(2024, 5, 1, 10, 0, 0, tzinfo=dt.timezone.utc)
        end = dt.datetime(2024, 5, 1, 10, 0, 5, tzinfo=dt.timezone.utc)

        emit_query_lineage(
            query_id="qid-max-1",
            query_source_namespace="snowflake://example-acct",
            query_text=(
                "INSERT INTO analytics.public.user_events_summary "
                "SELECT user_id, COUNT(*) FROM analytics.public.user_events GROUP BY user_id"
            ),
            inputs=[
                _dataset_with_assertions("snowflake://example-acct", "analytics.public.user_events_extra")
            ],
            outputs=[
                _dataset_with_assertions("snowflake://example-acct", "analytics.public.user_events_summary")
            ],
            start_time=start,
            end_time=end,
            is_successful=True,
            default_database="ANALYTICS",
            default_schema="public",
            task_instance=ti,
            raise_on_error=True,
        )

    @task
    def query_multiple_in_one_task() -> None:
        """Three emit_query_lineage calls — counter goes .1, .2, .3; third call is FAIL."""
        emit_query_lineage(
            query_id="qid-multi-1",
            query_source_namespace="snowflake://example-acct",
            query_text="SELECT id, email FROM analytics.public.users WHERE active = true",
        )
        emit_query_lineage(
            query_id="qid-multi-2",
            query_source_namespace="snowflake://example-acct",
            query_text="SELECT * FROM analytics.public.orders WHERE created_at > '2024-01-01'",
        )
        emit_query_lineage(
            query_id="qid-multi-3",
            query_source_namespace="snowflake://example-acct",
            query_text="SELECT broken(",
            is_successful=False,
            error_message="syntax error at or near 'broken'",
        )

    @task
    def query_isolated_task() -> None:
        """Single call in a new task — counter resets to .1, proving no cross-task spill."""
        emit_query_lineage(
            query_id="qid-isolated-1",
            query_source_namespace="snowflake://example-acct",
            query_text="SELECT 1",
        )

    @task
    def query_with_explicit_job_name() -> None:
        """Explicit job_name= bypasses the counter; counter stays unset after the call."""
        emit_query_lineage(
            query_id="qid-explicit-name",
            query_source_namespace="snowflake://example-acct",
            query_text="SELECT version()",
            job_name="custom_job_name_set_by_caller",
        )

    @task
    def query_with_explicit_task_instance() -> None:
        """Explicit task_instance= bypasses context resolution inside the helper."""
        ti = get_current_context()["task_instance"]
        emit_query_lineage(
            query_id="qid-explicit-ti",
            query_source_namespace="snowflake://example-acct",
            query_text="SELECT now()",
            task_instance=ti,
        )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        event_count_assertions={
            # emit_dataset_lineage produces a RUNNING event
            f"{DAG_ID}.datasets_minimal.event.running": "==1",
            f"{DAG_ID}.datasets_maximal.event.running": "==1",
            # minimal query: 1 start + 1 complete
            f"{DAG_ID}.query_minimal.manual_query.1.event.start": "==1",
            f"{DAG_ID}.query_minimal.manual_query.1.event.complete": "==1",
            # maximal query
            f"{DAG_ID}.query_maximal.manual_query.1.event.start": "==1",
            f"{DAG_ID}.query_maximal.manual_query.1.event.complete": "==1",
            # three calls in one task: .1 and .2 complete, .3 fail
            f"{DAG_ID}.query_multiple_in_one_task.manual_query.1.event.start": "==1",
            f"{DAG_ID}.query_multiple_in_one_task.manual_query.1.event.complete": "==1",
            f"{DAG_ID}.query_multiple_in_one_task.manual_query.2.event.start": "==1",
            f"{DAG_ID}.query_multiple_in_one_task.manual_query.2.event.complete": "==1",
            f"{DAG_ID}.query_multiple_in_one_task.manual_query.3.event.start": "==1",
            f"{DAG_ID}.query_multiple_in_one_task.manual_query.3.event.fail": "==1",
            # isolated task resets counter to .1
            f"{DAG_ID}.query_isolated_task.manual_query.1.event.start": "==1",
            f"{DAG_ID}.query_isolated_task.manual_query.1.event.complete": "==1",
            # explicit job_name bypasses counter
            "custom_job_name_set_by_caller.event.start": "==1",
            "custom_job_name_set_by_caller.event.complete": "==1",
            # explicit task_instance
            f"{DAG_ID}.query_with_explicit_task_instance.manual_query.1.event.start": "==1",
            f"{DAG_ID}.query_with_explicit_task_instance.manual_query.1.event.complete": "==1",
        },
    )

    (
        datasets_minimal()
        >> datasets_maximal()
        >> query_minimal()
        >> query_maximal()
        >> query_multiple_in_one_task()
        >> query_isolated_task()
        >> query_with_explicit_job_name()
        >> query_with_explicit_task_instance()
        >> check_events
    )


openlineage_manual_lineage_dag()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
