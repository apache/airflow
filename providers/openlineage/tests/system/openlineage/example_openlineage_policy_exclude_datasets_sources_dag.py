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
DAG exercising the OpenLineage ``exclude_datasets`` control on every source of task lineage.

Every source is filtered with the same Dag list, which has two patterns. Hook and operator rules are regexes
on the names of the stand-in classes below.

The comment above each task says which rules and authoring calls apply to it, and what its events must show.

The rules are in ``AIRFLOW__OPENLINEAGE__EMISSION_POLICY`` of ``airflow-e2e-tests/docker/openlineage.yml``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from openlineage.client.event_v2 import Dataset

from airflow import DAG
from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.common.sql.hooks.lineage import SqlJobHookLineageExtra
from airflow.providers.openlineage.api import emit_dataset_lineage, emit_query_lineage
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.sqlparser import DatabaseInfo
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

SQL_NAMESPACE = "postgres://exclude-datasets:5432"
SQL_DAG = "INSERT INTO sql_keep_out SELECT a FROM sql_keep_in JOIN sql_excluded_in ON sql_keep_in.a = sql_excluded_in.a"
SQL_DAG_EXCLUDED_OUT = "INSERT INTO sql_excluded_out SELECT a FROM sql_keep_in"
SQL_HOOK_RULE = (
    "INSERT INTO sql_keep_out SELECT a FROM sql_hook_excluded_in "
    "JOIN sql_excluded_in ON sql_hook_excluded_in.a = sql_excluded_in.a"
)
SQL_ALL_EXCLUDED = "SELECT a FROM sql_excluded_in"
SQL_KEPT = "SELECT a FROM sql_keep_in"
SQL_DROPPED = "INSERT INTO sql_keep_out SELECT a FROM sql_keep_in"


class QuietHook:
    """Hook that no rule is scoped to."""


class SourceHook:
    """Hook with a Dag + hook rule."""


class SqlHook:
    """
    Hook reporting SQL hook lineage.

    It has what emit_lineage_from_sql_extras asks a SQL hook for, and never connects to a database.
    """

    conn_name_attr = "conn_id"
    conn_id = "exclude_datasets_sql"

    def get_connection(self, conn_id: str) -> None:
        return None

    def get_openlineage_database_info(self, connection: Any) -> DatabaseInfo:
        return DatabaseInfo(scheme="postgres", authority="exclude-datasets:5432", database="exclude_db")

    def get_openlineage_database_dialect(self, connection: Any) -> str:
        return "generic"

    def get_openlineage_default_schema(self) -> str:
        return "public"


class SqlRuledHook(SqlHook):
    """SQL hook with a Dag + hook rule."""


class SqlDroppedHook(SqlHook):
    """SQL hook whose datasets a Dag + hook rule with ``hook_lineage: false`` drops."""


class ExtractorOperator(PythonOperator):
    """
    Operator whose OpenLineage extractor reports the given datasets on COMPLETE.

    :param lineage_stems: stems of the datasets the extractor returns, as inputs and as outputs
    """

    def __init__(self, *, lineage_stems: Sequence[str] = (), **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.lineage_stems = list(lineage_stems)

    def get_openlineage_facets_on_complete(self, task_instance: Any) -> OperatorLineage:
        return OperatorLineage(
            inputs=[Dataset(namespace="extractor://source", name=f"{s}_in") for s in self.lineage_stems],
            outputs=[Dataset(namespace="extractor://source", name=f"{s}_out") for s in self.lineage_stems],
        )


def _register_stems(hook: object, stems: Sequence[str]):
    collector = get_hook_lineage_collector()
    for stem in stems:
        collector.add_input_asset(context=hook, uri=f"file://host1/{stem}_in.txt")
        collector.add_output_asset(context=hook, uri=f"file://host1/{stem}_out.txt")


def _register_sql(hook: object, *statements: str):
    collector = get_hook_lineage_collector()
    for statement in statements:
        collector.add_extra(
            context=hook,
            key=SqlJobHookLineageExtra.KEY.value,
            value={SqlJobHookLineageExtra.VALUE__SQL_STATEMENT.value: statement},
        )


def _inlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_in.txt") for stem in stems]


def _outlets(stems: Sequence[str]) -> list[Asset]:
    return [Asset(uri=f"file://host1/{stem}_out.txt") for stem in stems]


def _do_nothing():
    pass


def _extractor_all_excluded():
    _register_stems(QuietHook(), ["keep"])


def _extractor_empty_hook():
    _register_stems(QuietHook(), ["dag_excluded", "keep"])


def _hook_scoped():
    _register_stems(SourceHook(), ["hook_scoped", "dag_excluded", "keep"])


def _sql_dag():
    _register_sql(SqlHook(), SQL_DAG, SQL_DAG_EXCLUDED_OUT)


def _sql_hook_rule():
    _register_sql(SqlRuledHook(), SQL_HOOK_RULE)


def _sql_all_excluded():
    _register_sql(SqlHook(), SQL_ALL_EXCLUDED, SQL_KEPT)


def _sql_dropped():
    _register_sql(SqlDroppedHook(), SQL_DROPPED)
    _register_sql(SqlHook(), SQL_DROPPED)


def _emit_dataset():
    stems = ["dag_excluded", "hook_scoped", "keep"]
    emit_dataset_lineage(
        inputs=[Dataset(namespace="emit://source", name=f"{s}_in") for s in stems],
        outputs=[Dataset(namespace="emit://source", name=f"{s}_out") for s in stems],
        raise_on_error=True,
    )


def _emit_dataset_all_excluded():
    emit_dataset_lineage(
        inputs=[Dataset(namespace="emit://source", name="dag_excluded_in")],
        outputs=[Dataset(namespace="emit://source", name="dag_excluded_out")],
        raise_on_error=True,
    )
    emit_dataset_lineage(
        inputs=[Dataset(namespace="emit://source", name="keep_in")],
        outputs=[Dataset(namespace="emit://source", name="keep_out")],
        raise_on_error=True,
    )


def _emit_query():
    emit_query_lineage(
        inputs=[
            Dataset(namespace="emit://source", name="dag_excluded_in"),
            Dataset(namespace="emit://source", name="keep_in"),
        ],
        outputs=[
            Dataset(namespace="emit://source", name="dag_excluded_out"),
            Dataset(namespace="emit://source", name="keep_out"),
        ],
        query_text=SQL_DAG,
        query_source_namespace=SQL_NAMESPACE,
        raise_on_error=True,
    )


def _emit_query_all_excluded():
    emit_query_lineage(
        inputs=[Dataset(namespace="emit://source", name="dag_excluded_in")],
        outputs=[Dataset(namespace="emit://source", name="dag_excluded_out")],
        raise_on_error=True,
    )
    emit_query_lineage(
        inputs=[Dataset(namespace="emit://source", name="keep_in")],
        outputs=[Dataset(namespace="emit://source", name="keep_out")],
        raise_on_error=True,
    )


def _failed():
    _register_stems(QuietHook(), ["dag_excluded", "keep"])
    raise ValueError("Intentional failure to check the FAIL event")


DAG_ID = "openlineage_policy_exclude_datasets_sources_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    # The Dag list filters the declared inlets and outlets, the SourceHook rule does not apply to them:
    # COMPLETE drops dag_excluded and keeps hook_scoped and keep.
    t_declared = PythonOperator(
        task_id="t_declared",
        python_callable=_do_nothing,
        inlets=_inlets(["dag_excluded", "hook_scoped", "keep"]),
        outlets=_outlets(["dag_excluded", "hook_scoped", "keep"]),
    )
    # The Dag list filters the extractor datasets, which have no hook: COMPLETE drops dag_excluded and keeps
    # hook_scoped and keep.
    t_extractor = ExtractorOperator(
        task_id="t_extractor",
        python_callable=_do_nothing,
        lineage_stems=["dag_excluded", "hook_scoped", "keep"],
    )
    # The extractor reports only dag_excluded, which the Dag list drops, and the hook lineage dataset (keep)
    # is not used instead: COMPLETE has no datasets.
    t_extractor_all_excluded = ExtractorOperator(
        task_id="t_extractor_all_excluded",
        python_callable=_extractor_all_excluded,
        lineage_stems=["dag_excluded"],
    )
    # The extractor reports nothing, so the hook lineage is used and filtered: COMPLETE drops dag_excluded and
    # keeps keep.
    t_extractor_empty_hook = ExtractorOperator(
        task_id="t_extractor_empty_hook", python_callable=_extractor_empty_hook
    )
    # The extractor and the hook lineage report nothing, so the declared datasets are used and filtered:
    # COMPLETE keeps keep.
    t_extractor_empty_declared = ExtractorOperator(
        task_id="t_extractor_empty_declared",
        python_callable=_do_nothing,
        inlets=_inlets(["dag_excluded", "keep"]),
        outlets=_outlets(["dag_excluded", "keep"]),
    )
    # The Dag + SourceHook list (hook_scoped) replaces the Dag list: COMPLETE drops hook_scoped and keeps
    # dag_excluded and keep.
    t_hook_scoped = PythonOperator(task_id="t_hook_scoped", python_callable=_hook_scoped)
    # Two SQL hook lineage queries, filtered by the Dag list: both query events are emitted, without the
    # sql_excluded tables.
    t_sql_dag = PythonOperator(task_id="t_sql_dag", python_callable=_sql_dag)
    # The Dag + SqlRuledHook list (sql_hook_excluded) replaces the Dag list: the query event drops
    # sql_hook_excluded_in and keeps sql_excluded_in.
    t_sql_hook_rule = PythonOperator(task_id="t_sql_hook_rule", python_callable=_sql_hook_rule)
    # The first query reads only sql_excluded_in and is still emitted, without datasets. The second query
    # keeps sql_keep_in.
    t_sql_all_excluded = PythonOperator(task_id="t_sql_all_excluded", python_callable=_sql_all_excluded)
    # The Dag + SqlDroppedHook rule hook_lineage: false drops that hook's datasets, not its query event. The
    # same query from SqlHook keeps its datasets.
    t_sql_dropped = PythonOperator(task_id="t_sql_dropped", python_callable=_sql_dropped)
    # emit_dataset_lineage is filtered by the Dag list: its RUNNING event drops dag_excluded and keeps
    # hook_scoped and keep.
    t_emit_dataset = PythonOperator(task_id="t_emit_dataset", python_callable=_emit_dataset)
    # The first emit_dataset_lineage call has only excluded datasets and emits nothing. The second one (keep)
    # emits a RUNNING event.
    t_emit_dataset_all_excluded = PythonOperator(
        task_id="t_emit_dataset_all_excluded", python_callable=_emit_dataset_all_excluded
    )
    # emit_query_lineage is filtered, for the given and the parsed datasets: the query events keep keep_in and
    # sql_keep_in.
    t_emit_query = PythonOperator(task_id="t_emit_query", python_callable=_emit_query)
    # The first emit_query_lineage call has only excluded datasets and still emits its query events, without
    # datasets. The second one keeps keep.
    t_emit_query_all_excluded = PythonOperator(
        task_id="t_emit_query_all_excluded", python_callable=_emit_query_all_excluded
    )
    # Fails after registering hook lineage. The FAIL event is filtered by the Dag list: it drops dag_excluded
    # and keeps keep.
    t_failed = PythonOperator(task_id="t_failed", python_callable=_failed)
    # Not a check: runs because t_failed failed, so that check_events still runs. An EmptyOperator emits no
    # events, which emits_ol_events reports as false.
    t_after_failed = EmptyOperator(task_id="t_after_failed", trigger_rule=TriggerRule.ONE_FAILED)
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    (
        t_declared
        >> t_extractor
        >> t_extractor_all_excluded
        >> t_extractor_empty_hook
        >> t_extractor_empty_declared
        >> t_hook_scoped
        >> t_sql_dag
        >> t_sql_hook_rule
        >> t_sql_all_excluded
        >> t_sql_dropped
        >> t_emit_dataset
        >> t_emit_dataset_all_excluded
        >> t_emit_query
        >> t_emit_query_all_excluded
        >> check_events
    )
    t_failed >> t_after_failed >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
