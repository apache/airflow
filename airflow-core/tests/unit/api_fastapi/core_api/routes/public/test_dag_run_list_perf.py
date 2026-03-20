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

from datetime import timedelta

import pytest
from sqlalchemy import select
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm.attributes import NO_VALUE

from airflow.models.dagrun import DagRun
from airflow.utils import timezone

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def _cleanup_db():
    # Keep this isolated/repeatable and aligned with other route tests.
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_serialized_dags()


def _list_stmt(dag_id: str):
    # Import here so test fails loudly if someone refactors paths.
    from airflow.api_fastapi.common.db.dag_runs import eager_load_dag_run_for_list

    return select(DagRun).where(DagRun.dag_id == dag_id).options(*eager_load_dag_run_for_list())


@pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
def test_dagrun_list_loader_does_not_eager_load_task_instances(dag_maker, session):
    """
    Regression for #62025:
    List loader must not eager load task_instances or task_instances_histories.
    """
    dag_id = "perf_dagrun_list_no_ti"

    # Create a serialized DAG + a bunch of runs with unique logical_date.
    with dag_maker(dag_id=dag_id, schedule=None, start_date=timezone.utcnow(), serialized=True):
        pass

    base = timezone.utcnow()
    for i in range(40):
        dag_maker.create_dagrun(
            run_id=f"run_{i}",
            logical_date=base + timedelta(seconds=i),
        )

    dag_maker.sync_dagbag_to_db()
    session.commit()

    stmt = _list_stmt(dag_id).limit(100)

    # 1 query to fetch dagruns + joined relationships in loader
    with assert_queries_count(1):
        runs = session.scalars(stmt).all()

    # Confirm TI/TIH were NOT eager loaded (still NO_VALUE)
    for dr in runs:
        insp = sa_inspect(dr)
        assert insp.attrs.task_instances.loaded_value is NO_VALUE
        assert insp.attrs.task_instances_histories.loaded_value is NO_VALUE


@pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
def test_dagrun_list_loader_keeps_access_to_loaded_relationships_bounded(dag_maker, session):
    """
    Ensure list loader eager loads only the small relationships needed by response serialization:
    - dag_model
    - dag_run_note
    - created_dag_version (+ bundle)
    and accessing them does not trigger extra queries.
    """
    dag_id = "perf_dagrun_list_bounded"

    with dag_maker(dag_id=dag_id, schedule=None, start_date=timezone.utcnow(), serialized=True):
        pass

    base = timezone.utcnow()
    for i in range(40):
        dag_maker.create_dagrun(
            run_id=f"run_{i}",
            logical_date=base + timedelta(seconds=i),
        )

    dag_maker.sync_dagbag_to_db()
    session.commit()

    stmt = _list_stmt(dag_id).limit(100)

    with assert_queries_count(1):
        runs = session.scalars(stmt).all()

    # Accessing these must not emit additional queries.
    with assert_queries_count(0):
        for dr in runs:
            _ = dr.dag_model
            _ = dr.dag_run_note
            # created_dag_version can be None depending on how the DagRun was created;
            # the important bit: if present, bundle access must not query.
            cdv = dr.created_dag_version
            if cdv is not None:
                _ = cdv.bundle