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

from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from airflow.models.dagbag import DBDagBag

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.serialization.definitions.dag import SerializedDAG


def create_dag_bag() -> DBDagBag:
    """Create DagBag to retrieve DAGs from the database."""
    return DBDagBag()


def dag_bag_from_app(request: Request) -> DBDagBag:
    """
    FastAPI dependency resolver that returns the shared DagBag instance from app.state.

    This ensures that all API routes using DagBag via dependency injection receive the same
    singleton instance that was initialized at app startup.
    """
    return request.app.state.dag_bag


def get_latest_version_of_dag(
    dag_bag: DBDagBag, dag_id: str, session: Session, include_reason: bool = False
) -> SerializedDAG:
    dag = dag_bag.get_latest_version_of_dag(dag_id, session=session)
    if not dag:
        if include_reason:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                detail={
                    "reason": "not_found",
                    "message": f"The Dag with ID: `{dag_id}` was not found",
                },
            )
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Dag with ID: `{dag_id}` was not found")
    return dag


def get_dag_for_run(dag_bag: DBDagBag, dag_run: DagRun, session: Session) -> SerializedDAG:
    dag = dag_bag.get_dag_for_run(dag_run, session=session)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Dag with ID: `{dag_run.dag_id}` was not found")
    return dag


def get_dag_for_run_or_latest_version(
    dag_bag: DBDagBag, dag_run: DagRun | None, dag_id: str | None, session: Session
) -> SerializedDAG:
    """
    Retrieve the serialized DAG for a specific run, or the latest version if no run is given.

    When a dag_run is provided, we prefer the exact DAG version the run was created with
    (``created_dag_version_id``) so that task group lookups, operator metadata, etc. match
    the DAG structure at the time of the run.

    This is necessary because ``get_dag_for_run`` delegates to ``_version_from_dag_run``
    which, for unversioned bundles (e.g. ``LocalDagBundle``), falls back to the *latest*
    ``DagVersion``.
    """
    dag: SerializedDAG | None = None
    if dag_run:
        if dag_run.created_dag_version_id:
            dag = dag_bag._get_dag(dag_run.created_dag_version_id, session=session)
        if not dag:
            dag = dag_bag.get_dag_for_run(dag_run, session=session)
    elif dag_id:
        dag = dag_bag.get_latest_version_of_dag(dag_id, session=session)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Dag with ID: `{dag_id}` was not found")
    return dag


DagBagDep = Annotated[DBDagBag, Depends(dag_bag_from_app)]
