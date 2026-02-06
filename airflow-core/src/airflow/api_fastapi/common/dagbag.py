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

import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from airflow.configuration import conf
from airflow.models.dagbag import DBDagBag

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.serialization.definitions.dag import SerializedDAG

log = logging.getLogger(__name__)


def create_dag_bag() -> DBDagBag:
    """Create DagBag with configurable LRU+TTL caching for API server usage."""
    cache_size = conf.getint("api", "dag_cache_size", fallback=64)
    cache_ttl_config = conf.getint("api", "dag_cache_ttl", fallback=3600)

    if cache_size < 0:
        log.warning("dag_cache_size must be >= 0, disabling cache")
        cache_size = 0
    if cache_ttl_config < 0:
        log.warning("dag_cache_ttl must be >= 0, disabling TTL")
        cache_ttl_config = 0

    # Disable caching if cache_size is 0
    if cache_size <= 0:
        return DBDagBag(cache_size=0)

    # Disable TTL if cache_ttl is 0
    cache_ttl: int | None = cache_ttl_config if cache_ttl_config > 0 else None

    return DBDagBag(cache_size=cache_size, cache_ttl=cache_ttl)


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
    dag: SerializedDAG | None = None
    if dag_run:
        dag = dag_bag.get_dag_for_run(dag_run, session=session)
    elif dag_id:
        dag = dag_bag.get_latest_version_of_dag(dag_id, session=session)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Dag with ID: `{dag_id}` was not found")
    return dag


DagBagDep = Annotated[DBDagBag, Depends(dag_bag_from_app)]
