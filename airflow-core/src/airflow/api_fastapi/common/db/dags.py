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

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import aliased, joinedload, selectinload

from airflow.api_fastapi.common.db.common import (
    apply_filters_to_select,
)
from airflow.api_fastapi.common.parameters import BaseParam, RangeFilter, SortParam
from airflow.configuration import conf
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun

if TYPE_CHECKING:
    from sqlalchemy.orm.strategy_options import _AbstractLoad
    from sqlalchemy.sql import Select


def generate_dag_with_latest_run_query(
    max_run_filters: list[BaseParam], order_by: SortParam, *, dag_ids: set[str] | None = None
) -> Select:
    """
    Generate a query to fetch Dags with their latest run.

    :param max_run_filters: List of filters to apply to the latest run
    :param order_by: Sort parameter for ordering results
    :param dag_ids: Optional set of Dag IDs to limit the query to. When provided, both the main
        Dag query and the subquery for finding the latest runs will be filtered to
        only these Dag IDs, improving performance when users have limited Dag access.
    :return: SQLAlchemy Select statement
    """
    query = select(DagModel).options(selectinload(DagModel.tags))

    # Filter main query by dag_ids if provided
    if dag_ids is not None:
        query = query.where(DagModel.dag_id.in_(dag_ids or set()))

    latest_run = aliased(DagRun, name="latest_run")
    latest_run_id = (
        select(latest_run.id)
        .where(latest_run.dag_id == DagModel.dag_id)
        .order_by(latest_run.run_after.desc(), latest_run.id.desc())
        .limit(1)
        .scalar_subquery()
    )

    # Also filter the subquery for finding latest runs
    latest_run_id_query_stmt = select(DagModel.dag_id, latest_run_id.label("latest_dag_run_id"))
    if dag_ids is not None:
        latest_run_id_query_stmt = latest_run_id_query_stmt.where(DagModel.dag_id.in_(dag_ids or set()))
    latest_run_id_query = latest_run_id_query_stmt.subquery(name="latest_run_ids")

    has_max_run_filter = False

    for max_run_filter in max_run_filters:
        if isinstance(max_run_filter, RangeFilter):
            if max_run_filter.is_active():
                has_max_run_filter = True
                break
        if max_run_filter.value:
            has_max_run_filter = True
            break

    requested_order_by_set = set(order_by.value) if order_by.value is not None else set()
    dag_run_order_by_set = {
        "last_run_state",
        "-last_run_state",
        "last_run_start_date",
        "-last_run_start_date",
        "last_run_run_after",
        "-last_run_run_after",
    }

    if has_max_run_filter or (requested_order_by_set & dag_run_order_by_set):
        query = query.join(
            latest_run_id_query,
            DagModel.dag_id == latest_run_id_query.c.dag_id,
            isouter=True,
        ).join(DagRun, DagRun.id == latest_run_id_query.c.latest_dag_run_id, isouter=True)

    if has_max_run_filter:
        query = apply_filters_to_select(
            statement=query,
            filters=max_run_filters,
        )

    return query


def eager_load_teams(*path: Any) -> tuple[_AbstractLoad, ...]:
    """
    Eager loading options for the team owning a Dag, for serializing ``team_name``.

    ``DagModel.bundle`` is ``lazy="raise"``, so any endpoint whose response exposes
    ``team_name`` must apply these options. Nothing is loaded when multi-team mode is
    off: :attr:`DagModel.team_name` short-circuits to ``None`` without touching the
    relationship, so single-team deployments pay for no extra join.

    :param path: relationship attributes leading to ``DagModel``, empty when selecting it
        directly. For example, ``eager_load_teams(DagRun.dag_model)`` for a Dag run query.
    """
    if not conf.getboolean("core", "multi_team"):
        return ()

    loader: Any = None
    for attribute in path:
        loader = joinedload(attribute) if loader is None else loader.joinedload(attribute)
    bundle_loader = joinedload(DagModel.bundle) if loader is None else loader.joinedload(DagModel.bundle)
    return (bundle_loader.selectinload(DagBundleModel.teams),)
