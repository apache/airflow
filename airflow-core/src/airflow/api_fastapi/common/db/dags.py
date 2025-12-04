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

from typing import TYPE_CHECKING

from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from airflow.api_fastapi.common.db.common import (
    apply_filters_to_select,
)
from airflow.api_fastapi.common.parameters import BaseParam, RangeFilter, SortParam
from airflow.models import DagModel
from airflow.models.dagrun import DagRun

if TYPE_CHECKING:
    from sqlalchemy.sql import Select


def generate_dag_with_latest_run_query(
    max_run_filters: list[BaseParam], order_by: SortParam, *, dag_ids: set[str] | None = None
) -> Select:
    """
    Generate a query to fetch DAGs with their latest run.

    :param max_run_filters: List of filters to apply to the latest run
    :param order_by: Sort parameter for ordering results
    :param dag_ids: Optional set of DAG IDs to limit the query to. When provided, both the main
        DAG query and the subquery for finding the latest runs will be filtered to
        only these DAG IDs, improving performance when users have limited DAG access.
    :return: SQLAlchemy Select statement
    """
    query = select(DagModel).options(selectinload(DagModel.tags))

    # Filter main query by dag_ids if provided
    if dag_ids is not None:
        query = query.where(DagModel.dag_id.in_(dag_ids or set()))

    # Also filter the subquery for finding latest runs
    max_run_id_query_stmt = select(DagRun.dag_id, func.max(DagRun.id).label("max_dag_run_id"))
    if dag_ids is not None:
        max_run_id_query_stmt = max_run_id_query_stmt.where(DagRun.dag_id.in_(dag_ids or set()))
    max_run_id_query = max_run_id_query_stmt.group_by(DagRun.dag_id).subquery(name="mrq")

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
    dag_run_order_by_set = set(
        ["last_run_state", "last_run_start_date", "-last_run_state", "-last_run_start_date"],
    )

    if has_max_run_filter or (requested_order_by_set & dag_run_order_by_set):
        query = query.join(
            max_run_id_query,
            DagModel.dag_id == max_run_id_query.c.dag_id,
            isouter=True,
        ).join(DagRun, DagRun.id == max_run_id_query.c.max_dag_run_id, isouter=True)

    if has_max_run_filter:
        query = apply_filters_to_select(
            statement=query,
            filters=max_run_filters,
        )

    return query
