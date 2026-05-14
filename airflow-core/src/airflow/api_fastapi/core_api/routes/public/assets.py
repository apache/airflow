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

from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, cast

from fastapi import Depends, HTTPException, status
from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import joinedload, subqueryload

from airflow._shared.timezones import timezone
from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    BaseParam,
    FilterParam,
    OptionalDateTimeQuery,
    QueryAssetAliasNamePatternSearch,
    QueryAssetAliasNamePrefixPatternSearch,
    QueryAssetDagIdPatternSearch,
    QueryAssetNamePatternSearch,
    QueryAssetNamePrefixPatternSearch,
    QueryLimit,
    QueryOffset,
    QueryUriPatternSearch,
    QueryUriPrefixPatternSearch,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.assets import (
    AssetAliasCollectionResponse,
    AssetAliasResponse,
    AssetCollectionResponse,
    AssetEventCollectionResponse,
    AssetEventResponse,
    AssetLineageEdge,
    AssetLineageGraphResponse,
    AssetLineageNode,
    AssetResponse,
    ColumnLineageSource,
    CreateAssetEventsBody,
    MaterializeAssetBody,
    QueuedEventCollectionResponse,
    QueuedEventResponse,
)
from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    GetUserDep,
    ReadableDagsFilterDep,
    requires_access_asset,
    requires_access_asset_alias,
    requires_access_dag,
)
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.assets.manager import asset_manager
from airflow.models.asset import (
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetWatcherModel,
    TaskInletAssetReference,
    TaskOutletAssetReference,
)
from airflow.typing_compat import Unpack
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from sqlalchemy.engine import Result
    from sqlalchemy.sql import Select

assets_router = AirflowRouter(tags=["Asset"])

_DEFAULT_ASSET_ONLY_EVENT_LIMIT = 25


def _generate_queued_event_where_clause(
    *,
    asset_id: int | None = None,
    dag_id: str | None = None,
    before: datetime | str | None = None,
    permitted_dag_ids: set[str] | None = None,
) -> list:
    """Get AssetDagRunQueue where clause."""
    where_clause = []
    if dag_id is not None:
        where_clause.append(AssetDagRunQueue.target_dag_id == dag_id)
    if asset_id is not None:
        where_clause.append(AssetDagRunQueue.asset_id == asset_id)
    if before is not None:
        where_clause.append(AssetDagRunQueue.created_at < before)
    if permitted_dag_ids is not None:
        where_clause.append(AssetDagRunQueue.target_dag_id.in_(permitted_dag_ids))
    return where_clause


class OnlyActiveFilter(BaseParam[bool]):
    """Filter on asset activeness."""

    def to_orm(self, select: Select) -> Select:
        if self.value and self.skip_none:
            return select.where(AssetModel.active.has())
        return select

    @classmethod
    def depends(cls, only_active: bool = True) -> OnlyActiveFilter:
        return cls().set_value(only_active)


def _deduplicate_lineage_edges(edges: list[AssetLineageEdge]) -> list[AssetLineageEdge]:
    seen_edges: set[tuple[str, str]] = set()
    unique_edges: list[AssetLineageEdge] = []
    for edge in edges:
        key = (edge.source_id, edge.target_id)
        if key not in seen_edges:
            seen_edges.add(key)
            unique_edges.append(edge)
    return unique_edges


def _merge_column_lineage(
    left: dict[str, list[ColumnLineageSource]] | None,
    right: dict[str, list[ColumnLineageSource]] | None,
) -> dict[str, list[ColumnLineageSource]] | None:
    if left is None:
        return right
    if right is None:
        return left

    merged: dict[str, list[ColumnLineageSource]] = {column: list(sources) for column, sources in left.items()}
    seen_sources = {
        column: {(source.source_asset_uri, source.source_column) for source in sources}
        for column, sources in merged.items()
    }

    for column, sources in right.items():
        merged.setdefault(column, [])
        seen_sources.setdefault(column, set())
        for source in sources:
            source_key = (source.source_asset_uri, source.source_column)
            if source_key in seen_sources[column]:
                continue
            seen_sources[column].add(source_key)
            merged[column].append(source)

    return merged


def _deduplicate_asset_only_edges(edges: list[AssetLineageEdge]) -> list[AssetLineageEdge]:
    deduplicated_edges: dict[tuple[str, str], AssetLineageEdge] = {}
    for edge in edges:
        key = (edge.source_id, edge.target_id)
        existing_edge = deduplicated_edges.get(key)
        if existing_edge is None:
            deduplicated_edges[key] = edge
            continue
        existing_edge.column_lineage = _merge_column_lineage(
            existing_edge.column_lineage, edge.column_lineage
        )
    return list(deduplicated_edges.values())


def _get_asset_or_404(asset_id: int, session: SessionDep) -> AssetModel:
    asset = session.scalar(select(AssetModel).where(AssetModel.id == asset_id))
    if asset is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Asset with ID: `{asset_id}` was not found")
    return asset


def _normalize_lineage_depth(depth: int) -> int:
    if depth < 1:
        return 1
    if depth > _MAX_LINEAGE_DEPTH:
        return _MAX_LINEAGE_DEPTH
    return depth


def _build_full_asset_lineage_graph(
    *,
    asset: AssetModel,
    depth: int,
    session: SessionDep,
) -> AssetLineageGraphResponse:
    nodes: dict[str, AssetLineageNode] = {}
    edges: list[AssetLineageEdge] = []

    def _add_asset_node(a: AssetModel) -> str:
        node_id = f"asset:{a.id}"
        if node_id not in nodes:
            nodes[node_id] = AssetLineageNode(
                id=node_id,
                node_type="asset",
                name=a.name,
                uri=a.uri,
                group=a.group,
            )
        return node_id

    def _add_task_node(dag_id: str, task_id: str) -> str:
        task_node_id = f"task:{dag_id}:{task_id}"
        if task_node_id not in nodes:
            nodes[task_node_id] = AssetLineageNode(
                id=task_node_id,
                node_type="task",
                name=task_id,
            )
            dag_node_id = f"dag:{dag_id}"
            if dag_node_id not in nodes:
                nodes[dag_node_id] = AssetLineageNode(
                    id=dag_node_id,
                    node_type="dag",
                    name=dag_id,
                )
            edges.append(AssetLineageEdge(source_id=dag_node_id, target_id=task_node_id))
        return task_node_id

    _add_asset_node(asset)

    downstream_frontier: set[int] = {asset.id}
    visited_downstream: set[int] = {asset.id}

    for _ in range(depth):
        if not downstream_frontier:
            break

        consuming_tasks = session.execute(
            select(
                TaskInletAssetReference.asset_id,
                TaskInletAssetReference.dag_id,
                TaskInletAssetReference.task_id,
            ).where(TaskInletAssetReference.asset_id.in_(downstream_frontier))
        ).all()

        task_keys: set[tuple[str, str]] = set()
        for inlet_ref in consuming_tasks:
            asset_node_id = f"asset:{inlet_ref.asset_id}"
            task_node_id = _add_task_node(inlet_ref.dag_id, inlet_ref.task_id)
            edges.append(AssetLineageEdge(source_id=asset_node_id, target_id=task_node_id))
            task_keys.add((inlet_ref.dag_id, inlet_ref.task_id))

        if not task_keys:
            break

        outlet_conditions = [
            and_(
                TaskOutletAssetReference.dag_id == dag_id,
                TaskOutletAssetReference.task_id == task_id,
            )
            for dag_id, task_id in task_keys
        ]
        outlet_refs = session.execute(
            select(
                TaskOutletAssetReference.asset_id,
                TaskOutletAssetReference.dag_id,
                TaskOutletAssetReference.task_id,
            ).where(or_(*outlet_conditions))
        ).all()

        next_frontier: set[int] = set()
        for outlet_ref in outlet_refs:
            task_node_id = f"task:{outlet_ref.dag_id}:{outlet_ref.task_id}"
            _add_task_node(outlet_ref.dag_id, outlet_ref.task_id)
            downstream_asset = session.scalar(select(AssetModel).where(AssetModel.id == outlet_ref.asset_id))
            if downstream_asset is not None:
                downstream_node_id = _add_asset_node(downstream_asset)
                edges.append(AssetLineageEdge(source_id=task_node_id, target_id=downstream_node_id))
                if downstream_asset.id not in visited_downstream:
                    visited_downstream.add(downstream_asset.id)
                    next_frontier.add(downstream_asset.id)

        downstream_frontier = next_frontier

    upstream_frontier: set[int] = {asset.id}
    visited_upstream: set[int] = {asset.id}

    for _ in range(depth):
        if not upstream_frontier:
            break

        producing_tasks = session.execute(
            select(
                TaskOutletAssetReference.asset_id,
                TaskOutletAssetReference.dag_id,
                TaskOutletAssetReference.task_id,
            ).where(TaskOutletAssetReference.asset_id.in_(upstream_frontier))
        ).all()

        task_keys_up: set[tuple[str, str]] = set()
        for outlet_ref in producing_tasks:
            asset_node_id = f"asset:{outlet_ref.asset_id}"
            task_node_id = _add_task_node(outlet_ref.dag_id, outlet_ref.task_id)
            edges.append(AssetLineageEdge(source_id=task_node_id, target_id=asset_node_id))
            task_keys_up.add((outlet_ref.dag_id, outlet_ref.task_id))

        if not task_keys_up:
            break

        inlet_conditions = [
            and_(
                TaskInletAssetReference.dag_id == dag_id,
                TaskInletAssetReference.task_id == task_id,
            )
            for dag_id, task_id in task_keys_up
        ]
        inlet_refs = session.execute(
            select(
                TaskInletAssetReference.asset_id,
                TaskInletAssetReference.dag_id,
                TaskInletAssetReference.task_id,
            ).where(or_(*inlet_conditions))
        ).all()

        next_frontier_up: set[int] = set()
        for inlet_ref in inlet_refs:
            task_node_id = f"task:{inlet_ref.dag_id}:{inlet_ref.task_id}"
            _add_task_node(inlet_ref.dag_id, inlet_ref.task_id)
            upstream_asset = session.scalar(select(AssetModel).where(AssetModel.id == inlet_ref.asset_id))
            if upstream_asset is not None:
                upstream_node_id = _add_asset_node(upstream_asset)
                edges.append(AssetLineageEdge(source_id=upstream_node_id, target_id=task_node_id))
                if upstream_asset.id not in visited_upstream:
                    visited_upstream.add(upstream_asset.id)
                    next_frontier_up.add(upstream_asset.id)

        upstream_frontier = next_frontier_up

    return AssetLineageGraphResponse(nodes=list(nodes.values()), edges=_deduplicate_lineage_edges(edges))


def _extract_matching_column_lineage(
    *,
    source_asset_uri: str | None,
    target_asset_id: int,
    session: SessionDep,
) -> dict[str, list[ColumnLineageSource]] | None:
    if source_asset_uri is None:
        return None

    asset_events = session.scalars(
        select(AssetEvent)
        .where(AssetEvent.asset_id == target_asset_id)
        .order_by(AssetEvent.timestamp.desc())
        .limit(_DEFAULT_ASSET_ONLY_EVENT_LIMIT)
    ).all()

    merged_sources: dict[str, list[ColumnLineageSource]] = {}
    seen_sources: dict[str, set[tuple[str, str]]] = defaultdict(set)

    for asset_event in asset_events:
        column_lineage = asset_event.extra.get("column_lineage")
        if not isinstance(column_lineage, dict):
            continue

        for target_column, source_entries in column_lineage.items():
            if not isinstance(target_column, str) or not isinstance(source_entries, list):
                continue

            for source_entry in source_entries:
                if not isinstance(source_entry, dict):
                    continue

                entry_source_asset_uri = source_entry.get("source_asset_uri")
                entry_source_column = source_entry.get("source_column")
                if entry_source_asset_uri != source_asset_uri or not isinstance(entry_source_column, str):
                    continue

                source_key = (entry_source_asset_uri, entry_source_column)
                if source_key in seen_sources[target_column]:
                    continue

                seen_sources[target_column].add(source_key)
                merged_sources.setdefault(target_column, []).append(
                    ColumnLineageSource(
                        source_asset_uri=entry_source_asset_uri,
                        source_column=entry_source_column,
                    )
                )

    return merged_sources or None


def _build_asset_only_lineage_graph(
    *,
    graph: AssetLineageGraphResponse,
    root_asset_id: int,
    session: SessionDep,
) -> AssetLineageGraphResponse:
    nodes_by_id = {node.id: node for node in graph.nodes}
    asset_nodes = {node_id: node for node_id, node in nodes_by_id.items() if node.node_type == "asset"}
    task_inputs: dict[str, set[str]] = defaultdict(set)
    task_outputs: dict[str, set[str]] = defaultdict(set)

    for edge in graph.edges:
        source_node = nodes_by_id.get(edge.source_id)
        target_node = nodes_by_id.get(edge.target_id)
        if source_node is None or target_node is None:
            continue
        if source_node.node_type == "asset" and target_node.node_type == "task":
            task_inputs[target_node.id].add(source_node.id)
        elif source_node.node_type == "task" and target_node.node_type == "asset":
            task_outputs[source_node.id].add(target_node.id)

    root_node_id = f"asset:{root_asset_id}"
    asset_only_edges: list[AssetLineageEdge] = []
    asset_node_ids: set[str] = {root_node_id}

    for task_id, source_ids in task_inputs.items():
        target_ids = task_outputs.get(task_id, set())
        for source_id in source_ids:
            source_node = asset_nodes.get(source_id)
            for target_id in target_ids:
                target_node = asset_nodes.get(target_id)
                if source_node is None or target_node is None:
                    continue

                target_asset_id = int(target_id.split(":", maxsplit=1)[1])
                asset_only_edges.append(
                    AssetLineageEdge(
                        source_id=source_id,
                        target_id=target_id,
                        column_lineage=_extract_matching_column_lineage(
                            source_asset_uri=source_node.uri,
                            target_asset_id=target_asset_id,
                            session=session,
                        ),
                    )
                )
                asset_node_ids.add(source_id)
                asset_node_ids.add(target_id)

    deduplicated_edges = _deduplicate_asset_only_edges(asset_only_edges)
    asset_only_nodes = [asset_nodes[node_id] for node_id in asset_node_ids if node_id in asset_nodes]
    asset_only_nodes.sort(key=lambda node: node.id)

    return AssetLineageGraphResponse(nodes=asset_only_nodes, edges=deduplicated_edges)


@assets_router.get(
    "/assets",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_asset(method="GET")),
        Depends(requires_access_asset_alias(method="GET")),
    ],
)
def get_assets(
    limit: QueryLimit,
    offset: QueryOffset,
    name_pattern: QueryAssetNamePatternSearch,
    name_prefix_pattern: QueryAssetNamePrefixPatternSearch,
    uri_pattern: QueryUriPatternSearch,
    uri_prefix_pattern: QueryUriPrefixPatternSearch,
    dag_ids: QueryAssetDagIdPatternSearch,
    only_active: Annotated[OnlyActiveFilter, Depends(OnlyActiveFilter.depends)],
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "name", "uri", "created_at", "updated_at"], AssetModel).dynamic_depends()),
    ],
    session: SessionDep,
) -> AssetCollectionResponse:
    """Get assets."""
    # Build a query that will be used to retrieve the ID and timestamp of the latest AssetEvent
    last_asset_events = (
        select(AssetEvent.asset_id, func.max(AssetEvent.timestamp).label("last_timestamp"))
        .group_by(AssetEvent.asset_id)
        .subquery()
    )

    # First, we're pulling the Asset ID, AssetEvent ID, and AssetEvent timestamp for the latest (last)
    # AssetEvent. We'll eventually OUTER JOIN this to the AssetModel
    asset_event_query = (
        select(
            AssetEvent.asset_id,  # The ID of the Asset, which we'll need to JOIN to the AssetModel
            func.max(AssetEvent.id).label("last_asset_event_id"),  # The ID of the last AssetEvent
            func.max(AssetEvent.timestamp).label("last_asset_event_timestamp"),
        )
        .join(
            last_asset_events,
            and_(
                AssetEvent.asset_id == last_asset_events.c.asset_id,
                AssetEvent.timestamp == last_asset_events.c.last_timestamp,
            ),
        )
        .group_by(AssetEvent.asset_id)
        .subquery()
    )

    assets_select_statement = select(
        AssetModel,
        asset_event_query.c.last_asset_event_id,  # This should be the AssetEvent.id
        asset_event_query.c.last_asset_event_timestamp,
    ).outerjoin(asset_event_query, AssetModel.id == asset_event_query.c.asset_id)

    assets_select, total_entries = paginated_select(
        statement=assets_select_statement,
        filters=[only_active, name_pattern, name_prefix_pattern, uri_pattern, uri_prefix_pattern, dag_ids],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    # The below type annotation is acceptable on SQLA2.1, but not on 2.0
    assets_rows: Result[Unpack[tuple[AssetModel, int, datetime]]] = session.execute(  # type: ignore[type-arg]
        assets_select.options(
            subqueryload(AssetModel.scheduled_dags),
            subqueryload(AssetModel.producing_tasks),
            subqueryload(AssetModel.consuming_tasks),
            subqueryload(AssetModel.aliases),
            subqueryload(AssetModel.watchers).joinedload(AssetWatcherModel.trigger),
        )
    )

    assets = []

    for asset, last_asset_event_id, last_asset_event_timestamp in assets_rows:
        watchers_data = [
            {
                "name": watcher.name,
                "trigger_id": watcher.trigger_id,
                "created_date": watcher.trigger.created_date,
            }
            for watcher in asset.watchers
        ]

        asset_response = AssetResponse.model_validate(
            {
                **asset.__dict__,
                "aliases": asset.aliases,
                "watchers": watchers_data,
                "last_asset_event": {
                    "id": last_asset_event_id,
                    "timestamp": last_asset_event_timestamp,
                },
            }
        )
        assets.append(asset_response)

    return AssetCollectionResponse(
        assets=assets,
        total_entries=total_entries,
    )


@assets_router.get(
    "/assets/aliases",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset_alias(method="GET"))],
)
def get_asset_aliases(
    limit: QueryLimit,
    offset: QueryOffset,
    name_pattern: QueryAssetAliasNamePatternSearch,
    name_prefix_pattern: QueryAssetAliasNamePrefixPatternSearch,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "name"], AssetAliasModel).dynamic_depends()),
    ],
    session: SessionDep,
) -> AssetAliasCollectionResponse:
    """Get asset aliases."""
    asset_aliases_select, total_entries = paginated_select(
        statement=select(AssetAliasModel),
        filters=[name_pattern, name_prefix_pattern],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    return AssetAliasCollectionResponse(
        asset_aliases=session.scalars(asset_aliases_select),
        total_entries=total_entries,
    )


@assets_router.get(
    "/assets/aliases/{asset_alias_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset_alias(method="GET"))],
)
def get_asset_alias(asset_alias_id: int, session: SessionDep):
    """Get an asset alias."""
    alias = session.scalar(select(AssetAliasModel).where(AssetAliasModel.id == asset_alias_id))
    if alias is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Asset Alias with ID: `{asset_alias_id}` was not found",
        )
    return AssetAliasResponse.model_validate(alias)


@assets_router.get(
    "/assets/events",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def get_asset_events(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "source_task_id",
                    "source_dag_id",
                    "source_run_id",
                    "source_map_index",
                    "timestamp",
                ],
                AssetEvent,
            ).dynamic_depends("timestamp")
        ),
    ],
    asset_id: Annotated[
        FilterParam[int | None], Depends(filter_param_factory(AssetEvent.asset_id, int | None))
    ],
    source_dag_id: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(AssetEvent.source_dag_id, str | None))
    ],
    source_task_id: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(AssetEvent.source_task_id, str | None))
    ],
    source_run_id: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(AssetEvent.source_run_id, str | None))
    ],
    source_map_index: Annotated[
        FilterParam[int | None], Depends(filter_param_factory(AssetEvent.source_map_index, int | None))
    ],
    name_pattern: QueryAssetNamePatternSearch,
    name_prefix_pattern: QueryAssetNamePrefixPatternSearch,
    timestamp_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("timestamp", AssetEvent))],
    session: SessionDep,
) -> AssetEventCollectionResponse:
    """Get asset events."""
    base_statement = select(AssetEvent)
    if name_pattern.value or name_prefix_pattern.value:
        base_statement = base_statement.join(AssetModel, AssetEvent.asset_id == AssetModel.id)

    assets_event_select, total_entries = paginated_select(
        statement=base_statement,
        filters=[
            asset_id,
            source_dag_id,
            source_task_id,
            source_run_id,
            source_map_index,
            name_pattern,
            name_prefix_pattern,
            timestamp_range,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    assets_event_select = assets_event_select.options(
        subqueryload(AssetEvent.created_dagruns), joinedload(AssetEvent.asset)
    )
    assets_events = session.scalars(assets_event_select)

    return AssetEventCollectionResponse(
        asset_events=assets_events,
        total_entries=total_entries,
    )


@assets_router.post(
    "/assets/events",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="POST")), Depends(action_logging())],
)
def create_asset_event(
    body: CreateAssetEventsBody,
    session: SessionDep,
) -> AssetEventResponse:
    """Create asset events."""
    asset_model = session.scalar(select(AssetModel).where(AssetModel.id == body.asset_id).limit(1))
    if not asset_model:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Asset with ID: `{body.asset_id}` was not found")
    timestamp = timezone.utcnow()

    assets_event = asset_manager.register_asset_change(
        asset=asset_model,
        timestamp=timestamp,
        extra=body.extra,
        partition_key=body.partition_key,
        session=session,
    )

    if not assets_event:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Asset with ID: `{body.asset_id}` was not found")
    return AssetEventResponse.model_validate(assets_event)


@assets_router.post(
    "/assets/{asset_id}/materialize",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_asset(method="POST")), Depends(action_logging())],
)
def materialize_asset(
    asset_id: int,
    dag_bag: DagBagDep,
    user: GetUserDep,
    session: SessionDep,
    body: MaterializeAssetBody | None = None,
) -> DAGRunResponse:
    """Materialize an asset by triggering a DAG run that produces it."""
    dag_id_it = iter(
        session.scalars(
            select(TaskOutletAssetReference.dag_id)
            .where(TaskOutletAssetReference.asset_id == asset_id)
            .group_by(TaskOutletAssetReference.dag_id)
            .limit(2)
        )
    )

    if (dag_id := next(dag_id_it, None)) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"No DAG materializes asset with ID: {asset_id}")
    if next(dag_id_it, None) is not None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"More than one DAG materializes asset with ID: {asset_id}",
        )

    if not get_auth_manager().is_authorized_dag(
        method="POST",
        access_entity=DagAccessEntity.RUN,
        details=DagDetails(id=dag_id),
        user=user,
    ):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"User is not authorized to trigger a run for DAG: {dag_id} that materializes this asset",
        )

    dag = get_latest_version_of_dag(dag_bag, dag_id, session)

    if dag.allowed_run_types is not None and DagRunType.ASSET_MATERIALIZATION not in dag.allowed_run_types:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Dag with dag_id: '{dag_id}' does not allow asset materialization runs",
        )

    params = (body or MaterializeAssetBody()).validate_context(dag)
    return dag.create_dagrun(
        run_id=params["run_id"],
        logical_date=params["logical_date"],
        data_interval=params["data_interval"],
        run_after=params["run_after"],
        conf=params["conf"],
        run_type=DagRunType.ASSET_MATERIALIZATION,
        triggered_by=DagRunTriggeredByType.REST_API,
        triggering_user_name=user.get_name(),
        state=DagRunState.QUEUED,
        partition_key=params["partition_key"],
        note=params["note"],
        session=session,
    )


@assets_router.get(
    "/assets/{asset_id}/queuedEvents",
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def get_asset_queued_events(
    asset_id: int,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
) -> QueuedEventCollectionResponse:
    """Get queued asset events for an asset."""
    where_clause = _generate_queued_event_where_clause(
        asset_id=asset_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )
    query = select(AssetDagRunQueue).where(*where_clause).options(joinedload(AssetDagRunQueue.dag_model))

    dag_asset_queued_events_select, total_entries = paginated_select(statement=query)
    adrqs = session.scalars(dag_asset_queued_events_select).all()

    queued_events = [
        QueuedEventResponse(
            created_at=adrq.created_at,
            dag_id=adrq.target_dag_id,
            asset_id=adrq.asset_id,
            dag_display_name=adrq.dag_model.dag_display_name,
        )
        for adrq in adrqs
    ]

    return QueuedEventCollectionResponse(
        queued_events=queued_events,
        total_entries=total_entries,
    )


@assets_router.get(
    "/assets/{asset_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_asset(method="GET")),
        Depends(requires_access_asset_alias(method="GET")),
    ],
)
def get_asset(
    asset_id: int,
    session: SessionDep,
) -> AssetResponse:
    """Get an asset."""
    # Build a subquery to be used to retrieve the latest AssetEvent by matching timestamp
    last_asset_event = (
        select(func.max(AssetEvent.timestamp)).where(AssetEvent.asset_id == asset_id).scalar_subquery()
    )

    # Now, find the latest AssetEvent details using the subquery from above
    asset_event_rows = session.execute(
        select(AssetEvent.asset_id, AssetEvent.id, AssetEvent.timestamp).where(
            AssetEvent.asset_id == asset_id, AssetEvent.timestamp == last_asset_event
        )
    ).one_or_none()

    # Retrieve the Asset; there should only be one for that asset_id
    asset = session.scalar(
        select(AssetModel)
        .where(AssetModel.id == asset_id)
        .options(
            joinedload(AssetModel.scheduled_dags),
            joinedload(AssetModel.producing_tasks),
            joinedload(AssetModel.consuming_tasks),
            joinedload(AssetModel.watchers).joinedload(AssetWatcherModel.trigger),
        )
    )

    last_asset_event_id = asset_event_rows[1] if asset_event_rows else None
    last_asset_event_timestamp = asset_event_rows[2] if asset_event_rows else None

    if asset is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Asset with ID: `{asset_id}` was not found")

    watchers_data = [
        {
            "name": watcher.name,
            "trigger_id": watcher.trigger_id,
            "created_date": watcher.trigger.created_date,
        }
        for watcher in asset.watchers
    ]

    return AssetResponse.model_validate(
        {
            **asset.__dict__,
            "aliases": asset.aliases,
            "watchers": watchers_data,
            "last_asset_event": {
                "id": last_asset_event_id,
                "timestamp": last_asset_event_timestamp,
            },
        }
    )


_DEFAULT_LINEAGE_DEPTH = 5
_MAX_LINEAGE_DEPTH = 10


@assets_router.get(
    "/assets/{asset_id}/lineage",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_asset(method="GET")),
    ],
)
def get_asset_lineage(
    asset_id: int,
    session: SessionDep,
    depth: int = _DEFAULT_LINEAGE_DEPTH,
) -> AssetLineageGraphResponse:
    """
    Get the cross-DAG lineage graph for an asset.

    Performs a breadth-first traversal across asset-task-asset chains in both
    upstream and downstream directions, up to *depth* hops from the root asset.
    """
    normalized_depth = _normalize_lineage_depth(depth)
    asset = _get_asset_or_404(asset_id, session)
    return _build_full_asset_lineage_graph(asset=asset, depth=normalized_depth, session=session)


@assets_router.get(
    "/assets/{asset_id}/lineage/asset-only",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_asset(method="GET")),
    ],
)
def get_asset_only_lineage(
    asset_id: int,
    session: SessionDep,
    depth: int = _DEFAULT_LINEAGE_DEPTH,
) -> AssetLineageGraphResponse:
    """
    Get the asset-only lineage graph for an asset.

    Builds the standard lineage graph first, then collapses asset-task-asset
    paths into direct asset-to-asset edges and excludes task and DAG nodes.
    """
    normalized_depth = _normalize_lineage_depth(depth)
    asset = _get_asset_or_404(asset_id, session)
    graph = _build_full_asset_lineage_graph(asset=asset, depth=normalized_depth, session=session)
    return _build_asset_only_lineage_graph(graph=graph, root_asset_id=asset.id, session=session)


@assets_router.get(
    "/dags/{dag_id}/assets/queuedEvents",
    dependencies=[Depends(requires_access_asset(method="GET")), Depends(requires_access_dag(method="GET"))],
)
def get_dag_asset_queued_events(
    dag_id: str,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
) -> QueuedEventCollectionResponse:
    """Get queued asset events for a DAG."""
    where_clause = _generate_queued_event_where_clause(
        dag_id=dag_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )
    query = select(AssetDagRunQueue).where(*where_clause).options(joinedload(AssetDagRunQueue.dag_model))

    dag_asset_queued_events_select, total_entries = paginated_select(statement=query)
    adrqs = session.scalars(dag_asset_queued_events_select).all()

    queued_events = [
        QueuedEventResponse(
            created_at=adrq.created_at,
            dag_id=adrq.target_dag_id,
            asset_id=adrq.asset_id,
            dag_display_name=adrq.dag_model.dag_display_name,
        )
        for adrq in adrqs
    ]

    return QueuedEventCollectionResponse(
        queued_events=queued_events,
        total_entries=total_entries,
    )


@assets_router.get(
    "/dags/{dag_id}/assets/{asset_id}/queuedEvents",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET")), Depends(requires_access_dag(method="GET"))],
)
def get_dag_asset_queued_event(
    dag_id: str,
    asset_id: int,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
) -> QueuedEventResponse:
    """Get a queued asset event for a DAG."""
    where_clause = _generate_queued_event_where_clause(
        dag_id=dag_id, asset_id=asset_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )
    query = select(AssetDagRunQueue).where(*where_clause)
    adrq = session.scalar(query)
    if not adrq:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Queued event with dag_id: `{dag_id}` and asset_id: `{asset_id}` was not found",
        )

    return QueuedEventResponse(
        created_at=adrq.created_at,
        dag_id=adrq.target_dag_id,
        asset_id=asset_id,
        dag_display_name=adrq.dag_model.dag_display_name,
    )


@assets_router.delete(
    "/assets/{asset_id}/queuedEvents",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_asset(method="DELETE")),
        Depends(requires_access_dag(method="GET")),
        Depends(action_logging()),
    ],
)
def delete_asset_queued_events(
    asset_id: int,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
):
    """Delete queued asset events for an asset."""
    where_clause = _generate_queued_event_where_clause(
        asset_id=asset_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )
    delete_stmt = delete(AssetDagRunQueue).where(*where_clause).execution_options(synchronize_session="fetch")
    result = cast("CursorResult", session.execute(delete_stmt))
    if result.rowcount == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Queue event with asset_id: `{asset_id}` was not found",
        )


@assets_router.delete(
    "/dags/{dag_id}/assets/queuedEvents",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(requires_access_asset(method="DELETE")),
        Depends(requires_access_dag(method="GET")),
        Depends(action_logging()),
    ],
)
def delete_dag_asset_queued_events(
    dag_id: str,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
):
    where_clause = _generate_queued_event_where_clause(
        dag_id=dag_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )

    delete_statement = delete(AssetDagRunQueue).where(*where_clause)
    result = cast("CursorResult", session.execute(delete_statement))

    if result.rowcount == 0:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Queue event with dag_id: `{dag_id}` was not found")


@assets_router.delete(
    "/dags/{dag_id}/assets/{asset_id}/queuedEvents",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(requires_access_asset(method="DELETE")),
        Depends(requires_access_dag(method="GET")),
        Depends(action_logging()),
    ],
)
def delete_dag_asset_queued_event(
    dag_id: str,
    asset_id: int,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
):
    """Delete a queued asset event for a DAG."""
    where_clause = _generate_queued_event_where_clause(
        dag_id=dag_id, before=before, asset_id=asset_id, permitted_dag_ids=readable_dags_filter.value
    )
    delete_statement = (
        delete(AssetDagRunQueue).where(*where_clause).execution_options(synchronize_session="fetch")
    )
    result = cast("CursorResult", session.execute(delete_statement))
    if result.rowcount == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Queued event with dag_id: `{dag_id}` and asset_id: `{asset_id}` was not found",
        )
