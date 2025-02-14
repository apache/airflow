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

from fastapi import HTTPException, Request, status
from sqlalchemy import and_, func, select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.common import BaseGraphResponse
from airflow.models import DagModel
from airflow.models.asset import AssetDagRunQueue, AssetEvent, AssetModel, DagScheduleAssetReference
from airflow.models.serialized_dag import SerializedDagModel

assets_router = AirflowRouter(tags=["Asset"])


@assets_router.get("/next_run_assets/{dag_id}")
def next_run_assets(
    dag_id: str,
    request: Request,
    session: SessionDep,
) -> dict:
    dag = request.app.state.dag_bag.get_dag(dag_id)

    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"can't find dag {dag_id}")

    dag_model = DagModel.get_dagmodel(dag_id, session=session)

    if dag_model is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"can't find associated dag_model {dag_id}")

    latest_run = dag_model.get_last_dagrun(session=session)

    events = [
        dict(info._mapping)
        for info in session.execute(
            select(
                AssetModel.id,
                AssetModel.uri,
                func.max(AssetEvent.timestamp).label("lastUpdate"),
            )
            .join(DagScheduleAssetReference, DagScheduleAssetReference.asset_id == AssetModel.id)
            .join(
                AssetDagRunQueue,
                and_(
                    AssetDagRunQueue.asset_id == AssetModel.id,
                    AssetDagRunQueue.target_dag_id == DagScheduleAssetReference.dag_id,
                ),
                isouter=True,
            )
            .join(
                AssetEvent,
                and_(
                    AssetEvent.asset_id == AssetModel.id,
                    (
                        AssetEvent.timestamp >= latest_run.logical_date
                        if latest_run and latest_run.logical_date
                        else True
                    ),
                ),
                isouter=True,
            )
            .where(DagScheduleAssetReference.dag_id == dag_id, AssetModel.active.has())
            .group_by(AssetModel.id, AssetModel.uri)
            .order_by(AssetModel.uri)
        )
    ]
    data = {"asset_expression": dag_model.asset_expression, "events": events}
    return data


@assets_router.get(
    "/asset_dependencies",
)
def asset_dependencies(
    session: SessionDep,
) -> BaseGraphResponse:
    """Asset dependencies graph."""
    nodes_dict: dict[str, dict] = {}
    edge_tuples: set[tuple[str, str]] = set()

    for dag, dependencies in sorted(SerializedDagModel.get_dag_dependencies().items()):
        dag_node_id = f"dag:{dag}"
        if dag_node_id not in nodes_dict:
            for dep in dependencies:
                if dep.dependency_type in ("dag", "asset", "asset-alias"):
                    # Add nodes
                    nodes_dict[dag_node_id] = {"id": dag_node_id, "label": dag, "type": "dag"}
                    if dep.node_id not in nodes_dict:
                        nodes_dict[dep.node_id] = {
                            "id": dep.node_id,
                            "label": dep.dependency_id,
                            "type": dep.dependency_type,
                        }

                    # Add edges
                    # start dependency
                    if dep.source == dep.dependency_type:
                        source = dep.node_id
                        target = dep.target if ":" in dep.target else f"dag:{dep.target}"
                        edge_tuples.add((source, target))

                    # end dependency
                    if dep.target == dep.dependency_type:
                        source = dep.source if ":" in dep.source else f"dag:{dep.source}"
                        target = dep.node_id
                        edge_tuples.add((source, target))

    nodes = list(nodes_dict.values())
    edges = [{"source_id": source, "target_id": target} for source, target in sorted(edge_tuples)]

    data = {
        "nodes": nodes,
        "edges": edges,
    }

    return BaseGraphResponse(**data)
