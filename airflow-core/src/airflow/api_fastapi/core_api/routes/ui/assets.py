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

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import and_, func, select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.security import requires_access_asset, requires_access_dag
from airflow.models import DagModel
from airflow.models.asset import AssetDagRunQueue, AssetEvent, AssetModel, DagScheduleAssetReference

assets_router = AirflowRouter(tags=["Asset"])


@assets_router.get(
    "/next_run_assets/{dag_id}",
    dependencies=[Depends(requires_access_asset(method="GET")), Depends(requires_access_dag(method="GET"))],
)
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
                AssetModel.name,
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
            .group_by(AssetModel.id, AssetModel.uri, AssetModel.name)
            .order_by(AssetModel.uri)
        )
    ]

    data = {"asset_expression": dag_model.asset_expression, "events": events}
    return data
