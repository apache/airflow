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

from fastapi import Depends, HTTPException, status
from sqlalchemy import and_, case, func, select, true

from airflow.api_fastapi.common.dagbag import DagBagDep
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
    dag_bag: DagBagDep,
    session: SessionDep,
) -> dict:
    dag_model = DagModel.get_dagmodel(dag_id, session=session)
    if dag_model is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"can't find associated dag_model {dag_id}")

    latest_run = dag_model.get_last_dagrun(session=session)

    if latest_run and latest_run.logical_date:
        on_clause = AssetEvent.timestamp >= latest_run.logical_date
    else:
        on_clause = true()

    query_result = session.execute(
        select(
            AssetModel.id,
            AssetModel.uri,
            AssetModel.name,
            func.max(AssetEvent.timestamp).label("lastUpdate"),
            func.max(
                case(
                    (AssetDagRunQueue.asset_id.is_not(None), 1),
                    else_=0,
                )
            ).label("queued"),
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
                on_clause,
            ),
            isouter=True,
        )
        .where(DagScheduleAssetReference.dag_id == dag_id, AssetModel.active.has())
        .group_by(AssetModel.id, AssetModel.uri, AssetModel.name)
        .order_by(AssetModel.uri)
    )
    events = [dict(info._mapping) for info in query_result]

    for event in events:
        if not event.pop("queued", None):
            event["lastUpdate"] = None

    data = {"asset_expression": dag_model.asset_expression, "events": events}
    return data
