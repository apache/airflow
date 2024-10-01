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

from fastapi import Depends, HTTPException, Request
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.db.common import get_session
from airflow.api_fastapi.views.router import AirflowRouter
from airflow.models import DagModel
from airflow.models.asset import AssetDagRunQueue, AssetEvent, AssetModel, DagScheduleAssetReference

assets_router = AirflowRouter(tags=["Asset"])


@assets_router.get("/next_run_datasets/{dag_id}", include_in_schema=False)
async def next_run_assets(
    dag_id: str,
    request: Request,
    session: Annotated[Session, Depends(get_session)],
) -> dict:
    dag = request.app.state.dag_bag.get_dag(dag_id)

    if not dag:
        raise HTTPException(404, f"can't find dag {dag_id}")

    dag_model = DagModel.get_dagmodel(dag_id, session=session)

    if dag_model is None:
        raise HTTPException(404, f"can't find associated dag_model {dag_id}")

    latest_run = dag_model.get_last_dagrun(session=session)

    events = [
        dict(info._mapping)
        for info in session.execute(
            select(
                AssetModel.id,
                AssetModel.uri,
                func.max(AssetEvent.timestamp).label("lastUpdate"),
            )
            .join(DagScheduleAssetReference, DagScheduleAssetReference.dataset_id == AssetModel.id)
            .join(
                AssetDagRunQueue,
                and_(
                    AssetDagRunQueue.dataset_id == AssetModel.id,
                    AssetDagRunQueue.target_dag_id == DagScheduleAssetReference.dag_id,
                ),
                isouter=True,
            )
            .join(
                AssetEvent,
                and_(
                    AssetEvent.dataset_id == AssetModel.id,
                    (
                        AssetEvent.timestamp >= latest_run.execution_date
                        if latest_run and latest_run.execution_date
                        else True
                    ),
                ),
                isouter=True,
            )
            .where(DagScheduleAssetReference.dag_id == dag_id, ~AssetModel.is_orphaned)
            .group_by(AssetModel.id, AssetModel.uri)
            .order_by(AssetModel.uri)
        )
    ]
    data = {"dataset_expression": dag_model.dataset_expression, "events": events}
    return data
