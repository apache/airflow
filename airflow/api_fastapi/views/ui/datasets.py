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

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.db import get_session
from airflow.models import DagModel
from airflow.models.dataset import DagScheduleDatasetReference, DatasetDagRunQueue, DatasetEvent, DatasetModel

datasets_router = APIRouter(tags=["Dataset"])


@datasets_router.get("/next_run_datasets/{dag_id}", include_in_schema=False)
async def next_run_datasets(
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
                DatasetModel.id,
                DatasetModel.uri,
                func.max(DatasetEvent.timestamp).label("lastUpdate"),
            )
            .join(DagScheduleDatasetReference, DagScheduleDatasetReference.dataset_id == DatasetModel.id)
            .join(
                DatasetDagRunQueue,
                and_(
                    DatasetDagRunQueue.dataset_id == DatasetModel.id,
                    DatasetDagRunQueue.target_dag_id == DagScheduleDatasetReference.dag_id,
                ),
                isouter=True,
            )
            .join(
                DatasetEvent,
                and_(
                    DatasetEvent.dataset_id == DatasetModel.id,
                    (
                        DatasetEvent.timestamp >= latest_run.execution_date
                        if latest_run and latest_run.execution_date
                        else True
                    ),
                ),
                isouter=True,
            )
            .where(DagScheduleDatasetReference.dag_id == dag_id, ~DatasetModel.is_orphaned)
            .group_by(DatasetModel.id, DatasetModel.uri)
            .order_by(DatasetModel.uri)
        )
    ]
    data = {"dataset_expression": dag_model.dataset_expression, "events": events}
    return data
