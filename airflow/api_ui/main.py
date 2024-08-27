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

import os

from fastapi import APIRouter, FastAPI, HTTPException, Request
from sqlalchemy import and_, func, select

from airflow.models import DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dataset import DagScheduleDatasetReference, DatasetDagRunQueue, DatasetEvent, DatasetModel
from airflow.settings import DAGS_FOLDER
from airflow.utils.session import create_session


def init_dag_bag(app: FastAPI) -> None:
    """
    Create global DagBag for the FastAPI application.

    To access it use ``request.app.state.dag_bag``.
    """
    if os.environ.get("SKIP_DAGS_PARSING") == "True":
        app.state.dag_bag = DagBag(os.devnull, include_examples=False)
    else:
        app.state.dag_bag = DagBag(DAGS_FOLDER, read_dags_from_db=True)


def create_app() -> FastAPI:
    app = FastAPI(
        description="Internal Rest API for the UI frontend. It shouldn't be used and is subject to breaking "
        "change if needed by the front-end. Users should use the public API instead."
    )

    init_dag_bag(app)

    return app


app = create_app()


router = APIRouter(prefix="/ui", tags=["UI"])


# Ultimately we want an async route, with async sqlalchemy session / context manager.
# Additional effort, not handled for now and most likely part of the AIP-70
@router.get("/next_run_datasets/{dag_id}")
def next_run_datasets(dag_id: str, request: Request) -> dict:
    dag = request.app.state.dag_bag.get_dag(dag_id)

    if not dag:
        raise HTTPException(404, f"can't find dag {dag_id}")

    with create_session() as session:
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


app.include_router(router)
