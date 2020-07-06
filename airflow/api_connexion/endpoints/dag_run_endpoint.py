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
from connexion import NoContent
from flask import request
from sqlalchemy import and_, func

from airflow.api_connexion.exceptions import AlreadyExists, NotFound
from airflow.api_connexion.parameters import check_limit, format_datetime, format_parameters
from airflow.api_connexion.schemas.dag_run_schema import (
    DAGRunCollection, dagrun_collection_schema, dagrun_schema,
)
from airflow.models import DagModel, DagRun
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType


@provide_session
def delete_dag_run(dag_id, dag_run_id, session):
    """
    Delete a DAG Run
    """
    if (
        session.query(DagRun)
        .filter(and_(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))
        .delete()
        == 0
    ):
        raise NotFound(detail=f"DAGRun with DAG ID: '{dag_id}' and DagRun ID: '{dag_run_id}' not found")
    return NoContent, 204


@provide_session
def get_dag_run(dag_id, dag_run_id, session):
    """
    Get a DAG Run.
    """
    dag_run = session.query(DagRun).filter(
        DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id).one_or_none()
    if dag_run is None:
        raise NotFound("DAGRun not found")
    return dagrun_schema.dump(dag_run)


@format_parameters({
    'start_date_gte': format_datetime,
    'start_date_lte': format_datetime,
    'execution_date_gte': format_datetime,
    'execution_date_lte': format_datetime,
    'end_date_gte': format_datetime,
    'end_date_lte': format_datetime,
    'limit': check_limit
})
@provide_session
def get_dag_runs(
    session,
    dag_id,
    start_date_gte=None,
    start_date_lte=None,
    execution_date_gte=None,
    execution_date_lte=None,
    end_date_gte=None,
    end_date_lte=None,
    offset=None,
    limit=None,
):
    """
    Get all DAG Runs.
    """

    query = session.query(DagRun)

    #  This endpoint allows specifying ~ as the dag_id to retrieve DAG Runs for all DAGs.
    if dag_id != "~":
        query = query.filter(DagRun.dag_id == dag_id)

    # filter start date
    if start_date_gte:
        query = query.filter(DagRun.start_date >= start_date_gte)

    if start_date_lte:
        query = query.filter(DagRun.start_date <= start_date_lte)

    # filter execution date
    if execution_date_gte:
        query = query.filter(DagRun.execution_date >= execution_date_gte)

    if execution_date_lte:
        query = query.filter(DagRun.execution_date <= execution_date_lte)

    # filter end date
    if end_date_gte:
        query = query.filter(DagRun.end_date >= end_date_gte)

    if end_date_lte:
        query = query.filter(DagRun.end_date <= end_date_lte)

    # apply offset and limit
    dag_run = query.order_by(DagRun.id).offset(offset).limit(limit).all()
    total_entries = session.query(func.count(DagRun.id)).scalar()

    return dagrun_collection_schema.dump(
        DAGRunCollection(dag_runs=dag_run, total_entries=total_entries)
    )


def get_dag_runs_batch():
    """
    Get list of DAG Runs
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def post_dag_run(dag_id, session):
    """
    Trigger a DAG.
    """
    if not session.query(DagModel).filter(DagModel.dag_id == dag_id).first():
        raise NotFound(f"DAG with dag_id: '{dag_id}' not found")

    post_body = dagrun_schema.load(request.json, session=session)
    dagrun_instance = (
        session.query(DagRun)
        .filter(and_(DagRun.dag_id == dag_id, DagRun.run_id == post_body["run_id"]))
        .first()
    )
    if not dagrun_instance:
        dag_run = DagRun(dag_id=dag_id, run_type=DagRunType.MANUAL.value, **post_body)
        session.add(dag_run)
        session.commit()
        return dagrun_schema.dump(dag_run)
    raise AlreadyExists(
        detail=f"DAGRun with DAG ID: '{dag_id}' and DAGRun ID: '{post_body['run_id']}' already exists"
    )
