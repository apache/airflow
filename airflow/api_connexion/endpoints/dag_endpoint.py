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
from flask import current_app
from sqlalchemy import func

from airflow import DAG
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.api_connexion.schemas.dag_schema import (
    DAGCollection, dag_detail_schema, dag_schema, dags_collection_schema,
)
from airflow.models.dag import DagModel
from airflow.utils.session import provide_session


@provide_session
def get_dag(dag_id, session):
    """
    Get basic information about a DAG.
    """
    dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()

    if dag is None:
        raise NotFound("DAG not found")

    return dag_schema.dump(dag)


def get_dag_details(dag_id):
    """
    Get details of DAG.
    """
    dag: DAG = current_app.dag_bag.get_dag(dag_id)
    if not dag:
        raise NotFound("DAG not found")
    return dag_detail_schema.dump(dag)


@format_parameters({
    'limit': check_limit
})
@provide_session
def get_dags(session, limit, offset=0):
    """
    Get all DAGs.
    """
    dags = session.query(DagModel).order_by(DagModel.dag_id).offset(offset).limit(limit).all()

    total_entries = session.query(func.count(DagModel.dag_id)).scalar()

    return dags_collection_schema.dump(DAGCollection(dags=dags, total_entries=total_entries))


def patch_dag():
    """
    Update the specific DAG
    """
    raise NotImplementedError("Not implemented yet.")
