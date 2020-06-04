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

# TODO(mik-laj): We have to implement it.
#     Do you want to help? Please look at: https://github.com/apache/airflow/issues/8129

from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.schemas.dagrun_schema import dagrun_schema
from airflow.models import DagRun
from airflow.utils.session import provide_session


def delete_dag_run():
    """
    Delete a DAG Run
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def get_dag_run(dag_id, dag_run_id, session):
    """
    Get a DAG Run.
    """
    query = session.query(DagRun)
    query = query.filter(DagRun.dag_id == dag_id)
    query = query.filter(DagRun.run_id == dag_run_id)
    dag_run = query.one_or_none()
    if dag_run is None:
        raise NotFound("DAGRun not found")
    return dagrun_schema.dump(dag_run)


def get_dag_runs():
    """
    Get all DAG Runs.
    """
    raise NotImplementedError("Not implemented yet.")


def get_dag_runs_batch():
    """
    Get list of DAG Runs
    """
    raise NotImplementedError("Not implemented yet.")


def patch_dag_run():
    """
    Update a DAG Run
    """
    raise NotImplementedError("Not implemented yet.")


def post_dag_run():
    """
    Trigger a DAG.
    """
    raise NotImplementedError("Not implemented yet.")
