#
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
"""Delete DAGs APIs."""
import logging

from sqlalchemy import and_, or_

from airflow import models
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models import DagModel, TaskFail
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.db import get_sqla_model_classes
from airflow.utils.session import provide_session
from airflow.utils.state import State

log = logging.getLogger(__name__)


@provide_session
def delete_dag(dag_id: str, keep_records_in_log: bool = True, session=None) -> int:
    """
    :param dag_id: the dag_id of the DAG to delete
    :param keep_records_in_log: whether keep records of the given dag_id
        in the Log table in the backend database (for reasons like auditing).
        The default value is True.
    :param session: session used
    :return count of deleted dags
    """
    log.info("Deleting DAG: %s", dag_id)
    running_tis = (
        session.query(models.TaskInstance.state)
        .filter(models.TaskInstance.dag_id == dag_id)
        .filter(models.TaskInstance.state == State.RUNNING)
        .first()
    )
    if running_tis:
        raise AirflowException("TaskInstances still running")
    dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
    if dag is None:
        raise DagNotFound(f"Dag id {dag_id} not found")

    # deleting a DAG should also delete all of its subdags
    dags_to_delete_query = session.query(DagModel.dag_id).filter(
        or_(
            DagModel.dag_id == dag_id,
            and_(DagModel.dag_id.like(f"{dag_id}.%"), DagModel.is_subdag),
        )
    )
    dags_to_delete = [dag_id for dag_id, in dags_to_delete_query]

    # Scheduler removes DAGs without files from serialized_dag table every dag_dir_list_interval.
    # There may be a lag, so explicitly removes serialized DAG here.
    if SerializedDagModel.has_dag(dag_id=dag_id, session=session):
        SerializedDagModel.remove_dag(dag_id=dag_id, session=session)

    count = 0

    for model in get_sqla_model_classes():
        if hasattr(model, "dag_id"):
            if keep_records_in_log and model.__name__ == 'Log':
                continue
            count += (
                session.query(model)
                .filter(model.dag_id.in_(dags_to_delete))
                .delete(synchronize_session='fetch')
            )
    if dag.is_subdag:
        parent_dag_id, task_id = dag_id.rsplit(".", 1)
        for model in TaskFail, models.TaskInstance:
            count += (
                session.query(model).filter(model.dag_id == parent_dag_id, model.task_id == task_id).delete()
            )

    # Delete entries in Import Errors table for a deleted DAG
    # This handles the case when the dag_id is changed in the file
    session.query(models.ImportError).filter(models.ImportError.filename == dag.fileloc).delete(
        synchronize_session='fetch'
    )

    return count
