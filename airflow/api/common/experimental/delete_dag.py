# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow import models, settings
from airflow.exceptions import AirflowException
from sqlalchemy import or_


class DagFileExists(AirflowException):
    status = 400


class DagNotFound(AirflowException):
    status = 404


def delete_dag(dag_id):
    session = settings.Session()

    DM = models.DagModel
    dag = session.query(DM).filter(DM.dag_id == dag_id).first()
    if dag is None:
        raise DagNotFound("Dag id {} not found".format(dag_id))

    dagbag = models.DagBag()
    if dag_id in dagbag.dags:
        raise DagFileExists("Dag id {} is still in DagBag. "
                            "Remove the DAG file first.".format(dag_id))

    count = 0

    for m in models.Base._decl_class_registry.values():
        if hasattr(m, "dag_id"):
            cond = or_(m.dag_id == dag_id, m.dag_id.like(dag_id + ".%"))
            count += session.query(m).filter(cond).delete(synchronize_session='fetch')

    if dag.is_subdag:
        p, c = dag_id.rsplit(".", 1)
        for m in models.DagRun, models.TaskFail, models.TaskInstance:
            count += session.query(m).filter(m.dag_id == p, m.task_id == c).delete()

    session.commit()

    return count
