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

import json

from airflow.exceptions import AirflowException
from airflow.models import DagRun, DagBag
from airflow.utils import timezone
from airflow.utils.state import State


def trigger_dag(dag_id, run_id=None, conf=None, execution_date=None):
    dagbag = DagBag()

    if dag_id not in dagbag.dags:
        raise AirflowException("Dag id {} not found".format(dag_id))

    dag = dagbag.get_dag(dag_id)

    if not execution_date:
        execution_date = timezone.utcnow()

    assert timezone.is_localized(execution_date)
    execution_date = execution_date.replace(microsecond=0)

    if not run_id:
        run_id = "manual__{0}".format(execution_date.isoformat())

    dr = DagRun.find(dag_id=dag_id, run_id=run_id)
    if dr:
        raise AirflowException("Run id {} already exists for dag id {}".format(
            run_id,
            dag_id
        ))

    run_conf = None
    if conf:
        run_conf = json.loads(conf)

    trigger = dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.RUNNING,
        conf=run_conf,
        external_trigger=True
    )

    return trigger
