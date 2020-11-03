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
"""Triggering DAG runs APIs."""
import json
from datetime import datetime
from typing import List, Optional, Union

from airflow.exceptions import DagNotFound, DagRunAlreadyExists
from airflow.models import DagBag, DagModel, DagRun
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType


def _trigger_dag(
    dag_id: str,
    dag_bag: DagBag,
    run_id: Optional[str] = None,
    conf: Optional[Union[dict, str]] = None,
    execution_date: Optional[datetime] = None,
    replace_microseconds: bool = True,
) -> List[DagRun]:  # pylint: disable=too-many-arguments
    """Triggers DAG run.

    :param dag_id: DAG ID
    :param dag_bag: DAG Bag model
    :param run_id: ID of the dag_run
    :param conf: configuration
    :param execution_date: date of execution
    :param replace_microseconds: whether microseconds should be zeroed
    :return: list of triggered dags
    """
    dag = dag_bag.get_dag(dag_id)  # prefetch dag if it is stored serialized

    if dag_id not in dag_bag.dags:
        raise DagNotFound("Dag id {} not found".format(dag_id))

    execution_date = execution_date if execution_date else timezone.utcnow()

    if not timezone.is_localized(execution_date):
        raise ValueError("The execution_date should be localized")

    if replace_microseconds:
        execution_date = execution_date.replace(microsecond=0)

    if dag.default_args and 'start_date' in dag.default_args:
        min_dag_start_date = dag.default_args["start_date"]
        if min_dag_start_date and execution_date < min_dag_start_date:
            raise ValueError(
                "The execution_date [{0}] should be >= start_date [{1}] from DAG's default_args".format(
                    execution_date.isoformat(),
                    min_dag_start_date.isoformat()))

    run_id = run_id or DagRun.generate_run_id(DagRunType.MANUAL, execution_date)
    dag_run = DagRun.find(dag_id=dag_id, run_id=run_id)

    if dag_run:
        raise DagRunAlreadyExists(
            f"Run id {run_id} already exists for dag id {dag_id}"
        )

    run_conf = None
    if conf:
        run_conf = conf if isinstance(conf, dict) else json.loads(conf)

    triggers = []
    dags_to_trigger = [dag] + dag.subdags
    for _dag in dags_to_trigger:
        trigger = _dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True,
            dag_hash=dag_bag.dags_hash.get(dag_id),
        )

        triggers.append(trigger)
    return triggers


def trigger_dag(
        dag_id: str,
        run_id: Optional[str] = None,
        conf: Optional[Union[dict, str]] = None,
        execution_date: Optional[datetime] = None,
        replace_microseconds: bool = True,
) -> Optional[DagRun]:
    """Triggers execution of DAG specified by dag_id

    :param dag_id: DAG ID
    :param run_id: ID of the dag_run
    :param conf: configuration
    :param execution_date: date of execution
    :param replace_microseconds: whether microseconds should be zeroed
    :return: first dag run triggered - even if more than one Dag Runs were triggered or None
    """
    dag_model = DagModel.get_current(dag_id)
    if dag_model is None:
        raise DagNotFound("Dag id {} not found in DagModel".format(dag_id))

    dagbag = DagBag(dag_folder=dag_model.fileloc, read_dags_from_db=True)
    triggers = _trigger_dag(
        dag_id=dag_id,
        dag_bag=dagbag,
        run_id=run_id,
        conf=conf,
        execution_date=execution_date,
        replace_microseconds=replace_microseconds,
    )

    return triggers[0] if triggers else None
