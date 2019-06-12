# -*- coding: utf-8 -*-
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
from typing import Union, Optional, List

from airflow.exceptions import DagRunAlreadyExists, DagNotFound
from airflow.models import DagRun, DagBag, DagModel
from airflow.utils import timezone
from airflow.utils.state import State


def _trigger_dag(
        dag_id: str,
        dag_bag: DagBag,
        dag_run: DagModel,
        run_id: Optional[str],
        conf: Optional[Union[dict, str]],
        execution_date: Optional[datetime],
        replace_microseconds: bool,
) -> List[DagRun]:  # pylint: disable=too-many-arguments
    """Triggers DAG run.

    :param dag_id: DAG ID
    :param dag_bag: DAG Bag model
    :param dag_run: DAG Run model
    :param run_id: ID of the dag_run
    :param conf: configuration
    :param execution_date: date of execution
    :param replace_microseconds: whether microseconds should be zeroed
    :return: list of triggered dags
    """
    if dag_id not in dag_bag.dags:
        raise DagNotFound("Dag id {} not found".format(dag_id))

    dag = dag_bag.get_dag(dag_id)

    execution_date = execution_date if execution_date else timezone.utcnow()

    assert timezone.is_localized(execution_date)

    if replace_microseconds:
        execution_date = execution_date.replace(microsecond=0)

    if not run_id:
        run_id = "manual__{0}".format(execution_date.isoformat())

    dag_run_id = dag_run.find(dag_id=dag_id, run_id=run_id)
    if dag_run_id:
        raise DagRunAlreadyExists("Run id {} already exists for dag id {}".format(
            run_id,
            dag_id
        ))

    run_conf = None
    if conf:
        if isinstance(conf, dict):
            run_conf = conf
        else:
            run_conf = json.loads(conf)

    triggers = list()
    dags_to_trigger = list()
    dags_to_trigger.append(dag)
    while dags_to_trigger:
        dag = dags_to_trigger.pop()
        trigger = dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True,
        )
        triggers.append(trigger)
        if dag.subdags:
            dags_to_trigger.extend(dag.subdags)
    return triggers


def trigger_dag(
        dag_id: str,
        run_id: str = None,
        conf: Optional[Union[dict, str]] = None,
        execution_date: datetime = None,
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
    dagbag = DagBag(dag_folder=dag_model.fileloc)
    dag_run = DagRun()
    triggers = _trigger_dag(
        dag_id=dag_id,
        dag_run=dag_run,
        dag_bag=dagbag,
        run_id=run_id,
        conf=conf,
        execution_date=execution_date,
        replace_microseconds=replace_microseconds,
    )

    return triggers[0] if triggers else None
