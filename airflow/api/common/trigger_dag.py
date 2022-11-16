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
from __future__ import annotations

import json
from datetime import datetime

from airflow.exceptions import DagNotFound, DagRunAlreadyExists
from airflow.models import DagBag, DagModel, DagRun
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType


def _trigger_dag(
    dag_id: str,
    dag_bag: DagBag,
    run_id: str | None = None,
    conf: dict | str | None = None,
    execution_date: datetime | None = None,
    replace_microseconds: bool = True,
    notes: str | None = None,
) -> list[DagRun | None]:
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

    if dag is None or dag_id not in dag_bag.dags:
        raise DagNotFound(f"Dag id {dag_id} not found")

    execution_date = execution_date if execution_date else timezone.utcnow()

    if not timezone.is_localized(execution_date):
        raise ValueError("The execution_date should be localized")

    if replace_microseconds:
        execution_date = execution_date.replace(microsecond=0)

    if dag.default_args and "start_date" in dag.default_args:
        min_dag_start_date = dag.default_args["start_date"]
        if min_dag_start_date and execution_date < min_dag_start_date:
            raise ValueError(
                f"The execution_date [{execution_date.isoformat()}] should be >= start_date "
                f"[{min_dag_start_date.isoformat()}] from DAG's default_args"
            )
    logical_date = timezone.coerce_datetime(execution_date)

    data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)
    run_id = run_id or dag.timetable.generate_run_id(
        run_type=DagRunType.MANUAL, logical_date=logical_date, data_interval=data_interval
    )
    dag_run = DagRun.find_duplicate(dag_id=dag_id, execution_date=execution_date, run_id=run_id)

    if dag_run:
        raise DagRunAlreadyExists(dag_run=dag_run, execution_date=execution_date, run_id=run_id)

    run_conf = None
    if conf:
        run_conf = conf if isinstance(conf, dict) else json.loads(conf)

    dag_runs = []
    dags_to_run = [dag] + dag.subdags
    for _dag in dags_to_run:
        dag_run = _dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=DagRunState.QUEUED,
            conf=run_conf,
            external_trigger=True,
            dag_hash=dag_bag.dags_hash.get(dag_id),
            data_interval=data_interval,
            notes=notes,
        )
        dag_runs.append(dag_run)

    return dag_runs


def trigger_dag(
    dag_id: str,
    run_id: str | None = None,
    conf: dict | str | None = None,
    execution_date: datetime | None = None,
    replace_microseconds: bool = True,
    notes: str | None = None,
) -> DagRun | None:
    """Triggers execution of DAG specified by dag_id

    :param dag_id: DAG ID
    :param run_id: ID of the dag_run
    :param conf: configuration
    :param execution_date: date of execution
    :param replace_microseconds: whether microseconds should be zeroed
    :param notes: set a custom note for the newly created DagRun
    :return: first dag run triggered - even if more than one Dag Runs were triggered or None
    """
    dag_model = DagModel.get_current(dag_id)
    if dag_model is None:
        raise DagNotFound(f"Dag id {dag_id} not found in DagModel")

    dagbag = DagBag(dag_folder=dag_model.fileloc, read_dags_from_db=True)
    triggers = _trigger_dag(
        dag_id=dag_id,
        dag_bag=dagbag,
        run_id=run_id,
        conf=conf,
        execution_date=execution_date,
        replace_microseconds=replace_microseconds,
        notes=notes,
    )

    return triggers[0] if triggers else None
