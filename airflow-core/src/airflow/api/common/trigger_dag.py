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
from typing import TYPE_CHECKING

from airflow._shared.timezones import timezone
from airflow.exceptions import DagNotFound, DagRunAlreadyExists
from airflow.models import DagModel, DagRun
from airflow.models.dagbag import DBDagBag
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm.session import Session

    from airflow.timetables.base import DataInterval


@provide_session
def _trigger_dag(
    dag_id: str,
    dag_bag: DBDagBag,
    *,
    triggered_by: DagRunTriggeredByType,
    triggering_user_name: str | None = None,
    run_after: datetime | None = None,
    run_id: str | None = None,
    conf: dict | str | None = None,
    logical_date: datetime | None = None,
    replace_microseconds: bool = True,
    session: Session = NEW_SESSION,
) -> DagRun | None:
    """
    Triggers DAG run.

    :param dag_id: DAG ID
    :param dag_bag: DAG Bag model
    :param triggered_by: the entity which triggers the dag_run
    :param triggering_user_name: the user name who triggers the dag_run
    :param run_after: the datetime before which dag cannot run
    :param run_id: ID of the run
    :param conf: configuration
    :param logical_date: logical date of the run
    :param replace_microseconds: whether microseconds should be zeroed
    :return: list of triggered dags
    """
    if (dag := dag_bag.get_latest_version_of_dag(dag_id, session=session)) is None:
        raise DagNotFound(f"Dag id {dag_id} not found")

    run_after = run_after or timezone.coerce_datetime(timezone.utcnow())
    coerced_logical_date: datetime | None = None
    if logical_date:
        if not timezone.is_localized(logical_date):
            raise ValueError("The logical date should be localized")

        if replace_microseconds:
            logical_date = logical_date.replace(microsecond=0)

        if dag.default_args and "start_date" in dag.default_args:
            min_dag_start_date = dag.default_args["start_date"]
            if min_dag_start_date and logical_date < min_dag_start_date:
                raise ValueError(
                    f"Logical date [{logical_date.isoformat()}] should be >= start_date "
                    f"[{min_dag_start_date.isoformat()}] from DAG's default_args"
                )
        coerced_logical_date = timezone.coerce_datetime(logical_date)
        data_interval: DataInterval | None = dag.timetable.infer_manual_data_interval(
            run_after=timezone.coerce_datetime(run_after)
        )
    else:
        data_interval = None

    run_id = run_id or dag.timetable.generate_run_id(
        run_type=DagRunType.MANUAL,
        run_after=timezone.coerce_datetime(run_after),
        data_interval=data_interval,
    )

    # This intentionally does not use 'session' in the current scope because it
    # may be rolled back when this function exits with an exception (due to how
    # provide_session is implemented). This would make the DagRun object in the
    # DagRunAlreadyExists expire and unusable.
    if dag_run := DagRun.find_duplicate(dag_id=dag_id, run_id=run_id):
        raise DagRunAlreadyExists(dag_run)

    run_conf = None
    if conf:
        run_conf = conf if isinstance(conf, dict) else json.loads(conf)
    dag_run = dag.create_dagrun(
        run_id=run_id,
        logical_date=coerced_logical_date,
        data_interval=data_interval,
        run_after=run_after,
        conf=run_conf,
        run_type=DagRunType.MANUAL,
        triggered_by=triggered_by,
        triggering_user_name=triggering_user_name,
        state=DagRunState.QUEUED,
        session=session,
    )

    return dag_run


@provide_session
def trigger_dag(
    dag_id: str,
    *,
    triggered_by: DagRunTriggeredByType,
    triggering_user_name: str | None = None,
    run_after: datetime | None = None,
    run_id: str | None = None,
    conf: dict | str | None = None,
    logical_date: datetime | None = None,
    replace_microseconds: bool = True,
    session: Session = NEW_SESSION,
) -> DagRun | None:
    """
    Triggers execution of DAG specified by dag_id.

    :param dag_id: DAG ID
    :param triggered_by: the entity which triggers the dag_run
    :param triggering_user_name: the user name who triggers the dag_run
    :param run_after: the datetime before which dag won't run
    :param run_id: ID of the dag_run
    :param conf: configuration
    :param logical_date: date of execution
    :param replace_microseconds: whether microseconds should be zeroed
    :param session: Unused. Only added in compatibility with database isolation mode
    :return: first dag run triggered - even if more than one Dag Runs were triggered or None
    """
    dag_model = DagModel.get_current(dag_id, session=session)
    if dag_model is None:
        raise DagNotFound(f"Dag id {dag_id} not found in DagModel")

    dagbag = DBDagBag()
    dr = _trigger_dag(
        dag_id=dag_id,
        dag_bag=dagbag,
        run_id=run_id,
        run_after=run_after or timezone.utcnow(),
        conf=conf,
        logical_date=logical_date,
        replace_microseconds=replace_microseconds,
        triggered_by=triggered_by,
        triggering_user_name=triggering_user_name,
        session=session,
    )

    return dr if dr else None
