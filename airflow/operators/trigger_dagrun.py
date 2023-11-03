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
from __future__ import annotations

import datetime
import json
import time
from typing import TYPE_CHECKING, Any, Sequence, cast

from sqlalchemy import select
from sqlalchemy.orm.exc import NoResultFound

from airflow.api.common.trigger_dag import trigger_dag
from airflow.configuration import conf
from airflow.exceptions import AirflowException, DagNotFound, DagRunAlreadyExists
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCom
from airflow.triggers.external_task import DagStateTrigger
from airflow.utils import timezone
from airflow.utils.helpers import build_airflow_url_with_query
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

XCOM_EXECUTION_DATE_ISO = "trigger_execution_date_iso"
XCOM_RUN_ID = "trigger_run_id"


if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


class TriggerDagRunLink(BaseOperatorLink):
    """
    Operator link for TriggerDagRunOperator.

    It allows users to access DAG triggered by task using TriggerDagRunOperator.
    """

    name = "Triggered DAG"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        # Fetch the correct execution date for the triggerED dag which is
        # stored in xcom during execution of the triggerING task.
        when = XCom.get_value(ti_key=ti_key, key=XCOM_EXECUTION_DATE_ISO)
        query = {"dag_id": cast(TriggerDagRunOperator, operator).trigger_dag_id, "base_date": when}
        return build_airflow_url_with_query(query)


class TriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id``.

    :param trigger_dag_id: The dag_id to trigger (templated).
    :param trigger_run_id: The run ID to use for the triggered DAG run (templated).
        If not provided, a run ID will be automatically generated.
    :param conf: Configuration for the DAG run (templated).
    :param execution_date: Execution date for the dag (templated).
    :param reset_dag_run: Whether clear existing dag run if already exists.
        This is useful when backfill or rerun an existing dag run.
        This only resets (not recreates) the dag run.
        Dag run conf is immutable and will not be reset on rerun of an existing dag run.
        When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised.
        When reset_dag_run=True and dag run exists, existing dag run will be cleared to rerun.
    :param wait_for_completion: Whether or not wait for dag run completion. (default: False)
    :param poke_interval: Poke interval to check dag run status when wait_for_completion=True.
        (default: 60)
    :param allowed_states: List of allowed states, default is ``['success']``.
    :param failed_states: List of failed or dis-allowed states, default is ``None``.
    :param deferrable: If waiting for completion, whether or not to defer the task until done,
        default is ``False``.
    """

    template_fields: Sequence[str] = (
        "trigger_dag_id",
        "trigger_run_id",
        "execution_date",
        "conf",
        "wait_for_completion",
    )
    template_fields_renderers = {"conf": "py"}
    ui_color = "#ffefeb"
    operator_extra_links = [TriggerDagRunLink()]

    def __init__(
        self,
        *,
        trigger_dag_id: str,
        trigger_run_id: str | None = None,
        conf: dict | None = None,
        execution_date: str | datetime.datetime | None = None,
        reset_dag_run: bool = False,
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        allowed_states: list[str] | None = None,
        failed_states: list[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.trigger_run_id = trigger_run_id
        self.conf = conf
        self.reset_dag_run = reset_dag_run
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        if allowed_states:
            self.allowed_states = [DagRunState(s) for s in allowed_states]
        else:
            self.allowed_states = [DagRunState.SUCCESS]
        if failed_states:
            self.failed_states = [DagRunState(s) for s in failed_states]
        else:
            self.failed_states = [DagRunState.FAILED]
        self._defer = deferrable

        if execution_date is not None and not isinstance(execution_date, (str, datetime.datetime)):
            raise TypeError(
                f"Expected str or datetime.datetime type for execution_date.Got {type(execution_date)}"
            )

        self.execution_date = execution_date

    def execute(self, context: Context):
        if isinstance(self.execution_date, datetime.datetime):
            parsed_execution_date = self.execution_date
        elif isinstance(self.execution_date, str):
            parsed_execution_date = timezone.parse(self.execution_date)
        else:
            parsed_execution_date = timezone.utcnow()

        try:
            json.dumps(self.conf)
        except TypeError:
            raise AirflowException("conf parameter should be JSON Serializable")

        if self.trigger_run_id:
            run_id = self.trigger_run_id
        else:
            run_id = DagRun.generate_run_id(DagRunType.MANUAL, parsed_execution_date)

        try:
            dag_run = trigger_dag(
                dag_id=self.trigger_dag_id,
                run_id=run_id,
                conf=self.conf,
                execution_date=parsed_execution_date,
                replace_microseconds=False,
            )

        except DagRunAlreadyExists as e:
            if self.reset_dag_run:
                self.log.info("Clearing %s on %s", self.trigger_dag_id, parsed_execution_date)

                # Get target dag object and call clear()
                dag_model = DagModel.get_current(self.trigger_dag_id)
                if dag_model is None:
                    raise DagNotFound(f"Dag id {self.trigger_dag_id} not found in DagModel")

                dag_bag = DagBag(dag_folder=dag_model.fileloc, read_dags_from_db=True)
                dag = dag_bag.get_dag(self.trigger_dag_id)
                dag_run = e.dag_run
                dag.clear(start_date=dag_run.execution_date, end_date=dag_run.execution_date)
            else:
                raise e
        if dag_run is None:
            raise RuntimeError("The dag_run should be set here!")
        # Store the execution date from the dag run (either created or found above) to
        # be used when creating the extra link on the webserver.
        ti = context["task_instance"]
        ti.xcom_push(key=XCOM_EXECUTION_DATE_ISO, value=dag_run.execution_date.isoformat())
        ti.xcom_push(key=XCOM_RUN_ID, value=dag_run.run_id)

        if self.wait_for_completion:
            # Kick off the deferral process
            if self._defer:
                self.defer(
                    trigger=DagStateTrigger(
                        dag_id=self.trigger_dag_id,
                        states=self.allowed_states + self.failed_states,
                        execution_dates=[parsed_execution_date],
                        poll_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )
            # wait for dag to complete
            while True:
                self.log.info(
                    "Waiting for %s on %s to become allowed state %s ...",
                    self.trigger_dag_id,
                    dag_run.execution_date,
                    self.allowed_states,
                )
                time.sleep(self.poke_interval)

                dag_run.refresh_from_db()
                state = dag_run.state
                if state in self.failed_states:
                    raise AirflowException(f"{self.trigger_dag_id} failed with failed states {state}")
                if state in self.allowed_states:
                    self.log.info("%s finished with allowed state %s", self.trigger_dag_id, state)
                    return

    @provide_session
    def execute_complete(self, context: Context, session: Session, event: tuple[str, dict[str, Any]]):
        # This execution date is parsed from the return trigger event
        provided_execution_date = event[1]["execution_dates"][0]
        try:
            dag_run = session.execute(
                select(DagRun).where(
                    DagRun.dag_id == self.trigger_dag_id, DagRun.execution_date == provided_execution_date
                )
            ).scalar_one()
        except NoResultFound:
            raise AirflowException(
                f"No DAG run found for DAG {self.trigger_dag_id} and execution date {self.execution_date}"
            )

        state = dag_run.state

        if state in self.failed_states:
            raise AirflowException(f"{self.trigger_dag_id} failed with failed state {state}")
        if state in self.allowed_states:
            self.log.info("%s finished with allowed state %s", self.trigger_dag_id, state)
            return

        raise AirflowException(
            f"{self.trigger_dag_id} return {state} which is not in {self.failed_states}"
            f" or {self.allowed_states}"
        )
