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

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, List, Optional, Set

from sqlalchemy import func, or_
from sqlalchemy.orm.session import Session

from airflow.configuration import conf
from airflow.exceptions import TaskNotFound
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.email import get_email_address_list, send_email
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models import DAG, DagBag
    from airflow.models.taskinstance import SimpleTaskInstance


class CallbackRequest:
    """
    Base Class with information about the callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param msg: Additional Message that can be used for logging
    """

    def __init__(self, full_filepath: str, msg: Optional[str] = None):
        self.full_filepath = full_filepath
        self.msg = msg

    def __eq__(self, other):
        if isinstance(other, CallbackRequest):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self):
        return str(self.__dict__)

    def execute(self, dagbag: "DagBag", *, session: Session) -> None:
        """Execute the callback."""
        raise NotImplementedError("implement in subclass")


class TaskCallbackRequest(CallbackRequest, LoggingMixin):
    """
    A Class with information about the success/failure TI callback to be executed. Currently, only failure
    callbacks (when tasks are externally killed) and Zombies are run via DagFileProcessorProcess.

    :param full_filepath: File Path to use to run the callback
    :param simple_task_instance: Simplified Task Instance representation
    :param is_failure_callback: Flag to determine whether it is a Failure Callback or Success Callback
    :param msg: Additional Message that can be used for logging to determine failure/zombie
    """

    def __init__(
        self,
        full_filepath: str,
        simple_task_instance: "SimpleTaskInstance",
        is_failure_callback: Optional[bool] = True,
        msg: Optional[str] = None,
    ):
        super().__init__(full_filepath=full_filepath, msg=msg)
        self.simple_task_instance = simple_task_instance
        self.is_failure_callback = is_failure_callback

    def execute(self, dagbag: "DagBag", *, session: Session) -> None:  # pylint: disable=unused-argument
        from airflow.models import TaskInstance

        simple_ti = self.simple_task_instance
        if simple_ti.dag_id not in dagbag.dags:
            return
        dag = dagbag.dags[simple_ti.dag_id]
        if simple_ti.task_id not in dag.task_ids:
            return
        task = dag.get_task(simple_ti.task_id)
        ti = TaskInstance(task, simple_ti.execution_date)
        # Get properties needed for failure handling from SimpleTaskInstance.
        ti.start_date = simple_ti.start_date
        ti.end_date = simple_ti.end_date
        ti.try_number = simple_ti.try_number
        ti.state = simple_ti.state
        ti.test_mode = conf.getboolean('core', 'UNIT_TEST_MODE')
        if not self.is_failure_callback:
            return
        ti.handle_failure_with_callback(error=self.msg, test_mode=ti.test_mode)
        self.log.info('Executed failure callback for %s in state %s', ti, ti.state)


class DagCallbackRequest(CallbackRequest):
    """
    A Class with information about the success/failure DAG callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param dag_id: DAG ID
    :param execution_date: Execution Date for the DagRun
    :param is_failure_callback: Flag to determine whether it is a Failure Callback or Success Callback
    :param msg: Additional Message that can be used for logging
    """

    def __init__(
        self,
        full_filepath: str,
        dag_id: str,
        execution_date: datetime,
        is_failure_callback: Optional[bool] = True,
        msg: Optional[str] = None,
    ):
        super().__init__(full_filepath=full_filepath, msg=msg)
        self.dag_id = dag_id
        self.execution_date = execution_date
        self.is_failure_callback = is_failure_callback

    def execute(self, dagbag: "DagBag", *, session: Session) -> None:
        dag = dagbag.dags[self.dag_id]
        dag_run = dag.get_dagrun(
            execution_date=self.execution_date,
            session=session,
        )
        dag.handle_callback(
            dagrun=dag_run,
            success=not self.is_failure_callback,
            reason=self.msg,
            session=session,
        )


class SlaCallbackRequest(CallbackRequest, LoggingMixin):
    """
    A class with information about the SLA callback to be executed.

    :param full_filepath: File Path to use to run the callback
    :param dag_id: DAG ID
    """

    def __init__(self, full_filepath: str, dag_id: str):
        super().__init__(full_filepath)
        self.dag_id = dag_id

    def _update_sla_misses(self, dag: "DAG", *, session: Session) -> None:
        from airflow.models import SlaMiss, TaskInstance as TI

        qry = (
            session.query(TI.task_id, func.max(TI.execution_date).label('max_ti'))
            .with_hint(TI, 'USE INDEX (PRIMARY)', dialect_name='mysql')
            .filter(TI.dag_id == dag.dag_id)
            .filter(or_(TI.state == State.SUCCESS, TI.state == State.SKIPPED))
            .filter(TI.task_id.in_(dag.task_ids))
            .group_by(TI.task_id)
            .subquery('sq')
        )

        max_tis: List[TI] = (
            session.query(TI)
            .filter(
                TI.dag_id == dag.dag_id,
                TI.task_id == qry.c.task_id,
                TI.execution_date == qry.c.max_ti,
            )
            .all()
        )

        ts = timezone.utcnow()
        for ti in max_tis:
            task = dag.get_task(ti.task_id)
            if task.sla and not isinstance(task.sla, timedelta):
                raise TypeError(
                    f"SLA is expected to be timedelta object, got "
                    f"{type(task.sla)} in {task.dag_id}:{task.task_id}"
                )

            dttm = dag.following_schedule(ti.execution_date)
            while dttm < timezone.utcnow():
                following_schedule = dag.following_schedule(dttm)
                if following_schedule + task.sla < timezone.utcnow():
                    session.merge(
                        SlaMiss(task_id=ti.task_id, dag_id=ti.dag_id, execution_date=dttm, timestamp=ts)
                    )
                dttm = dag.following_schedule(dttm)
        session.commit()

    def _update_sla_dates(self, dag: "DAG", *, session: Session) -> None:
        from airflow.models import SlaMiss, TaskInstance as TI

        # pylint: disable=singleton-comparison
        slas: List[SlaMiss] = (
            session.query(SlaMiss)
            .filter(SlaMiss.notification_sent == False, SlaMiss.dag_id == dag.dag_id)  # noqa
            .all()
        )
        # pylint: enable=singleton-comparison

        sla_dates: List[datetime.datetime] = [sla.execution_date for sla in slas]
        if not sla_dates:
            return

        fetched_tis: List[TI] = (
            session.query(TI)
            .filter(TI.state != State.SUCCESS, TI.execution_date.in_(sla_dates), TI.dag_id == dag.dag_id)
            .all()
        )
        blocking_tis: List[TI] = []
        for ti in fetched_tis:
            if ti.task_id in dag.task_ids:
                ti.task = dag.get_task(ti.task_id)
                blocking_tis.append(ti)
            else:
                session.delete(ti)
                session.commit()

        task_list = "\n".join([sla.task_id + ' on ' + sla.execution_date.isoformat() for sla in slas])
        blocking_task_list = "\n".join(
            [ti.task_id + ' on ' + ti.execution_date.isoformat() for ti in blocking_tis]
        )
        # Track whether email or any alert notification sent
        # We consider email or the alert callback as notifications
        email_sent = False
        notification_sent = False
        if dag.sla_miss_callback:
            # Execute the alert callback
            self.log.info('Calling SLA miss callback')
            try:
                dag.sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)
                notification_sent = True
            except Exception:  # pylint: disable=broad-except
                self.log.exception("Could not call sla_miss_callback for DAG %s", dag.dag_id)
        email_content = f"""\
        Here's a list of tasks that missed their SLAs:
        <pre><code>{task_list}\n<code></pre>
        Blocking tasks:
        <pre><code>{blocking_task_list}<code></pre>
        Airflow Webserver URL: {conf.get(section='webserver', key='base_url')}
        """

        tasks_missed_sla = []
        for sla in slas:
            try:
                task = dag.get_task(sla.task_id)
            except TaskNotFound:
                # task already deleted from DAG, skip it
                self.log.warning(
                    "Task %s doesn't exist in DAG anymore, skipping SLA miss notification.", sla.task_id
                )
                continue
            tasks_missed_sla.append(task)

        emails: Set[str] = set()
        for task in tasks_missed_sla:
            if task.email:
                if isinstance(task.email, str):
                    emails |= set(get_email_address_list(task.email))
                elif isinstance(task.email, (list, tuple)):
                    emails |= set(task.email)
        if emails:
            try:
                send_email(emails, f"[airflow] SLA miss on DAG={dag.dag_id}", email_content)
                email_sent = True
                notification_sent = True
            except Exception:  # pylint: disable=broad-except
                Stats.incr('sla_email_notification_failure')
                self.log.exception("Could not send SLA Miss email notification for DAG %s", dag.dag_id)
        # If we sent any notification, update the sla_miss table
        if notification_sent:
            for sla in slas:
                sla.email_sent = email_sent
                sla.notification_sent = True
                session.merge(sla)
        session.commit()

    def execute(self, dagbag: "DagBag", *, session: Session) -> None:
        """
        Finding all tasks that have SLAs defined, and sending alert emails
        where needed. New SLA misses are also recorded in the database.

        We are assuming that the scheduler runs often, so we only check for
        tasks that should have succeeded in the past hour.
        """
        dag = dagbag.dags[self.dag_id]
        self.log.info("Running SLA Checks for %s", dag.dag_id)
        if not any(isinstance(ti.sla, timedelta) for ti in dag.tasks):
            self.log.info("Skipping SLA check for %s because no tasks in DAG have SLAs", dag)
            return

        self._update_sla_misses(dag, session=session)
        self._update_sla_dates(dag, session=session)
