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
"""TaskReschedule tracks rescheduled task instances."""

import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKeyConstraint, Index, Integer, String, asc, desc, event, text
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator


class TaskReschedule(Base):
    """TaskReschedule tracks rescheduled task instances."""

    __tablename__ = "task_reschedule"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)
    run_id = Column(String(ID_LEN, **COLLATION_ARGS), nullable=False)
    map_index = Column(Integer, nullable=False, server_default=text("-1"))
    try_number = Column(Integer, nullable=False)
    start_date = Column(UtcDateTime, nullable=False)
    end_date = Column(UtcDateTime, nullable=False)
    duration = Column(Integer, nullable=False)
    reschedule_date = Column(UtcDateTime, nullable=False)

    __table_args__ = (
        Index('idx_task_reschedule_dag_task_run', dag_id, task_id, run_id, map_index, unique=False),
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_reschedule_ti_fkey",
            ondelete="CASCADE",
        ),
        Index('idx_task_reschedule_dag_run', dag_id, run_id),
        ForeignKeyConstraint(
            [dag_id, run_id],
            ['dag_run.dag_id', 'dag_run.run_id'],
            name='task_reschedule_dr_fkey',
            ondelete='CASCADE',
        ),
    )
    dag_run = relationship("DagRun")
    execution_date = association_proxy("dag_run", "execution_date")

    def __init__(
        self,
        task: "BaseOperator",
        run_id: str,
        try_number: int,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        reschedule_date: datetime.datetime,
        map_index: int = -1,
    ):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.run_id = run_id
        self.map_index = map_index
        self.try_number = try_number
        self.start_date = start_date
        self.end_date = end_date
        self.reschedule_date = reschedule_date
        self.duration = (self.end_date - self.start_date).total_seconds()

    @staticmethod
    @provide_session
    def query_for_task_instance(task_instance, descending=False, session=None, try_number=None):
        """
        Returns query for task reschedules for a given the task instance.

        :param session: the database session object
        :param task_instance: the task instance to find task reschedules for
        :param descending: If True then records are returned in descending order
        :param try_number: Look for TaskReschedule of the given try_number. Default is None which
            looks for the same try_number of the given task_instance.
        """
        if try_number is None:
            try_number = task_instance.try_number

        TR = TaskReschedule
        qry = session.query(TR).filter(
            TR.dag_id == task_instance.dag_id,
            TR.task_id == task_instance.task_id,
            TR.run_id == task_instance.run_id,
            TR.try_number == try_number,
        )
        if descending:
            return qry.order_by(desc(TR.id))
        else:
            return qry.order_by(asc(TR.id))

    @staticmethod
    @provide_session
    def find_for_task_instance(task_instance, session=None, try_number=None):
        """
        Returns all task reschedules for the task instance and try number,
        in ascending order.

        :param session: the database session object
        :param task_instance: the task instance to find task reschedules for
        :param try_number: Look for TaskReschedule of the given try_number. Default is None which
            looks for the same try_number of the given task_instance.
        """
        return TaskReschedule.query_for_task_instance(
            task_instance, session=session, try_number=try_number
        ).all()


@event.listens_for(TaskReschedule.__table__, "before_create")
def add_ondelete_for_mssql(table, conn, **kw):
    if conn.dialect.name != "mssql":
        return

    for constraint in table.constraints:
        if constraint.name != "task_reschedule_dr_fkey":
            continue
        constraint.ondelete = 'NO ACTION'
        return
